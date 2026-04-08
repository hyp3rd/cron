package cron

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule.
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(next time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance.
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//	Time Zone
//	  Description: The time zone in which schedules are interpreted
//	  Default:     time.Local
//
//	Parser
//	  Description: Parser converts cron spec strings into cron.Schedules.
//	  Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//	Chain
//	  Description: Wrap submitted jobs to customize behavior.
//	  Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	cronInstance := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger(),
		location:  time.Local,
		parser:    NewStandardParser(),
	}
	for _, opt := range opts {
		opt(cronInstance)
	}

	return cronInstance
}

const idleTimerDuration = 100000 * time.Hour

// FuncJob is a wrapper that turns a func() into a cron.Job.
type FuncJob func()

// Run executes the wrapped func.
func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, fmt.Errorf("parse schedule %q: %w", spec, err)
	}

	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	c.nextID++

	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}

	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan

		return <-replyChan
	}

	return c.entrySnapshot()
}

// Location gets the time zone location.
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}

	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		return
	}

	c.running = true
	go c.schedulerLoop()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()

		return
	}

	c.running = true
	c.runningMu.Unlock()
	c.schedulerLoop()
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running {
		c.stop <- struct{}{}

		c.running = false
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()

	return ctx
}

// schedulerLoop runs the scheduler. It remains private because access to the
// running state is coordinated by Start and Run.
func (c *Cron) schedulerLoop() {
	c.logger.Info("start")

	now := c.initializeEntries()

	for {
		sortEntriesByNext(c.entries)

		var shouldStop bool

		now, shouldStop = c.processSchedulerEvent(now)
		if shouldStop {
			return
		}
	}
}

func sortEntriesByNext(entries []*Entry) {
	slices.SortFunc(entries, compareEntryNext)
}

func compareEntryNext(left, right *Entry) int {
	leftNext := left.Next
	rightNext := right.Next

	switch {
	case leftNext.IsZero() && rightNext.IsZero():
		return compareEntryID(left, right)
	case leftNext.IsZero():
		return 1
	case rightNext.IsZero():
		return -1
	case leftNext.Before(rightNext):
		return -1
	case rightNext.Before(leftNext):
		return 1
	default:
		return compareEntryID(left, right)
	}
}

func compareEntryID(left, right *Entry) int {
	if left.ID < right.ID {
		return -1
	}

	if left.ID > right.ID {
		return 1
	}

	return 0
}

func (c *Cron) initializeEntries() time.Time {
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	return now
}

func (c *Cron) processSchedulerEvent(now time.Time) (time.Time, bool) {
	timer := c.newSchedulerTimer(now)
	defer timer.Stop()

	for {
		select {
		case firedAt := <-timer.C:
			return c.handleTimerFired(firedAt), false
		case newEntry := <-c.add:
			return c.handleEntryAdded(newEntry), false
		case replyChan := <-c.snapshot:
			replyChan <- c.entrySnapshot()
		case <-c.stop:
			c.logger.Info("stop")

			return now, true
		case id := <-c.remove:
			return c.handleEntryRemoved(id), false
		}
	}
}

func (c *Cron) newSchedulerTimer(now time.Time) *time.Timer {
	if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
		// If there are no entries yet, just sleep - it still handles new entries
		// and stop requests.
		return time.NewTimer(idleTimerDuration)
	}

	return time.NewTimer(c.entries[0].Next.Sub(now))
}

func (c *Cron) handleTimerFired(firedAt time.Time) time.Time {
	now := firedAt.In(c.location)
	c.logger.Info("wake", "now", now)
	c.runDueEntries(now)

	return now
}

func (c *Cron) runDueEntries(now time.Time) {
	for _, entry := range c.entries {
		if entry.Next.After(now) || entry.Next.IsZero() {
			break
		}

		c.startJob(entry.WrappedJob)
		entry.Prev = entry.Next
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("run", "now", now, "entry", entry.ID, "next", entry.Next)
	}
}

func (c *Cron) handleEntryAdded(newEntry *Entry) time.Time {
	now := c.now()
	newEntry.Next = newEntry.Schedule.Next(now)
	c.entries = append(c.entries, newEntry)
	c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

	return now
}

func (c *Cron) handleEntryRemoved(id EntryID) time.Time {
	now := c.now()
	c.removeEntry(id)
	c.logger.Info("removed", "entry", id)

	return now
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Go(func() {
		j.Run()
	})
}

// now returns current time in c location.
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	entries := make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}

	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}

	c.entries = entries
}
