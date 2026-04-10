package cron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// ErrAlreadyRunning is returned by [Cron.Run] when the scheduler is already
// running in another goroutine.
var ErrAlreadyRunning = errors.New("cron: already running")

// Cron keeps track of any number of entries, invoking the associated job as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries    []*Entry
	chain      Chain
	add        chan *Entry
	remove     chan EntryID
	snapshot   chan chan []Entry
	running    atomic.Bool
	logger     *slog.Logger
	runningMu  sync.Mutex
	location   *time.Location
	parser     Parser
	clock      Clock
	nextID     EntryID
	jobWaiter  sync.WaitGroup
	rootCtx    context.Context //nolint:containedctx // stored to propagate cancellation to in-flight jobs
	rootCancel context.CancelFunc
	loopDone   chan struct{}
	onError    ErrorFunc
	hooks      EventHooks
}

// Parser turns a cron spec string into a [Schedule]. The default
// implementation is [SpecParser]; callers can supply their own by passing
// [WithParser].
type Parser interface {
	Parse(spec string) (Schedule, error)
}

// Job is the unit of work scheduled by [Cron]. Implementations should honor
// the provided context: when ctx is cancelled the job is expected to return
// promptly. A non-nil error is logged by the scheduler but does not stop
// future executions.
type Job interface {
	Run(ctx context.Context) error
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(next time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance.
type EntryID int

// Entry consists of a schedule and the job to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Name is an optional human-readable label for this entry, useful for
	// logging, metrics, and debugging. It is set via [Cron.AddNamedFunc],
	// [Cron.AddNamedJob], or [Cron.ScheduleNamed].
	Name string

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable.
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// Job is the job that was submitted to cron, kept around so callers can
	// recover it via [Cron.Entries] or [Cron.Entry].
	Job Job

	// wrappedJob is the chain-wrapped job actually executed on activation.
	wrappedJob Job
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
//	  Default:     An empty chain; jobs are executed as-is.
//
//	Clock
//	  Description: Source of wall-clock time and timers used by the scheduler.
//	  Default:     A clock backed by time.Now / time.NewTimer.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	cronInstance := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		runningMu: sync.Mutex{},
		logger:    DefaultLogger(),
		location:  time.Local,
		parser:    NewStandardParser(),
		clock:     SystemClock(),
	}
	for _, opt := range opts {
		opt(cronInstance)
	}

	return cronInstance
}

const idleTimerDuration = 100000 * time.Hour

// FuncJob adapts an ordinary function into a [Job].
type FuncJob func(ctx context.Context) error

// Run executes the wrapped func.
func (f FuncJob) Run(ctx context.Context) error { return f(ctx) }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func(ctx context.Context) error) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	return c.parseAndSchedule("", spec, cmd)
}

// AddNamedFunc is like [Cron.AddFunc] but assigns a human-readable name to the
// entry. The name appears in log messages, [Entry.Name], and event hooks.
func (c *Cron) AddNamedFunc(name, spec string, cmd func(ctx context.Context) error) (EntryID, error) {
	return c.AddNamedJob(name, spec, FuncJob(cmd))
}

// AddNamedJob is like [Cron.AddJob] but assigns a human-readable name to the
// entry. The name appears in log messages, [Entry.Name], and event hooks.
func (c *Cron) AddNamedJob(name, spec string, cmd Job) (EntryID, error) {
	return c.parseAndSchedule(name, spec, cmd)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	return c.scheduleEntry("", schedule, cmd)
}

// ScheduleNamed is like [Cron.Schedule] but assigns a human-readable name to
// the entry. The name appears in log messages, [Entry.Name], and event hooks.
func (c *Cron) ScheduleNamed(name string, schedule Schedule, cmd Job) EntryID {
	return c.scheduleEntry(name, schedule, cmd)
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()

	if !c.running.Load() {
		snap := c.entrySnapshot()
		c.runningMu.Unlock()

		return snap
	}

	snapCh := c.snapshot
	loopDone := c.loopDone
	c.runningMu.Unlock()

	replyChan := make(chan []Entry, 1)
	select {
	case snapCh <- replyChan:
		return <-replyChan
	case <-loopDone:
		c.runningMu.Lock()
		defer c.runningMu.Unlock()

		return c.entrySnapshot()
	}
}

// Location gets the time zone location.
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or the zero Entry if it could
// not be found.
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

	if !c.running.Load() {
		c.removeEntry(id)
		c.runningMu.Unlock()

		return
	}

	removeCh := c.remove
	loopDone := c.loopDone
	c.runningMu.Unlock()

	select {
	case removeCh <- id:
	case <-loopDone:
		c.runningMu.Lock()
		c.removeEntry(id)
		c.runningMu.Unlock()
	}
}

// Start launches the scheduler in its own goroutine bound to ctx. When ctx is
// cancelled the scheduler exits; any jobs already in flight are allowed to
// finish. Calling Start on an already-running scheduler is a no-op.
func (c *Cron) Start(ctx context.Context) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running.Load() {
		return
	}

	loopCtx := c.enterRunning(ctx)
	loopDone := c.loopDone

	go func() {
		c.schedulerLoop(loopCtx)
		c.markStopped(loopDone)
	}()
}

// Run executes the scheduler synchronously on the calling goroutine until ctx
// is cancelled. It returns [ErrAlreadyRunning] if another goroutine is already
// running the scheduler.
func (c *Cron) Run(ctx context.Context) error {
	c.runningMu.Lock()

	if c.running.Load() {
		c.runningMu.Unlock()

		return ErrAlreadyRunning
	}

	loopCtx := c.enterRunning(ctx)
	loopDone := c.loopDone
	c.runningMu.Unlock()

	c.schedulerLoop(loopCtx)
	c.markStopped(loopDone)

	return nil
}

// Stop cancels the running scheduler and waits for in-flight jobs to finish,
// bounded by the provided context. It returns ctx.Err() if the context is
// cancelled before all jobs complete. Calling Stop on a scheduler that is not
// running is a no-op and returns nil.
func (c *Cron) Stop(ctx context.Context) error {
	c.runningMu.Lock()
	if c.rootCancel != nil {
		c.rootCancel()
	}

	loopDone := c.loopDone
	c.runningMu.Unlock()

	if loopDone != nil {
		select {
		case <-loopDone:
		case <-ctx.Done():
			return fmt.Errorf("cron: stop: %w", ctx.Err())
		}
	}

	done := make(chan struct{})

	go func() {
		c.jobWaiter.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cron: stop: %w", ctx.Err())
	}
}

func (c *Cron) parseAndSchedule(name, spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, fmt.Errorf("parse schedule %q: %w", spec, err)
	}

	return c.scheduleEntry(name, schedule, cmd), nil
}

func (c *Cron) scheduleEntry(name string, sched Schedule, cmd Job) EntryID {
	c.runningMu.Lock()

	c.nextID++

	entry := &Entry{
		ID:         c.nextID,
		Name:       name,
		Schedule:   sched,
		Job:        cmd,
		wrappedJob: c.chain.Then(cmd),
	}

	if !c.running.Load() {
		c.entries = append(c.entries, entry)
		c.runningMu.Unlock()

		return entry.ID
	}

	addCh := c.add
	loopDone := c.loopDone
	c.runningMu.Unlock()

	select {
	case addCh <- entry:
	case <-loopDone:
		// Scheduler loop has exited; append directly.
		c.runningMu.Lock()
		c.entries = append(c.entries, entry)
		c.runningMu.Unlock()
	}

	return entry.ID
}

// enterRunning must be called with runningMu held. It sets up the root context
// derived from the caller's ctx and marks the scheduler as running.
func (c *Cron) enterRunning(ctx context.Context) context.Context {
	c.running.Store(true)
	c.rootCtx, c.rootCancel = context.WithCancel(ctx) //nolint:gosec // G118: rootCancel is stored and called by Stop
	c.loopDone = make(chan struct{})

	return c.rootCtx
}

// markStopped clears the running flag once the scheduler loop has exited. It
// must not acquire runningMu: Schedule/Remove/Entries may hold it while
// waiting on loopDone.
func (c *Cron) markStopped(loopDone chan struct{}) {
	c.running.Store(false)
	close(loopDone)
}

// schedulerLoop runs the scheduler until ctx is cancelled.
func (c *Cron) schedulerLoop(ctx context.Context) {
	c.logger.Info("start")

	now := c.initializeEntries()

	for {
		sortEntriesByNext(c.entries)

		var shouldStop bool

		now, shouldStop = c.processSchedulerEvent(ctx, now)
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

func (c *Cron) processSchedulerEvent(ctx context.Context, now time.Time) (time.Time, bool) {
	timer := c.newSchedulerTimer(now)
	defer timer.Stop()

	for {
		select {
		case firedAt := <-timer.C():
			return c.handleTimerFired(ctx, firedAt), false
		case newEntry := <-c.add:
			return c.handleEntryAdded(newEntry), false
		case replyChan := <-c.snapshot:
			replyChan <- c.entrySnapshot()
		case <-ctx.Done():
			c.logger.Info("stop")

			return now, true
		case id := <-c.remove:
			return c.handleEntryRemoved(id), false
		}
	}
}

func (c *Cron) newSchedulerTimer(now time.Time) Timer {
	if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
		// If there are no entries yet, just sleep - it still handles new entries
		// and stop requests.
		return c.clock.NewTimer(idleTimerDuration)
	}

	return c.clock.NewTimer(c.entries[0].Next.Sub(now))
}

func (c *Cron) handleTimerFired(ctx context.Context, firedAt time.Time) time.Time {
	now := firedAt.In(c.location)
	c.logger.Info("wake", "now", now)
	c.runDueEntries(ctx, now)

	return now
}

func (c *Cron) runDueEntries(ctx context.Context, now time.Time) {
	for _, entry := range c.entries {
		if entry.Next.After(now) || entry.Next.IsZero() {
			break
		}

		c.startJob(ctx, entry)
		entry.Prev = entry.Next
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("run", "now", now, "entry", entry.ID, "name", entry.Name, "next", entry.Next)
	}
}

func (c *Cron) handleEntryAdded(newEntry *Entry) time.Time {
	now := c.now()
	newEntry.Next = newEntry.Schedule.Next(now)
	c.entries = append(c.entries, newEntry)
	c.logger.Info("added", "now", now, "entry", newEntry.ID, "name", newEntry.Name, "next", newEntry.Next)

	return now
}

func (c *Cron) handleEntryRemoved(id EntryID) time.Time {
	now := c.now()
	c.removeEntry(id)
	c.logger.Info("removed", "entry", id)

	return now
}

// startJob runs the entry's wrapped job in a new goroutine, firing any
// configured [EventHooks] and [ErrorFunc]. Non-nil errors are logged at Warn
// level and do not affect future executions. Hook panics are recovered and
// logged so that observability callbacks cannot crash the scheduler.
func (c *Cron) startJob(ctx context.Context, entry *Entry) {
	c.jobWaiter.Go(func() {
		c.executeJob(ctx, entry)
	})
}

func (c *Cron) executeJob(ctx context.Context, entry *Entry) {
	c.safeCallHook(func() {
		if c.hooks.OnJobStart != nil {
			c.hooks.OnJobStart(entry.ID, entry.Name)
		}
	})

	start := time.Now()

	err := entry.wrappedJob.Run(ctx)

	c.safeCallHook(func() {
		if c.hooks.OnJobComplete != nil {
			c.hooks.OnJobComplete(entry.ID, entry.Name, time.Since(start), err)
		}
	})

	if err != nil {
		c.logger.Warn("job error", "err", err, "entry", entry.ID, "name", entry.Name)

		c.safeCallHook(func() {
			if c.onError != nil {
				c.onError(entry.ID, entry.Name, err)
			}
		})
	}
}

// safeCallHook calls fn and recovers from any panic, logging it as an error.
func (c *Cron) safeCallHook(fn func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logger.Error("hook panic", "recovered", recovered)
		}
	}()

	fn()
}

// now returns current time in c location.
func (c *Cron) now() time.Time {
	return c.clock.Now().In(c.location)
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
	c.entries = slices.DeleteFunc(c.entries, func(e *Entry) bool { return e.ID == id })
}
