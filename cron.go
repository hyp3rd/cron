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
	currentRun *runLifecycle
	runs       []*runLifecycle
	onError    ErrorFunc
	hooks      EventHooks
}

type runLifecycle struct {
	schedulerCtx    context.Context //nolint:containedctx // stored to coordinate graceful and forced shutdown across a scheduler run
	schedulerCancel context.CancelFunc
	jobCtx          context.Context //nolint:containedctx // stored to propagate
	// cancellation semantics to in-flight jobs for a scheduler run
	jobCancel  context.CancelFunc
	loopDone   chan struct{}
	jobsDone   chan struct{}
	activeJobs int
}

// Parser turns a cron spec string into a [Schedule]. The default
// implementation is [SpecParser]; callers can supply their own by passing
// [WithParser].
type Parser interface {
	Parse(spec string) (Schedule, error)
}

// Job is the unit of work scheduled by [Cron]. Implementations should honor
// the provided context: when ctx is cancelled the job is expected to return
// promptly. Job contexts are cancelled when the parent [Cron.Start] / [Cron.Run]
// context is cancelled or when [Cron.Stop] is called. A non-nil error is logged
// by the scheduler but does not stop future executions.
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
	loopDone := c.currentRun.loopDone
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
	loopDone := c.currentRun.loopDone
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
// cancelled the scheduler exits and running job contexts are cancelled. Calling
// Start on an already-running scheduler is a no-op.
func (c *Cron) Start(ctx context.Context) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if c.running.Load() {
		return
	}

	run := c.enterRunning(ctx)

	go func() {
		c.schedulerLoop(run)
		c.markStopped(run)
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

	run := c.enterRunning(ctx)
	c.runningMu.Unlock()

	c.schedulerLoop(run)
	c.markStopped(run)

	return nil
}

// Stop cancels the running scheduler and the contexts already handed to
// in-flight jobs, then waits for those jobs to finish, bounded by the provided
// context. It returns ctx.Err() if the context is cancelled before all jobs
// complete. Calling Stop on a scheduler that is not running is a no-op and
// returns nil.
func (c *Cron) Stop(ctx context.Context) error {
	return c.stopRuns(ctx, "stop", true)
}

// Shutdown stops the scheduler and waits for in-flight jobs to finish without
// cancelling their contexts. The wait is bounded by the provided context. It
// returns ctx.Err() if the context is cancelled before all jobs complete.
func (c *Cron) Shutdown(ctx context.Context) error {
	return c.stopRuns(ctx, "shutdown", false)
}

func (c *Cron) stopRuns(ctx context.Context, op string, cancelJobs bool) error {
	c.runningMu.Lock()
	c.cleanupCompletedRunsLocked()

	runs := append([]*runLifecycle(nil), c.runs...)
	for _, run := range runs {
		run.schedulerCancel()

		if cancelJobs {
			run.jobCancel()
		}
	}
	c.runningMu.Unlock()

	for _, run := range runs {
		err := waitForRunChannel(ctx, run.loopDone)
		if err != nil {
			return fmt.Errorf("cron: %s: %w", op, err)
		}
	}

	for _, run := range runs {
		err := c.waitForRunJobs(ctx, run)
		if err != nil {
			return fmt.Errorf("cron: %s: %w", op, err)
		}
	}

	return nil
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
	loopDone := c.currentRun.loopDone
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

// enterRunning must be called with runningMu held. It sets up independent
// scheduler and job contexts derived from the caller's ctx and marks the
// scheduler as running.
func (c *Cron) enterRunning(ctx context.Context) *runLifecycle {
	c.cleanupCompletedRunsLocked()

	run := &runLifecycle{
		loopDone: make(chan struct{}),
	}
	run.schedulerCtx, run.schedulerCancel = context.WithCancel(ctx) //nolint:gosec // G118: cancellation is retained for Stop/Shutdown
	run.jobCtx, run.jobCancel = context.WithCancel(ctx)             //nolint:gosec // G118: cancellation is retained for Stop/Shutdown

	c.running.Store(true)
	c.currentRun = run
	c.runs = append(c.runs, run)

	return run
}

// markStopped clears the running flag once the scheduler loop has exited. It
// must not acquire runningMu: Schedule/Remove/Entries may hold it while
// waiting on loopDone.
func (c *Cron) markStopped(run *runLifecycle) {
	c.running.Store(false)
	close(run.loopDone)
}

// schedulerLoop runs the scheduler until ctx is cancelled.
func (c *Cron) schedulerLoop(run *runLifecycle) {
	c.logger.Info("start")

	now := c.initializeEntries()

	for {
		sortEntriesByNext(c.entries)

		var shouldStop bool

		now, shouldStop = c.processSchedulerEvent(run, now)
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

func (c *Cron) processSchedulerEvent(run *runLifecycle, now time.Time) (time.Time, bool) {
	timer := c.newSchedulerTimer(now)
	defer timer.Stop()

	for {
		select {
		case firedAt := <-timer.C():
			return c.handleTimerFired(run, firedAt), false
		case newEntry := <-c.add:
			return c.handleEntryAdded(newEntry), false
		case replyChan := <-c.snapshot:
			replyChan <- c.entrySnapshot()
		case <-run.schedulerCtx.Done():
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

func (c *Cron) handleTimerFired(run *runLifecycle, firedAt time.Time) time.Time {
	now := firedAt.In(c.location)
	c.logger.Info("wake", "now", now)
	c.runDueEntries(run, now)

	return now
}

func (c *Cron) runDueEntries(run *runLifecycle, now time.Time) {
	for _, entry := range c.entries {
		if entry.Next.After(now) || entry.Next.IsZero() {
			break
		}

		c.startJob(run, entry)
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
func (c *Cron) startJob(run *runLifecycle, entry *Entry) {
	c.runningMu.Lock()
	if run.activeJobs == 0 {
		run.jobsDone = make(chan struct{})
	}

	run.activeJobs++
	c.runningMu.Unlock()

	go func() {
		defer c.finishJob(run)

		c.executeJob(run.jobCtx, entry)
	}()
}

func (c *Cron) finishJob(run *runLifecycle) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if run.activeJobs == 0 {
		return
	}

	run.activeJobs--
	if run.activeJobs == 0 && run.jobsDone != nil {
		close(run.jobsDone)
		run.jobsDone = nil
	}

	c.cleanupCompletedRunsLocked()
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

func (c *Cron) waitForRunJobs(ctx context.Context, run *runLifecycle) error {
	c.runningMu.Lock()
	if run.activeJobs == 0 {
		c.cleanupCompletedRunsLocked()
		c.runningMu.Unlock()

		return nil
	}

	jobsDone := run.jobsDone
	c.runningMu.Unlock()

	return waitForRunChannel(ctx, jobsDone)
}

func (c *Cron) cleanupCompletedRunsLocked() {
	if len(c.runs) == 0 {
		return
	}

	filtered := c.runs[:0]
	for _, run := range c.runs {
		if run.activeJobs == 0 && isClosed(run.loopDone) {
			if c.currentRun == run {
				c.currentRun = nil
			}

			continue
		}

		filtered = append(filtered, run)
	}

	c.runs = filtered
}

func waitForRunChannel(ctx context.Context, done <-chan struct{}) error {
	if done == nil {
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	}
}

func isClosed(done <-chan struct{}) bool {
	if done == nil {
		return false
	}

	select {
	case <-done:
		return true
	default:
		return false
	}
}
