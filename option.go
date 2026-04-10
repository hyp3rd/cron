package cron

import (
	"log/slog"
	"time"
)

// Option represents a modification to the default behavior of a Cron.
type Option func(*Cron)

// WithLocation overrides the timezone of the cron instance.
func WithLocation(loc *time.Location) Option {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds overrides the parser used for interpreting job schedules to
// include a seconds field as the first one.
func WithSeconds() Option {
	return WithParser(NewSpecParser(
		Second | Minute | Hour | Dom | Month | Dow | Descriptor,
	))
}

// WithParser overrides the parser used for interpreting job schedules.
func WithParser(p Parser) Option {
	return func(c *Cron) {
		c.parser = p
	}
}

// WithChain specifies Job wrappers to apply to all jobs added to this cron.
// Refer to the Chain* functions in this package for provided wrappers.
func WithChain(wrappers ...JobWrapper) Option {
	return func(c *Cron) {
		c.chain = NewChain(wrappers...)
	}
}

// WithLogger uses the provided logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Cron) {
		c.logger = logger
	}
}

// WithClock overrides the clock used by the cron instance. It is intended
// primarily for tests that want to drive the scheduler deterministically.
func WithClock(clock Clock) Option {
	return func(c *Cron) {
		c.clock = clock
	}
}

// ErrorFunc is called after a job returns a non-nil error. It fires in the
// job's goroutine, so it must be safe for concurrent use.
type ErrorFunc func(id EntryID, name string, err error)

// WithOnError registers a callback invoked whenever a job returns a non-nil
// error. The callback receives the entry's ID, name, and the error.
func WithOnError(fn ErrorFunc) Option {
	return func(c *Cron) {
		c.onError = fn
	}
}

// EventHooks contains optional callbacks for job lifecycle events. All
// callbacks fire in the job's goroutine and must be safe for concurrent use.
type EventHooks struct {
	// OnJobStart is called just before a job begins execution.
	OnJobStart func(id EntryID, name string)

	// OnJobComplete is called after a job finishes, with the duration and
	// the error (or nil).
	OnJobComplete func(id EntryID, name string, elapsed time.Duration, err error)
}

// WithEventHooks registers lifecycle callbacks for job execution events.
func WithEventHooks(hooks EventHooks) Option {
	return func(c *Cron) {
		c.hooks = hooks
	}
}
