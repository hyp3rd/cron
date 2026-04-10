package cron

import "time"

// Clock abstracts the wall clock so that Cron can be driven by a fake clock in
// tests. The default clock returned by [SystemClock] delegates to the standard
// library [time] package.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer creates a timer that fires after the given duration. The timer
	// must be stopped by the caller to release resources.
	NewTimer(d time.Duration) Timer
}

// Timer is the subset of [*time.Timer] required by the scheduler loop. It
// exists so that fake clocks can drive the scheduler deterministically.
type Timer interface {
	// C returns the channel on which the fire event is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It returns true if the call stops
	// the timer, or false if the timer has already expired or been stopped.
	Stop() bool
}

// SystemClock returns a [Clock] backed by [time.Now] and [time.NewTimer]. It is
// the default clock used by [New] when no [WithClock] option is supplied.
func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(d time.Duration) Timer {
	return &systemTimer{Timer: time.NewTimer(d)}
}

type systemTimer struct {
	*time.Timer
}

func (t *systemTimer) C() <-chan time.Time { return t.Timer.C }

func (t *systemTimer) Stop() bool { return t.Timer.Stop() }
