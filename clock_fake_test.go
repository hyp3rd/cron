package cron

import (
	"sync"
	"time"
)

// fakeClock is a deterministic Clock for testing. Callers advance time
// explicitly via Advance; pending timers fire synchronously during the
// advance, unblocking the scheduler loop without any real-time waits.
type fakeClock struct {
	mu     sync.Mutex
	cur    time.Time
	timers []*fakeTimer
}

// newFakeClock returns a fakeClock anchored at the given time.
func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{cur: t}
}

// Now returns the current fake time.
func (fc *fakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	return fc.cur
}

// NewTimer creates a timer that fires when the clock advances past now+dur. If
// dur <= 0, the timer fires immediately (buffered send).
func (fc *fakeClock) NewTimer(dur time.Duration) Timer {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	ft := &fakeTimer{
		clock:    fc,
		ch:       make(chan time.Time, 1),
		deadline: fc.cur.Add(dur),
	}

	if dur <= 0 {
		ft.ch <- fc.cur

		ft.fired = true
	} else {
		fc.timers = append(fc.timers, ft)
	}

	return ft
}

// Advance moves the clock forward by dur and fires all timers whose deadlines
// have been reached. Timers fire in deadline order.
func (fc *fakeClock) Advance(dur time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.cur = fc.cur.Add(dur)
	fc.fireTimersLocked()
}

// BlockUntilTimers spins briefly until at least count active (non-stopped,
// non-fired) timers are registered. This synchronizes with the scheduler
// goroutine, which creates a timer at the top of each loop iteration.
func (fc *fakeClock) BlockUntilTimers(count int) {
	for {
		fc.mu.Lock()

		active := 0

		for _, ft := range fc.timers {
			if !ft.stopped && !ft.fired {
				active++
			}
		}

		fc.mu.Unlock()

		if active >= count {
			return
		}

		// Yield to the scheduler goroutine.
		time.Sleep(time.Millisecond)
	}
}

func (fc *fakeClock) fireTimersLocked() {
	remaining := fc.timers[:0]

	for _, ft := range fc.timers {
		if ft.stopped {
			continue
		}

		if !fc.cur.Before(ft.deadline) {
			ft.ch <- fc.cur

			ft.fired = true
		} else {
			remaining = append(remaining, ft)
		}
	}

	fc.timers = remaining
}

// fakeTimer implements the Timer interface for fakeClock. Stop acquires the
// parent clock's mutex to avoid data races with BlockUntilTimers.
type fakeTimer struct {
	clock    *fakeClock
	ch       chan time.Time
	deadline time.Time
	fired    bool
	stopped  bool
}

func (ft *fakeTimer) C() <-chan time.Time { return ft.ch }

func (ft *fakeTimer) Stop() bool {
	ft.clock.mu.Lock()
	defer ft.clock.mu.Unlock()

	if ft.fired || ft.stopped {
		return false
	}

	ft.stopped = true

	return true
}
