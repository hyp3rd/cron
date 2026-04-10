package cron

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// ErrPanic wraps a value recovered from a panicking job by [Recover]. Callers
// can use [errors.Is] to detect a recovered panic.
var ErrPanic = errors.New("cron: job panicked")

// JobWrapper decorates the given Job with some behavior.
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
func NewChain(c ...JobWrapper) Chain {
	return Chain{wrappers: c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//
//	NewChain(m1, m2, m3).Then(job)
//
// is equivalent to:
//
//	m1(m2(m3(job)))
func (c Chain) Then(j Job) Job {
	for i := range c.wrappers {
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}

	return j
}

// Recover converts panics in the wrapped job into a logged error. It is the
// recommended way to prevent a panicking job from tearing down the scheduler
// goroutine.
func Recover(logger *slog.Logger) JobWrapper {
	return func(job Job) Job {
		return FuncJob(func(ctx context.Context) (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					const size = 64 << 10

					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]

					var panicErr error
					if asErr, ok := recovered.(error); ok {
						panicErr = fmt.Errorf("%w: %w", ErrPanic, asErr)
					} else {
						panicErr = fmt.Errorf("%w: %v", ErrPanic, recovered)
					}

					logger.Error("panic", "err", panicErr, "stack", "...\n"+string(buf))
					err = panicErr
				}
			}()

			return job.Run(ctx)
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(logger *slog.Logger) JobWrapper {
	return func(job Job) Job {
		var mu sync.Mutex

		return FuncJob(func(ctx context.Context) error {
			start := time.Now()

			mu.Lock()
			defer mu.Unlock()

			if dur := time.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}

			return job.Run(ctx)
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
func SkipIfStillRunning(logger *slog.Logger) JobWrapper {
	return func(job Job) Job {
		ch := make(chan struct{}, 1)
		ch <- struct{}{}

		return FuncJob(func(ctx context.Context) error {
			select {
			case v := <-ch:
				defer func() { ch <- v }()

				return job.Run(ctx)
			default:
				logger.Info("skip")

				return nil
			}
		})
	}
}
