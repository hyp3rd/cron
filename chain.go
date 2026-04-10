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

// MaxConcurrent allows up to limit concurrent invocations of the wrapped job.
// Additional invocations beyond the limit are skipped and logged at Info level.
// It generalizes [SkipIfStillRunning], which is equivalent to MaxConcurrent
// with a limit of 1.
func MaxConcurrent(limit int, logger *slog.Logger) JobWrapper {
	limit = max(limit, 1)

	return func(job Job) Job {
		sem := make(chan struct{}, limit)

		for range limit {
			sem <- struct{}{}
		}

		return FuncJob(func(ctx context.Context) error {
			select {
			case token := <-sem:
				defer func() { sem <- token }()

				return job.Run(ctx)
			default:
				logger.Info("skip", "reason", "max_concurrent", "limit", limit)

				return nil
			}
		})
	}
}

// Timeout cancels the job's context after the given duration. If the job does
// not return before the deadline, the context passed to it is cancelled. The
// wrapper waits for the job to return and reports any error (including
// [context.DeadlineExceeded]) to the caller.
//
// Note: the wrapper does not forcefully kill the job goroutine. The job must
// honor ctx.Done() for cancellation to take effect.
func Timeout(duration time.Duration) JobWrapper {
	return func(job Job) Job {
		return FuncJob(func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()

			return job.Run(ctx)
		})
	}
}

// RetryOnError retries the wrapped job up to maxRetries times when it returns a
// non-nil error. Between attempts it waits for the given backoff duration (or
// until the context is cancelled). A zero backoff retries immediately.
//
// The backoff uses real wall-clock time, not the scheduler's [Clock] interface.
func RetryOnError(maxRetries int, backoff time.Duration) JobWrapper {
	maxRetries = max(maxRetries, 0)

	return func(job Job) Job {
		return FuncJob(func(ctx context.Context) error {
			return executeWithRetry(ctx, job, maxRetries, backoff)
		})
	}
}

func executeWithRetry(ctx context.Context, job Job, maxRetries int, backoff time.Duration) error {
	var err error

	for attempt := range maxRetries + 1 {
		err = job.Run(ctx)
		if err == nil {
			return nil
		}

		if attempt == maxRetries {
			break
		}

		waitErr := retryBackoff(ctx, backoff)
		if waitErr != nil {
			return waitErr
		}
	}

	return err
}

// retryBackoff waits for the given duration or until the context is cancelled.
// A zero duration returns immediately.
func retryBackoff(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return nil
	}

	timer := time.NewTimer(backoff)

	select {
	case <-ctx.Done():
		timer.Stop()

		return fmt.Errorf("retry backoff: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
