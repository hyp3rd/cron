//nolint:errcheck,gosec // examples prioritize clarity over exhaustive error handling
package cron_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/hyp3rd/cron/v4"
)

const (
	exampleEverySecond = "* * * * * *"
	exampleEveryHour   = "0 * * * *"
	exampleNextNCount  = 3
)

var errAttemptFailed = errors.New("attempt failed")

func ExampleCron_AddFunc() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cronInstance := cron.New(cron.WithSeconds())
	cronInstance.AddFunc(exampleEverySecond, func(_ context.Context) error {
		fmt.Println("tick")
		cancel()

		return nil
	})

	cronInstance.Start(ctx)
	<-ctx.Done()

	cronInstance.Stop(context.Background())

	// Output:
	// tick
}

func ExampleCron_AddNamedFunc() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cronInstance := cron.New(cron.WithSeconds())
	cronInstance.AddNamedFunc("heartbeat", exampleEverySecond, func(_ context.Context) error {
		fmt.Println("heartbeat")
		cancel()

		return nil
	})

	entry := cronInstance.Entries()[0]
	fmt.Println("name:", entry.Name)

	cronInstance.Start(ctx)
	<-ctx.Done()

	cronInstance.Stop(context.Background())

	// Output:
	// name: heartbeat
	// heartbeat
}

func ExampleNextN() {
	sched, _ := cron.ParseStandard(exampleEveryHour)

	//nolint:revive // example uses fixed date for deterministic output
	anchor := time.Date(2024, 6, 3, 12, 0, 0, 0, time.UTC)
	times := cron.NextN(sched, anchor, exampleNextNCount)

	for _, nextTime := range times {
		fmt.Println(nextTime.Format(time.DateTime))
	}

	// Output:
	// 2024-06-03 13:00:00
	// 2024-06-03 14:00:00
	// 2024-06-03 15:00:00
}

func ExampleSpecSchedule_String() {
	parser := cron.NewSpecParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	sched, _ := parser.Parse("*/5 * * * * *")
	fmt.Println(sched)

	// Output:
	// 0,5,10,15,20,25,30,35,40,45,50,55 * * * * *
}

func ExampleTimeout() {
	job := cron.NewChain(cron.Timeout(5 * time.Second)).Then(
		cron.FuncJob(func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
				fmt.Println("done")

				return nil
			}
		}),
	)

	job.Run(context.Background())

	// Output:
	// done
}

func ExampleMaxConcurrent() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	job := cron.NewChain(cron.MaxConcurrent(2, logger)).Then(
		cron.FuncJob(func(_ context.Context) error {
			fmt.Println("running")

			return nil
		}),
	)

	job.Run(context.Background())

	// Output:
	// running
}

func ExampleRetryOnError() {
	var attempts atomic.Int32

	job := cron.NewChain(cron.RetryOnError(2, time.Millisecond)).Then(
		cron.FuncJob(func(_ context.Context) error {
			attempt := attempts.Add(1)
			if attempt < 3 {
				return fmt.Errorf("%w: %d", errAttemptFailed, attempt)
			}

			fmt.Println("succeeded on attempt", attempt)

			return nil
		}),
	)

	job.Run(context.Background())

	// Output:
	// succeeded on attempt 3
}

func ExampleWithEventHooks() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cronInstance := cron.New(
		cron.WithSeconds(),
		cron.WithEventHooks(cron.EventHooks{
			OnJobStart: func(id cron.EntryID, name string) {
				fmt.Printf("start: %s (id=%d)\n", name, id)
			},
			OnJobComplete: func(id cron.EntryID, name string, _ time.Duration, _ error) {
				fmt.Printf("complete: %s (id=%d)\n", name, id)
				cancel()
			},
		}),
	)

	cronInstance.AddNamedFunc("hello", exampleEverySecond, func(_ context.Context) error {
		return nil
	})

	cronInstance.Start(ctx)
	<-ctx.Done()

	cronInstance.Stop(context.Background())

	// Output:
	// start: hello (id=1)
	// complete: hello (id=1)
}

func ExampleWithOnError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cronInstance := cron.New(
		cron.WithSeconds(),
		cron.WithLogger(cron.DiscardLogger()),
		cron.WithOnError(func(id cron.EntryID, name string, err error) {
			fmt.Printf("error in %s (id=%d): %v\n", name, id, err)
			cancel()
		}),
	)

	cronInstance.AddNamedFunc("failing-job", exampleEverySecond, func(_ context.Context) error {
		return errors.New("something went wrong") //nolint:err113 // example error
	})

	cronInstance.Start(ctx)
	<-ctx.Done()

	cronInstance.Stop(context.Background())

	// Output:
	// error in failing-job (id=1): something went wrong
}
