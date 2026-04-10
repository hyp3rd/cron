package cron

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	everySecondSpec         = "* * * * * ?"
	everySecondWithSeconds  = "* * * * * *"
	januaryFirstSpec        = "0 0 0 1 1 ?"
	decemberThirtyFirstSpec = "0 0 0 31 12 ?"
	invalidFebruarySpec     = "0 0 0 30 Feb ?"
	januaryFirstOffsetSpec  = "1 0 0 1 1 ?"

	expectedTwoFiringsMessage = "expected job fires 2 times"
	wrongJobRetrievedMessage  = "wrong job retrieved:"

	slowStopJobDelay      = 2 * time.Second
	waitForStopCheck      = 750 * time.Millisecond
	waitForStopCompletion = 1500 * time.Millisecond

	unexpectedStopError = "unexpected stop error: %v"
)

// baseTime is a fixed instant used by fake-clock tests. It is midnight UTC on
// a Monday in a neutral month so that January/December-specific specs never
// fire unexpectedly.
var baseTime = time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC) //nolint:gochecknoglobals // test constant

// awaitTimeout is the real-time safety net for tests that advance a fake clock
// and then wait for a goroutine to observe the result.
const awaitTimeout = 200 * time.Millisecond

type syncWriter struct {
	wr bytes.Buffer
	m  sync.Mutex
}

func (sw *syncWriter) Write(data []byte) (int, error) {
	sw.m.Lock()
	writtenBytes, err := sw.wr.Write(data)
	sw.m.Unlock()

	if err != nil {
		return writtenBytes, fmt.Errorf("write sync buffer: %w", err)
	}

	return writtenBytes, nil
}

func (sw *syncWriter) String() string {
	sw.m.Lock()
	defer sw.m.Unlock()

	return sw.wr.String()
}

func newBufLogger(sw *syncWriter) *slog.Logger {
	return slog.New(slog.NewTextHandler(sw, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// noop is a job body that does nothing and never errors.
func noop(_ context.Context) error { return nil }

// done wraps wg.Done into a FuncJob-compatible closure.
func done(wg *sync.WaitGroup) func(context.Context) error {
	return func(_ context.Context) error {
		wg.Done()

		return nil
	}
}

// startCron starts cron bound to a per-test context and registers a cleanup
// that stops it with a generous deadline.
func startCron(t *testing.T, cron *Cron) {
	t.Helper()

	runCtx, cancelRun := context.WithCancel(context.Background())
	cron.Start(runCtx)

	t.Cleanup(func() {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()

		_ = cron.Stop(stopCtx) //nolint:errcheck // best-effort cleanup

		cancelRun()
	})
}

// awaitWg waits for wg to complete with a short real-time safety net.
func awaitWg(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()

	select {
	case <-wait(wg):
	case <-time.After(awaitTimeout):
		t.Fatal("timed out waiting for jobs to complete")
	}
}

// newFakeWithSeconds creates a Cron with seconds-level parsing backed by a
// fake clock anchored at baseTime.
func newFakeWithSeconds() (*Cron, *fakeClock) {
	fc := newFakeClock(baseTime)

	return New(WithParser(testParserWithSeconds()), WithChain(), WithClock(fc)), fc
}

func TestFuncPanicRecovery(t *testing.T) {
	t.Parallel()

	var buf syncWriter

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(Recover(newBufLogger(&buf))),
		WithClock(fc),
	)

	mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		panic("YOLO")
	})

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)

	// Give the goroutine time to log the panic.
	time.Sleep(10 * time.Millisecond)

	if !strings.Contains(buf.String(), "YOLO") {
		t.Error("expected a panic to be logged, got none")
	}
}

type DummyJob struct{}

func (DummyJob) Run(_ context.Context) error {
	panic("YOLO")
}

func TestJobPanicRecovery(t *testing.T) {
	t.Parallel()

	var job DummyJob

	var buf syncWriter

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(Recover(newBufLogger(&buf))),
		WithClock(fc),
	)

	mustAddJob(t, cron, everySecondSpec, job)

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)

	time.Sleep(10 * time.Millisecond)

	if !strings.Contains(buf.String(), "YOLO") {
		t.Error("expected a panic to be logged, got none")
	}
}

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	t.Parallel()

	cron, _ := newFakeWithSeconds()
	cron.Start(context.Background())

	err := cron.Stop(context.Background())
	if err != nil {
		t.Fatalf("expected cron to stop immediately: %v", err)
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	cron, fc := newFakeWithSeconds()
	cron.Start(context.Background())
	_ = cron.Stop(context.Background()) //nolint:errcheck // tested elsewhere

	mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		calls.Add(1)

		return nil
	})

	// Cron is stopped — advancing the clock should not fire the job.
	fc.Advance(2 * time.Second)
	time.Sleep(10 * time.Millisecond)

	if c := calls.Load(); c != 0 {
		t.Fatalf("expected no job runs after stop, got %d", c)
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, fc := newFakeWithSeconds()

	startCron(t, cron)
	fc.BlockUntilTimers(1) // wait for idle timer

	mustAddFunc(t, cron, everySecondSpec, done(wg))

	// Let the scheduler process the add event and create a new timer.
	time.Sleep(5 * time.Millisecond)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test for #34. Adding a job after calling start results in multiple job invocations.
func TestAddWhileRunningWithDelay(t *testing.T) {
	t.Parallel()

	cron, fc := newFakeWithSeconds()

	startCron(t, cron)
	fc.BlockUntilTimers(1) // idle timer

	// Advance 5 seconds — the idle timer (100000h) does not fire, but
	// the internal clock moves forward so that fc.Now() returns baseTime+5s.
	fc.Advance(5 * time.Second)

	var calls atomic.Int64

	// Adding a job sends on the add channel, waking the scheduler.
	// The scheduler computes Next from fc.Now() (baseTime+5s).
	wg := &sync.WaitGroup{}
	wg.Add(1)

	mustAddFunc(t, cron, everySecondWithSeconds, func(_ context.Context) error {
		calls.Add(1)
		wg.Done()

		return nil
	})

	// Let the scheduler goroutine stop the old idle timer and create
	// a new timer for the next second.
	time.Sleep(5 * time.Millisecond)

	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	if c := calls.Load(); c != 1 {
		t.Errorf("called %d times, expected 1\n", c)
	}
}

// Add a job, remove a job, start cron, expect nothing runs.
func TestRemoveBeforeRunning(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	cron, fc := newFakeWithSeconds()
	id := mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		calls.Add(1)

		return nil
	})
	cron.Remove(id)

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	time.Sleep(10 * time.Millisecond)

	if c := calls.Load(); c != 0 {
		t.Fatalf("expected removed job not to run, got %d calls", c)
	}
}

// Start cron, add a job, remove it, expect it doesn't run.
func TestRemoveWhileRunning(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	cron, fc := newFakeWithSeconds()

	startCron(t, cron)
	fc.BlockUntilTimers(1) // wait for idle timer

	id := mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		calls.Add(1)

		return nil
	})
	cron.Remove(id)

	// Let the scheduler process add + remove events.
	time.Sleep(5 * time.Millisecond)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	time.Sleep(10 * time.Millisecond)

	if c := calls.Load(); c != 0 {
		t.Fatalf("expected removed job not to run, got %d calls", c)
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	fc := newFakeClock(baseTime)
	cron := New(WithClock(fc))
	mustAddFunc(t, cron, "@every 2s", done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)

	// Advance 1 second, call Entries mid-cycle.
	fc.Advance(1 * time.Second)
	time.Sleep(5 * time.Millisecond)
	cron.Entries()

	// Advance another second — the job should fire at the 2s mark.
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test that the entries are correctly sorted.
func TestMultipleEntries(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, noop)
	mustAddFunc(t, cron, everySecondSpec, done(wg))
	id1 := mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		t.Fatal()

		return nil
	})
	id2 := mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		t.Fatal()

		return nil
	})
	mustAddFunc(t, cron, decemberThirtyFirstSpec, noop)
	mustAddFunc(t, cron, everySecondSpec, done(wg))

	cron.Remove(id1)

	startCron(t, cron)
	fc.BlockUntilTimers(1) // wait for scheduler to be ready

	cron.Remove(id2)

	// Let the scheduler process the remove event.
	time.Sleep(5 * time.Millisecond)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, noop)
	mustAddFunc(t, cron, decemberThirtyFirstSpec, noop)
	mustAddFunc(t, cron, everySecondSpec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

func TestRunningMultipleSchedules(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, noop)
	mustAddFunc(t, cron, decemberThirtyFirstSpec, noop)
	mustAddFunc(t, cron, everySecondSpec, done(wg))
	cron.Schedule(Every(time.Minute), FuncJob(noop))
	cron.Schedule(Every(time.Second), FuncJob(done(wg)))
	cron.Schedule(Every(time.Hour), FuncJob(noop))

	startCron(t, cron)

	// Two "every second" entries: one advance fires both, satisfying wg.Add(2).
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test that the cron is run in the local time zone (as opposed to UTC).
// With a fake clock we pick a fixed time and schedule relative to it,
// eliminating the old secondsBoundaryThreshold hack.
func TestLocalTimezone(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	// Seconds 1 and 2 relative to baseTime will fire.
	fc := newFakeClock(baseTime)
	cron := New(WithParser(testParserWithSeconds()), WithChain(), WithClock(fc), WithLocation(time.UTC))

	spec := fmt.Sprintf("1,2 %d %d %d %d ?",
		baseTime.Minute(), baseTime.Hour(), baseTime.Day(), baseTime.Month())
	mustAddFunc(t, cron, spec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test that the cron is run in the given time zone (as opposed to local).
func TestNonLocalTimezone(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	loc, err := time.LoadLocation("Atlantic/Cape_Verde")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}

	// Anchor in Cape Verde time so the schedule matches.
	cvTime := baseTime.In(loc)
	fc := newFakeClock(baseTime)
	cron := New(WithLocation(loc), WithParser(testParserWithSeconds()), WithClock(fc))

	spec := fmt.Sprintf("1,2 %d %d %d %d ?",
		cvTime.Minute(), cvTime.Hour(), cvTime.Day(), cvTime.Month())
	mustAddFunc(t, cron, spec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)
}

// Test that calling Stop before Start silently returns without blocking.
func TestStopWithoutStart(t *testing.T) {
	t.Parallel()

	cron := New()

	err := cron.Stop(context.Background())
	if err != nil {
		t.Errorf(unexpectedStopError, err)
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run(_ context.Context) error {
	t.wg.Done()

	return nil
}

// Test that adding an invalid job spec returns an error.
func TestInvalidJobSpec(t *testing.T) {
	t.Parallel()

	cron := New()

	_, err := cron.AddJob("this will not parse", nil)
	if err == nil {
		t.Error("expected an error with invalid spec, got nil")
	}
}

// Test blocking run method behaves as Start().
func TestBlockingRun(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, done(wg))

	runCtx, cancelRun := context.WithCancel(context.Background())
	unblockChan := make(chan struct{})

	go func() {
		_ = cron.Run(runCtx) //nolint:errcheck // tested elsewhere

		close(unblockChan)
	}()

	t.Cleanup(func() {
		cancelRun()
		<-unblockChan
	})

	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)

	select {
	case <-time.After(awaitTimeout):
		t.Error("expected job fires")
	case <-unblockChan:
		t.Error("expected that Run() blocks")
	case <-wait(wg):
	}
}

// TestRunReturnsErrAlreadyRunning verifies a second Run returns an error while
// the first is active.
func TestRunReturnsErrAlreadyRunning(t *testing.T) {
	t.Parallel()

	cron, _ := newFakeWithSeconds()

	runCtx, cancelRun := context.WithCancel(context.Background())
	started := make(chan struct{})

	go func() {
		close(started)

		_ = cron.Run(runCtx) //nolint:errcheck // tested elsewhere
	}()

	<-started
	// Give the goroutine a chance to acquire the running flag.
	time.Sleep(10 * time.Millisecond)

	err := cron.Run(context.Background())
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("expected ErrAlreadyRunning, got %v", err)
	}

	cancelRun()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
	defer cancelStop()

	_ = cron.Stop(stopCtx) //nolint:errcheck // best-effort cleanup
}

// Test that double-Start is a no-op.
func TestStartNoop(t *testing.T) {
	t.Parallel()

	tickChan := make(chan struct{}, 2)

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, func(_ context.Context) error {
		tickChan <- struct{}{}

		return nil
	})

	startCron(t, cron)
	fc.BlockUntilTimers(1)

	// First tick.
	fc.Advance(1 * time.Second)

	select {
	case <-tickChan:
	case <-time.After(awaitTimeout):
		t.Fatal("first tick did not arrive")
	}

	// Double-start should be a no-op.
	cron.Start(context.Background())

	// Second tick.
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)

	select {
	case <-tickChan:
	case <-time.After(awaitTimeout):
		t.Fatal("second tick did not arrive")
	}

	// No third tick should appear.
	select {
	case <-time.After(10 * time.Millisecond):
	case <-tickChan:
		t.Error("expected job fires exactly twice")
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, fc := newFakeWithSeconds()
	mustAddJob(t, cron, invalidFebruarySpec, testJob{wg, "job0"})
	mustAddJob(t, cron, januaryFirstSpec, testJob{wg, "job1"})
	job2 := mustAddJob(t, cron, everySecondSpec, testJob{wg, "job2"})
	mustAddJob(t, cron, januaryFirstOffsetSpec, testJob{wg, "job3"})
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"})
	job5 := cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"})

	// Test getting an Entry pre-Start.
	if actualName := requireTestJobName(t, cron, job2); actualName != "job2" {
		t.Error(wrongJobRetrievedMessage, actualName)
	}

	if actualName := requireTestJobName(t, cron, job5); actualName != "job5" {
		t.Error(wrongJobRetrievedMessage, actualName)
	}

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	entries := cron.Entries()

	actuals := make([]string, 0, len(entries))
	for _, entry := range entries {
		actuals = append(actuals, requireType[testJob](t, entry.Job).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Fatalf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
		}
	}

	// Test getting Entries.
	if actualName := requireTestJobName(t, cron, job2); actualName != "job2" {
		t.Error(wrongJobRetrievedMessage, actualName)
	}

	if actualName := requireTestJobName(t, cron, job5); actualName != "job5" {
		t.Error(wrongJobRetrievedMessage, actualName)
	}
}

// Issue #206
// Ensure that the next run of a job after removing an entry is accurate.
// This test has time.Sleep inside the job body, so it must use real time.
func TestScheduleAfterRemoval(t *testing.T) {
	t.Parallel()

	var (
		wg1 sync.WaitGroup
		wg2 sync.WaitGroup
	)

	wg1.Add(1)
	wg2.Add(1)

	var (
		calls int
		mu    sync.Mutex
	)

	cron := newWithSeconds()
	hourJob := cron.Schedule(Every(time.Hour), FuncJob(noop))
	cron.Schedule(Every(time.Second), FuncJob(func(_ context.Context) error {
		mu.Lock()
		defer mu.Unlock()

		switch calls {
		case 0:
			wg1.Done()

			calls++
		case 1:
			time.Sleep(750 * time.Millisecond)
			cron.Remove(hourJob)

			calls++
		case 2:
			calls++

			wg2.Done()
		default:
			panic("unexpected extra call")
		}

		return nil
	}))

	startCron(t, cron)

	wg1.Wait()

	select {
	case <-time.After(3 * time.Second):
		t.Error(expectedTwoFiringsMessage)
	case <-wait(&wg2):
	}
}

type ZeroSchedule struct{}

// Next always returns the zero time, which is never.
func (*ZeroSchedule) Next(_ time.Time) time.Time {
	return time.Time{}
}

// Tests that job without time does not run.
func TestJobWithZeroTimeDoesNotRun(t *testing.T) {
	t.Parallel()

	cron, fc := newFakeWithSeconds()

	var calls atomic.Int64

	mustAddFunc(t, cron, everySecondWithSeconds, func(_ context.Context) error {
		calls.Add(1)

		return nil
	})
	cron.Schedule(new(ZeroSchedule), FuncJob(func(_ context.Context) error {
		t.Error("expected zero task will not run")

		return nil
	}))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	time.Sleep(10 * time.Millisecond)

	if calls.Load() != 1 {
		t.Errorf("called %d times, expected 1\n", calls.Load())
	}
}

func TestStopAndWait(t *testing.T) {
	t.Parallel()

	t.Run("nothing running, returns immediately", testStopAndWaitNothingRunning)
	t.Run("repeated calls to Stop", testStopAndWaitRepeatedCalls)
	t.Run("a couple fast jobs added, still returns immediately", testStopAndWaitFastJobs)
	t.Run("a couple fast jobs and a slow job added, waits for slow job", testStopAndWaitSlowJob)
}

func TestMultiThreadedStartAndStop(t *testing.T) {
	t.Parallel()

	cron := New()

	go func() {
		_ = cron.Run(context.Background()) //nolint:errcheck // tested elsewhere
	}()

	time.Sleep(2 * time.Millisecond)

	_ = cron.Stop(context.Background()) //nolint:errcheck // tested elsewhere
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)

	go func() {
		wg.Wait()

		ch <- true
	}()

	return ch
}

// newWithSeconds returns a Cron with the seconds field enabled (real clock).
func newWithSeconds() *Cron {
	return New(WithParser(testParserWithSeconds()), WithChain())
}

func requireTestJobName(t *testing.T, cron *Cron, id EntryID) string {
	t.Helper()

	return requireType[testJob](t, cron.Entry(id).Job).name
}

func testStopAndWaitNothingRunning(t *testing.T) {
	t.Parallel()

	cron, _ := newFakeWithSeconds()
	cron.Start(context.Background())

	err := cron.Stop(context.Background())
	if err != nil {
		t.Errorf(unexpectedStopError, err)
	}
}

func testStopAndWaitRepeatedCalls(t *testing.T) {
	t.Parallel()

	cron, _ := newFakeWithSeconds()
	cron.Start(context.Background())

	_ = cron.Stop(context.Background()) //nolint:errcheck // first stop

	time.Sleep(time.Millisecond)

	err := cron.Stop(context.Background())
	if err != nil {
		t.Errorf(unexpectedStopError, err)
	}
}

func testStopAndWaitFastJobs(t *testing.T) {
	t.Parallel()

	cron, fc := newFakeWithSeconds()
	mustAddFunc(t, cron, everySecondWithSeconds, noop)
	cron.Start(context.Background())
	fc.BlockUntilTimers(1)

	mustAddFunc(t, cron, everySecondWithSeconds, noop)
	mustAddFunc(t, cron, everySecondWithSeconds, noop)
	mustAddFunc(t, cron, everySecondWithSeconds, noop)

	time.Sleep(5 * time.Millisecond)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	time.Sleep(10 * time.Millisecond)

	err := cron.Stop(context.Background())
	if err != nil {
		t.Errorf(unexpectedStopError, err)
	}
}

func testStopAndWaitSlowJob(t *testing.T) {
	t.Parallel()

	slowJobStarted := make(chan struct{}, 1)
	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondWithSeconds, noop)
	cron.Start(context.Background())
	mustAddFunc(t, cron, everySecondWithSeconds, func(_ context.Context) error {
		signalJobStarted(slowJobStarted)
		time.Sleep(slowStopJobDelay)

		return nil
	})
	mustAddFunc(t, cron, everySecondWithSeconds, noop)

	waitForJobStarted(t, slowJobStarted)

	// A short deadline should trip because the slow job is still running.
	shortCtx, cancelShort := context.WithTimeout(context.Background(), waitForStopCheck)

	err := cron.Stop(shortCtx)

	cancelShort()

	if err == nil {
		t.Error("expected Stop to time out while slow job was running")
	}

	// A longer deadline should succeed once the slow job wraps up.
	longCtx, cancelLong := context.WithTimeout(context.Background(), waitForStopCompletion)
	defer cancelLong()

	err = cron.Stop(longCtx)
	if err != nil {
		t.Errorf("expected Stop to succeed, got %v", err)
	}
}

func waitForJobStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow job did not start in time")
	}
}

func signalJobStarted(started chan<- struct{}) {
	select {
	case started <- struct{}{}:
	default:
	}
}

func TestEventHooksOnSuccess(t *testing.T) {
	t.Parallel()

	var (
		startID   atomic.Int64
		startName atomic.Value
		compID    atomic.Int64
		compName  atomic.Value
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(),
		WithClock(fc),
		WithEventHooks(EventHooks{
			OnJobStart: func(id EntryID, name string) {
				startID.Store(int64(id))
				startName.Store(name)
			},
			OnJobComplete: func(id EntryID, name string, _ time.Duration, _ error) {
				compID.Store(int64(id))
				compName.Store(name)
			},
		}),
	)

	mustAddNamedFunc(t, cron, "test-job", everySecondSpec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	if startID.Load() != 1 {
		t.Errorf("OnJobStart: expected id=1, got %d", startID.Load())
	}

	if startName.Load() != "test-job" {
		t.Errorf("OnJobStart: expected name=test-job, got %v", startName.Load())
	}

	if compID.Load() != 1 {
		t.Errorf("OnJobComplete: expected id=1, got %d", compID.Load())
	}

	if compName.Load() != "test-job" {
		t.Errorf("OnJobComplete: expected name=test-job, got %v", compName.Load())
	}
}

func TestOnErrorCallback(t *testing.T) {
	t.Parallel()

	var (
		cbID   atomic.Int64
		cbName atomic.Value
		cbErr  atomic.Value
	)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(),
		WithClock(fc),
		WithLogger(DiscardLogger()),
		WithOnError(func(id EntryID, name string, err error) {
			cbID.Store(int64(id))
			cbName.Store(name)
			cbErr.Store(err.Error())
			wg.Done()
		}),
	)

	mustAddNamedFunc(t, cron, "failing", everySecondSpec, func(_ context.Context) error {
		return errors.New("boom") //nolint:err113 // test error
	})

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	if cbID.Load() != 1 {
		t.Errorf("OnError: expected id=1, got %d", cbID.Load())
	}

	if cbName.Load() != "failing" {
		t.Errorf("OnError: expected name=failing, got %v", cbName.Load())
	}

	if cbErr.Load() != "boom" {
		t.Errorf("OnError: expected err=boom, got %v", cbErr.Load())
	}
}

func TestOnErrorNotCalledOnSuccess(t *testing.T) {
	t.Parallel()

	var errorCalled atomic.Bool

	wg := &sync.WaitGroup{}
	wg.Add(1)

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(),
		WithClock(fc),
		WithOnError(func(_ EntryID, _ string, _ error) {
			errorCalled.Store(true)
		}),
	)

	mustAddFunc(t, cron, everySecondSpec, done(wg))

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	// Small yield to ensure callback would have fired.
	time.Sleep(5 * time.Millisecond)

	if errorCalled.Load() {
		t.Error("OnError should not be called on successful job")
	}
}

func TestHookPanicDoesNotCrashScheduler(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	fc := newFakeClock(baseTime)
	cron := New(
		WithParser(testParserWithSeconds()),
		WithChain(),
		WithClock(fc),
		WithLogger(DiscardLogger()),
		WithEventHooks(EventHooks{
			OnJobStart: func(_ EntryID, _ string) {
				panic("start hook panic")
			},
			OnJobComplete: func(_ EntryID, _ string, _ time.Duration, _ error) {
				panic("complete hook panic")
			},
		}),
		WithOnError(func(_ EntryID, _ string, _ error) {
			panic("error hook panic")
		}),
	)

	mustAddNamedFunc(t, cron, "panicky-hooks", everySecondSpec, func(_ context.Context) error {
		wg.Done()

		return errors.New("trigger onError") //nolint:err113 // test error
	})

	startCron(t, cron)
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	awaitWg(t, wg)

	// If we reach here, the scheduler survived all hook panics.
	// Small yield to let the hook goroutine complete.
	time.Sleep(10 * time.Millisecond)
}
