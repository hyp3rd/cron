package cron

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const OneSecond = 1*time.Second + 50*time.Millisecond

const (
	everySecondSpec          = "* * * * * ?"
	everySecondWithSeconds   = "* * * * * *"
	januaryFirstSpec         = "0 0 0 1 1 ?"
	decemberThirtyFirstSpec  = "0 0 0 31 12 ?"
	invalidFebruarySpec      = "0 0 0 30 Feb ?"
	januaryFirstOffsetSpec   = "1 0 0 1 1 ?"
	secondsBoundaryThreshold = 58

	expectedTwoFiringsMessage = "expected job fires 2 times"
	wrongJobRetrievedMessage  = "wrong job retrieved:"
	stopContextDoneMessage    = "context was not done immediately"

	slowStopJobDelay      = 2 * time.Second
	waitForStopCheck      = 750 * time.Millisecond
	waitForStopCompletion = 1500 * time.Millisecond
)

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

func newBufLogger(sw *syncWriter) Logger {
	return PrintfLogger(log.New(sw, "", log.LstdFlags))
}

func TestFuncPanicRecovery(t *testing.T) {
	t.Parallel()

	var buf syncWriter

	cron := New(WithParser(testParserWithSeconds()),
		WithChain(Recover(newBufLogger(&buf))))

	cron.Start()
	defer cron.Stop()

	mustAddFunc(t, cron, everySecondSpec, func() {
		panic("YOLO")
	})

	time.Sleep(OneSecond)

	if !strings.Contains(buf.String(), "YOLO") {
		t.Error("expected a panic to be logged, got none")
	}
}

type DummyJob struct{}

func (DummyJob) Run() {
	panic("YOLO")
}

func TestJobPanicRecovery(t *testing.T) {
	t.Parallel()

	var job DummyJob

	var buf syncWriter

	cron := New(WithParser(testParserWithSeconds()),
		WithChain(Recover(newBufLogger(&buf))))

	cron.Start()
	defer cron.Stop()

	mustAddJob(t, cron, everySecondSpec, job)

	time.Sleep(OneSecond)

	if !strings.Contains(buf.String(), "YOLO") {
		t.Error("expected a panic to be logged, got none")
	}
}

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	t.Parallel()

	cron := newWithSeconds()
	cron.Start()

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected cron will be stopped immediately")
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.Start()
	cron.Stop()
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		// No job ran!
	case <-wait(wg):
		t.Fatal("expected stopped cron does not run any job")
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()

	cron.Start()
	defer cron.Stop()

	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Test for #34. Adding a job after calling start results in multiple job invocations.
func TestAddWhileRunningWithDelay(t *testing.T) {
	t.Parallel()

	cron := newWithSeconds()

	cron.Start()
	defer cron.Stop()

	time.Sleep(5 * time.Second)

	var calls int64

	mustAddFunc(t, cron, everySecondWithSeconds, func() { atomic.AddInt64(&calls, 1) })

	<-time.After(OneSecond)

	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

// Add a job, remove a job, start cron, expect nothing runs.
func TestRemoveBeforeRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	id := mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })
	cron.Remove(id)

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		// Success, shouldn't run
	case <-wait(wg):
		t.FailNow()
	}
}

// Start cron, add a job, remove it, expect it doesn't run.
func TestRemoveWhileRunning(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()

	cron.Start()
	defer cron.Stop()

	id := mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })
	cron.Remove(id)

	select {
	case <-time.After(OneSecond):
	case <-wait(wg):
		t.FailNow()
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	mustAddFunc(t, cron, "@every 2s", func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	time.Sleep(OneSecond)
	cron.Entries()

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(OneSecond):
		t.Error("expected job runs at 2 second mark")
	case <-wait(wg):
	}
}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, func() {})
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })
	id1 := mustAddFunc(t, cron, everySecondSpec, func() { t.Fatal() })
	id2 := mustAddFunc(t, cron, everySecondSpec, func() { t.Fatal() })
	mustAddFunc(t, cron, decemberThirtyFirstSpec, func() {})
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	cron.Remove(id1)
	cron.Start()

	cron.Remove(id2)
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job run in proper order")
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, func() {})
	mustAddFunc(t, cron, decemberThirtyFirstSpec, func() {})
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error(expectedTwoFiringsMessage)
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	mustAddFunc(t, cron, januaryFirstSpec, func() {})
	mustAddFunc(t, cron, decemberThirtyFirstSpec, func() {})
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })
	cron.Schedule(Every(time.Minute), FuncJob(func() {}))
	cron.Schedule(Every(time.Second), FuncJob(func() { wg.Done() }))
	cron.Schedule(Every(time.Hour), FuncJob(func() {}))

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error(expectedTwoFiringsMessage)
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	now := time.Now()
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= secondsBoundaryThreshold {
		time.Sleep(2 * time.Second)

		now = time.Now()
	}

	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := newWithSeconds()
	mustAddFunc(t, cron, spec, func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error(expectedTwoFiringsMessage)
	case <-wait(wg):
	}
}

// Test that the cron is run in the given time zone (as opposed to local).
func TestNonLocalTimezone(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	loc := mustLoadLocation(t, "Atlantic/Cape_Verde")

	now := time.Now().In(loc)
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= secondsBoundaryThreshold {
		time.Sleep(2 * time.Second)

		now = time.Now().In(loc)
	}

	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New(WithLocation(loc), WithParser(testParserWithSeconds()))
	mustAddFunc(t, cron, spec, func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error(expectedTwoFiringsMessage)
	case <-wait(wg):
	}
}

// Test that calling stop before start silently returns without
// blocking the stop channel.
func TestStopWithoutStart(t *testing.T) {
	t.Parallel()

	cron := New()
	cron.Stop()
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
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

	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, func() { wg.Done() })

	unblockChan := make(chan struct{})

	go func() {
		cron.Run()
		close(unblockChan)
	}()

	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job fires")
	case <-unblockChan:
		t.Error("expected that Run() blocks")
	case <-wait(wg):
	}
}

// Test that double-running is a no-op.
func TestStartNoop(t *testing.T) {
	t.Parallel()

	tickChan := make(chan struct{}, 2)

	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondSpec, func() {
		tickChan <- struct{}{}
	})

	cron.Start()
	defer cron.Stop()

	// Wait for the first firing to ensure the runner is going
	<-tickChan

	cron.Start()

	<-tickChan

	// Fail if this job fires again in a short period, indicating a double-run
	select {
	case <-time.After(time.Millisecond):
	case <-tickChan:
		t.Error("expected job fires exactly twice")
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
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

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.FailNow()
	case <-wait(wg):
	}

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
func TestScheduleAfterRemoval(t *testing.T) {
	t.Parallel()

	var (
		wg1 sync.WaitGroup
		wg2 sync.WaitGroup
	)

	wg1.Add(1)
	wg2.Add(1)

	// The first time this job is run, set a timer and remove the other job
	// 750ms later. Correct behavior would be to still run the job again in
	// 250ms, but the bug would cause it to run instead 1s later.

	var (
		calls int
		mu    sync.Mutex
	)

	cron := newWithSeconds()
	hourJob := cron.Schedule(Every(time.Hour), FuncJob(func() {}))
	cron.Schedule(Every(time.Second), FuncJob(func() {
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
	}))

	cron.Start()
	defer cron.Stop()

	// the first run might be any length of time 0 - 1s, since the schedule
	// rounds to the second. wait for the first run to true up.
	wg1.Wait()

	select {
	case <-time.After(2 * OneSecond):
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

	cron := newWithSeconds()

	var calls int64

	mustAddFunc(t, cron, everySecondWithSeconds, func() { atomic.AddInt64(&calls, 1) })
	cron.Schedule(new(ZeroSchedule), FuncJob(func() { t.Error("expected zero task will not run") }))

	cron.Start()
	defer cron.Stop()

	<-time.After(OneSecond)

	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func TestStopAndWait(t *testing.T) {
	t.Parallel()

	t.Run("nothing running, returns immediately", testStopAndWaitNothingRunning)
	t.Run("repeated calls to Stop", testStopAndWaitRepeatedCalls)
	t.Run("a couple fast jobs added, still returns immediately", testStopAndWaitFastJobs)
	t.Run("a couple fast jobs and a slow job added, waits for slow job", testStopAndWaitSlowJob)
	t.Run("repeated calls to stop, waiting for completion and after", testStopAndWaitRepeatedWhileWaiting)
}

func TestMultiThreadedStartAndStop(t *testing.T) {
	t.Parallel()

	cron := New()
	go cron.Run()

	time.Sleep(2 * time.Millisecond)
	cron.Stop()
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)

	go func() {
		wg.Wait()

		ch <- true
	}()

	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)

	go func() {
		cron.Stop()

		ch <- true
	}()

	return ch
}

// newWithSeconds returns a Cron with the seconds field enabled.
func newWithSeconds() *Cron {
	return New(WithParser(testParserWithSeconds()), WithChain())
}

func requireTestJobName(t *testing.T, cron *Cron, id EntryID) string {
	t.Helper()

	return requireType[testJob](t, cron.Entry(id).Job).name
}

func testStopAndWaitNothingRunning(t *testing.T) {
	t.Parallel()

	cron := newWithSeconds()
	cron.Start()

	requireContextDoneWithin(cron.Stop(), t, time.Millisecond, stopContextDoneMessage)
}

func testStopAndWaitRepeatedCalls(t *testing.T) {
	t.Parallel()

	cron := newWithSeconds()
	cron.Start()
	_ = cron.Stop()

	time.Sleep(time.Millisecond)

	requireContextDoneWithin(cron.Stop(), t, time.Millisecond, stopContextDoneMessage)
}

func testStopAndWaitFastJobs(t *testing.T) {
	t.Parallel()

	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	cron.Start()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	time.Sleep(time.Second)

	requireContextDoneWithin(cron.Stop(), t, time.Millisecond, stopContextDoneMessage)
}

func testStopAndWaitSlowJob(t *testing.T) {
	t.Parallel()

	slowJobStarted := make(chan struct{}, 1)
	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	cron.Start()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {
		signalJobStarted(slowJobStarted)
		time.Sleep(slowStopJobDelay)
	})
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})

	waitForJobStarted(t, slowJobStarted)

	ctx := cron.Stop()
	requireContextPendingFor(ctx, t, waitForStopCheck, "context was done too quickly immediately")
	requireContextDoneWithin(ctx, t, waitForStopCompletion, "context not done after job should have completed")
}

func testStopAndWaitRepeatedWhileWaiting(t *testing.T) {
	t.Parallel()

	slowJobStarted := make(chan struct{}, 1)
	cron := newWithSeconds()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})
	mustAddFunc(t, cron, everySecondWithSeconds, func() {
		signalJobStarted(slowJobStarted)
		time.Sleep(slowStopJobDelay)
	})
	cron.Start()
	mustAddFunc(t, cron, everySecondWithSeconds, func() {})

	waitForJobStarted(t, slowJobStarted)

	ctx := cron.Stop()
	ctx2 := cron.Stop()

	select {
	case <-ctx.Done():
		t.Error("context was done too quickly immediately")
	case <-ctx2.Done():
		t.Error("context2 was done too quickly immediately")
	case <-time.After(waitForStopCompletion):
	}

	requireContextDoneWithin(ctx, t, time.Second, "context not done after job should have completed")
	requireContextDoneWithin(ctx2, t, time.Millisecond, "context2 not done even though context1 is")
	requireContextDoneWithin(cron.Stop(), t, time.Millisecond, "context not done even when cron Stop is completed")
}

func requireContextDoneWithin(ctx context.Context, t *testing.T, timeout time.Duration, message string) {
	t.Helper()

	select {
	case <-ctx.Done():
	case <-time.After(timeout):
		t.Error(message)
	}
}

func requireContextPendingFor(ctx context.Context, t *testing.T, duration time.Duration, message string) {
	t.Helper()

	select {
	case <-ctx.Done():
		t.Error(message)
	case <-time.After(duration):
	}
}

func waitForJobStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(OneSecond):
		t.Fatal("slow job did not start in time")
	}
}

func signalJobStarted(started chan<- struct{}) {
	select {
	case started <- struct{}{}:
	default:
	}
}
