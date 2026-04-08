package cron

import (
	"io"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	jobCompletionWait       = 2 * time.Millisecond
	twoJobCompletionWait    = 3 * time.Millisecond
	delayedJobDuration      = 10 * time.Millisecond
	waitForFirstJob         = 5 * time.Millisecond
	waitForDelayedJobs      = 25 * time.Millisecond
	rapidFireJobRuns        = 11
	rapidFireCompletionWait = 200 * time.Millisecond
	independentJobsWait     = 100 * time.Millisecond
)

func appendingJob(slice *[]int, value int) Job {
	var m sync.Mutex

	return FuncJob(func() {
		m.Lock()

		*slice = append(*slice, value)
		m.Unlock()
	})
}

func appendingWrapper(slice *[]int, value int) JobWrapper {
	return func(job Job) Job {
		return FuncJob(func() {
			appendingJob(slice, value).Run()
			job.Run()
		})
	}
}

func TestChain(t *testing.T) {
	t.Parallel()

	var (
		nums    []int
		append1 = appendingWrapper(&nums, 1)
		append2 = appendingWrapper(&nums, 2)
		append3 = appendingWrapper(&nums, 3)
		append4 = appendingJob(&nums, 4)
	)

	NewChain(append1, append2, append3).Then(append4).Run()

	if !reflect.DeepEqual(nums, []int{1, 2, 3, 4}) {
		t.Error("unexpected order of calls:", nums)
	}
}

func TestChainRecover(t *testing.T) {
	t.Parallel()

	panickingJob := FuncJob(func() {
		panic("panickingJob panics")
	})

	t.Run("panic exits job by default", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if err := recover(); err == nil {
				t.Error("panic expected, but none received")
			}
		}()

		NewChain().Then(panickingJob).
			Run()
	})

	t.Run("Recovering JobWrapper recovers", func(t *testing.T) {
		t.Parallel()

		NewChain(Recover(PrintfLogger(log.New(io.Discard, "", 0)))).
			Then(panickingJob).
			Run()
	})

	t.Run("composed with the *IfStillRunning wrappers", func(t *testing.T) {
		t.Parallel()

		NewChain(Recover(PrintfLogger(log.New(io.Discard, "", 0)))).
			Then(panickingJob).
			Run()
	})
}

type countJob struct {
	m       sync.Mutex
	started int
	done    int
	delay   time.Duration
}

func (j *countJob) Run() {
	j.m.Lock()
	j.started++
	j.m.Unlock()
	time.Sleep(j.delay)
	j.m.Lock()
	j.done++
	j.m.Unlock()
}

func (j *countJob) Started() int {
	defer j.m.Unlock()

	j.m.Lock()

	return j.started
}

func (j *countJob) Done() int {
	defer j.m.Unlock()

	j.m.Lock()

	return j.done
}

func TestChainDelayIfStillRunning(t *testing.T) {
	t.Parallel()

	t.Run("runs immediately", func(t *testing.T) {
		t.Parallel()

		var jobCounter countJob

		wrappedJob := NewChain(DelayIfStillRunning(DiscardLogger())).Then(&jobCounter)
		go wrappedJob.Run()

		time.Sleep(jobCompletionWait)

		if c := jobCounter.Done(); c != 1 {
			t.Errorf("expected job run once, immediately, got %d", c)
		}
	})

	t.Run("second run immediate if first done", func(t *testing.T) {
		t.Parallel()

		var jobCounter countJob

		wrappedJob := NewChain(DelayIfStillRunning(DiscardLogger())).Then(&jobCounter)

		go func() {
			go wrappedJob.Run()

			time.Sleep(time.Millisecond)

			go wrappedJob.Run()
		}()

		time.Sleep(twoJobCompletionWait)

		if c := jobCounter.Done(); c != 2 {
			t.Errorf("expected job run twice, immediately, got %d", c)
		}
	})

	t.Run("second run delayed if first not done", func(t *testing.T) {
		t.Parallel()

		var jobCounter countJob

		jobCounter.delay = delayedJobDuration
		wrappedJob := NewChain(DelayIfStillRunning(DiscardLogger())).Then(&jobCounter)

		go func() {
			go wrappedJob.Run()

			time.Sleep(time.Millisecond)

			go wrappedJob.Run()
		}()

		// After 5ms, the first job is still in progress, and the second job was
		// run but should be waiting for it to finish.
		time.Sleep(waitForFirstJob)

		started, done := jobCounter.Started(), jobCounter.Done()
		if started != 1 || done != 0 {
			t.Error("expected first job started, but not finished, got", started, done)
		}

		// Verify that the second job completes.
		time.Sleep(waitForDelayedJobs)

		started, done = jobCounter.Started(), jobCounter.Done()
		if started != 2 || done != 2 {
			t.Error("expected both jobs done, got", started, done)
		}
	})
}

func TestChainSkipIfStillRunning(t *testing.T) {
	t.Parallel()

	t.Run("runs immediately", testChainSkipRunsImmediately)
	t.Run("second run immediate if first done", testChainSkipSecondRunImmediate)
	t.Run("second run skipped if first not done", testChainSkipSecondRunSkipped)
	t.Run("skip 10 jobs on rapid fire", testChainSkipRapidFire)
	t.Run("different jobs independent", testChainSkipDifferentJobs)
}

func testChainSkipRunsImmediately(t *testing.T) {
	t.Parallel()

	var jobCounter countJob

	wrappedJob := NewChain(SkipIfStillRunning(DiscardLogger())).Then(&jobCounter)
	go wrappedJob.Run()

	time.Sleep(jobCompletionWait)

	if c := jobCounter.Done(); c != 1 {
		t.Errorf("expected job run once, immediately, got %d", c)
	}
}

func testChainSkipSecondRunImmediate(t *testing.T) {
	t.Parallel()

	var jobCounter countJob

	wrappedJob := NewChain(SkipIfStillRunning(DiscardLogger())).Then(&jobCounter)

	go func() {
		go wrappedJob.Run()

		time.Sleep(time.Millisecond)

		go wrappedJob.Run()
	}()

	time.Sleep(twoJobCompletionWait)

	if c := jobCounter.Done(); c != 2 {
		t.Errorf("expected job run twice, immediately, got %d", c)
	}
}

func testChainSkipSecondRunSkipped(t *testing.T) {
	t.Parallel()

	var jobCounter countJob

	jobCounter.delay = delayedJobDuration
	wrappedJob := NewChain(SkipIfStillRunning(DiscardLogger())).Then(&jobCounter)

	go func() {
		go wrappedJob.Run()

		time.Sleep(time.Millisecond)

		go wrappedJob.Run()
	}()

	time.Sleep(waitForFirstJob)

	started, done := jobCounter.Started(), jobCounter.Done()
	if started != 1 || done != 0 {
		t.Error("expected first job started, but not finished, got", started, done)
	}

	time.Sleep(waitForDelayedJobs)

	started, done = jobCounter.Started(), jobCounter.Done()
	if started != 1 || done != 1 {
		t.Error("expected second job skipped, got", started, done)
	}
}

func testChainSkipRapidFire(t *testing.T) {
	t.Parallel()

	var jobCounter countJob

	jobCounter.delay = delayedJobDuration

	wrappedJob := NewChain(SkipIfStillRunning(DiscardLogger())).Then(&jobCounter)
	for range [rapidFireJobRuns]struct{}{} {
		go wrappedJob.Run()
	}

	time.Sleep(rapidFireCompletionWait)

	done := jobCounter.Done()
	if done != 1 {
		t.Error("expected 1 jobs executed, 10 jobs dropped, got", done)
	}
}

func testChainSkipDifferentJobs(t *testing.T) {
	t.Parallel()

	var firstJob, secondJob countJob

	firstJob.delay = delayedJobDuration
	secondJob.delay = delayedJobDuration
	chain := NewChain(SkipIfStillRunning(DiscardLogger()))
	wrappedJob1 := chain.Then(&firstJob)
	wrappedJob2 := chain.Then(&secondJob)

	for range [rapidFireJobRuns]struct{}{} {
		go wrappedJob1.Run()
		go wrappedJob2.Run()
	}

	time.Sleep(independentJobsWait)

	done1 := firstJob.Done()

	done2 := secondJob.Done()
	if done1 != 1 || done2 != 1 {
		t.Error("expected both jobs executed once, got", done1, "and", done2)
	}
}
