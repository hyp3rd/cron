package cron

import (
	"testing"
	"time"
)

const nextNCount = 5

func TestNextNExpectedCount(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor)

	sched, err := parser.Parse("0 * * * * *")
	if err != nil {
		t.Fatal(err)
	}

	times := NextN(sched, baseTime, nextNCount)
	if len(times) != nextNCount {
		t.Fatalf("expected 5 times, got %d", len(times))
	}

	for idx := 1; idx < len(times); idx++ {
		diff := times[idx].Sub(times[idx-1])
		if diff != time.Minute {
			t.Errorf("expected 1m between fires %d and %d, got %v", idx-1, idx, diff)
		}
	}
}

func TestNextNUnsatisfiable(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor)

	sched, err := parser.Parse("0 0 0 30 Feb *")
	if err != nil {
		t.Fatal(err)
	}

	times := NextN(sched, baseTime, nextNCount)
	if len(times) != 0 {
		t.Errorf("expected 0 times for unsatisfiable schedule, got %d", len(times))
	}
}

func TestNextNZeroAndNegativeCount(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor)

	sched, err := parser.Parse("0 * * * * *")
	if err != nil {
		t.Fatal(err)
	}

	if times := NextN(sched, baseTime, 0); len(times) != 0 {
		t.Errorf("expected 0 times for count=0, got %d", len(times))
	}

	if times := NextN(sched, baseTime, -1); len(times) != 0 {
		t.Errorf("expected 0 times for count=-1, got %d", len(times))
	}
}
