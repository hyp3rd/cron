package cron

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestWithLocation(t *testing.T) {
	t.Parallel()

	c := New(WithLocation(time.UTC))
	if c.location != time.UTC {
		t.Errorf("expected UTC, got %v", c.location)
	}
}

func TestWithParser(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(Dow)

	c := New(WithParser(parser))
	if c.parser != parser {
		t.Error("expected provided parser")
	}
}

func TestWithLoggerCapturesSchedulerEvents(t *testing.T) {
	t.Parallel()

	var buf syncWriter

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fc := newFakeClock(baseTime)

	cron := New(WithLogger(logger), WithClock(fc))
	if cron.logger != logger {
		t.Error("expected provided logger")
	}

	mustAddFunc(t, cron, "@every 1s", func(context.Context) error { return nil })
	cron.Start(context.Background())
	fc.BlockUntilTimers(1)
	fc.Advance(1 * time.Second)
	time.Sleep(10 * time.Millisecond)

	err := cron.Stop(context.Background())
	if err != nil {
		t.Fatalf("stop: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "schedule") || !strings.Contains(out, "run") {
		t.Error("expected to see some actions, got:", out)
	}
}

func TestNilOptionsPreserveDefaults(t *testing.T) {
	t.Parallel()

	cronInstance := New(
		WithLocation(nil),
		WithParser(nil),
		WithLogger(nil),
		WithClock(nil),
	)

	if cronInstance.location != time.Local {
		t.Errorf("expected default location, got %v", cronInstance.location)
	}

	if cronInstance.logger == nil {
		t.Fatal("expected default logger")
	}

	if cronInstance.clock == nil {
		t.Fatal("expected default clock")
	}

	_, err := cronInstance.parser.Parse("* * * * *")
	if err != nil {
		t.Fatalf("expected default parser to remain active: %v", err)
	}
}
