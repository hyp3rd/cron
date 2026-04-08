package cron

import (
	"log"
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

	parser := NewParser(Dow)

	c := New(WithParser(parser))
	if c.parser != parser {
		t.Error("expected provided parser")
	}
}

func TestWithVerboseLogger(t *testing.T) {
	t.Parallel()

	var buf syncWriter

	logger := log.New(&buf, "", log.LstdFlags)

	cron := New(WithLogger(VerbosePrintfLogger(logger)))
	if requireType[printfLogger](t, cron.logger).logger != logger {
		t.Error("expected provided logger")
	}

	mustAddFunc(t, cron, "@every 1s", func() {})
	cron.Start()
	time.Sleep(OneSecond)
	cron.Stop()

	out := buf.String()
	if !strings.Contains(out, "schedule,") ||
		!strings.Contains(out, "run,") {
		t.Error("expected to see some actions, got:", out)
	}
}
