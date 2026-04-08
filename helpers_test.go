package cron

import (
	"testing"
	"time"
)

func testParserWithSeconds() Parser {
	return NewParser(Second | Minute | Hour | Dom | Month | DowOptional | Descriptor)
}

func mustAddFunc(t *testing.T, cron *Cron, spec string, cmd func()) EntryID {
	t.Helper()

	entryID, err := cron.AddFunc(spec, cmd)
	if err != nil {
		t.Fatalf("add func %q: %v", spec, err)
	}

	return entryID
}

func mustAddJob(t *testing.T, cron *Cron, spec string, job Job) EntryID {
	t.Helper()

	entryID, err := cron.AddJob(spec, job)
	if err != nil {
		t.Fatalf("add job %q: %v", spec, err)
	}

	return entryID
}

func mustLoadLocation(t *testing.T, name string) *time.Location {
	t.Helper()

	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %q: %v", name, err)
	}

	return location
}

func requireType[T any](t *testing.T, value any) T {
	t.Helper()

	typedValue, ok := value.(T)
	if !ok {
		var zeroValue T

		t.Fatalf("unexpected type %T", value)

		return zeroValue
	}

	return typedValue
}
