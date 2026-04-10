package cron

import (
	"log/slog"
	"os"
)

// DefaultLogger is used by Cron if none is specified. It writes structured
// text to stdout under the "cron" group at [slog.LevelWarn] and above, so
// routine scheduling events stay quiet unless the caller opts in to verbose
// logging via [WithLogger].
func DefaultLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})).WithGroup("cron")
}

// DiscardLogger can be used by callers to discard all log messages.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}
