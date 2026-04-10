# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.0.0] - Unreleased

### Added

- `Entry.Name` field with `AddNamedFunc`, `AddNamedJob`, and `ScheduleNamed`
  methods for human-readable entry labels in logs and hooks.
- `NextN(Schedule, time.Time, int) []time.Time` function to preview future
  activation times.
- `SpecSchedule.String()` method to reconstruct a 6-field cron expression from
  parsed bit fields.
- `Timeout(time.Duration)` job wrapper — cancels the job's context after a
  deadline.
- `MaxConcurrent(int, *slog.Logger)` job wrapper — allows up to N concurrent
  invocations, generalizes `SkipIfStillRunning`.
- `RetryOnError(int, time.Duration)` job wrapper — retries failed jobs with
  configurable backoff.
- `EventHooks` struct with `OnJobStart` and `OnJobComplete` callbacks, set via
  `WithEventHooks` option.
- `ErrorFunc` type and `WithOnError` option for error-only callbacks.
- `example_test.go` with runnable examples for pkg.go.dev.
- `context.Context` threading throughout the public API:
  - `Job.Run(ctx context.Context) error` — jobs receive a cancellable context
    and return errors.
  - `Start(ctx context.Context)` — binds the scheduler to the caller's context.
  - `Run(ctx context.Context) error` — blocking variant; returns
    `ErrAlreadyRunning` if a scheduler is already active.
  - `Stop(ctx context.Context) error` — cancels the scheduler, waits for
    in-flight jobs bounded by `ctx`.
- `Clock` interface (`Clock`, `Timer`) and `WithClock` option for deterministic
  testing without `time.Sleep`.
- `ErrAlreadyRunning` sentinel error returned by `Run` when called twice.
- `ErrPanic` sentinel error returned by `Recover` when a job panics, usable with
  `errors.Is`.
- `DiscardLogger()` convenience for silencing all scheduler output.

### Changed

- **Module path**: `github.com/hyp3rd/cron/v4` (was `github.com/robfig/cron/v3`).
- **Logging**: replaced custom `Logger` interface with `*slog.Logger`
  (`log/slog`). Default level is `slog.LevelWarn`.
- **Parser naming**: the `ScheduleParser` interface is now `Parser`; the concrete
  `Parser` struct is now `SpecParser`; `NewParser()` is now `NewSpecParser()`.
- `Entry.WrappedJob` is now unexported (`wrappedJob`).
- `FuncJob` signature: `func(context.Context) error` (was `func()`).
- Minimum Go version: **1.26**.

### Removed

- `Logger`, `PrintfLogger`, `VerbosePrintfLogger` — use `*slog.Logger` directly.
- `ScheduleParser` interface name — use `Parser`.
- `NewParser` constructor — use `NewSpecParser`.
- `.travis.yml` — CI is on GitHub Actions.

### Migration

See [MIGRATION.md](MIGRATION.md) for a step-by-step upgrade guide with
before/after code snippets.
