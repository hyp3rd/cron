# Migrating from robfig/cron/v3 to hyp3rd/cron/v4

This guide covers every breaking change between `github.com/robfig/cron/v3` and
`github.com/hyp3rd/cron/v4`.

---

## 1. Import path

```diff
- import "github.com/robfig/cron/v3"
+ import "github.com/hyp3rd/cron/v4"
```

```sh
go get github.com/hyp3rd/cron/v4@latest
```

## 2. Job interface — `context.Context` and `error` return

Jobs now receive a context (cancelled on scheduler stop) and return an error.

```diff
  type Job interface {
-     Run()
+     Run(ctx context.Context) error
  }
```

`FuncJob` follows the same change:

```diff
- type FuncJob func()
+ type FuncJob func(ctx context.Context) error
```

Update `AddFunc` call sites accordingly:

```diff
- c.AddFunc("@hourly", func() { fmt.Println("tick") })
+ c.AddFunc("@hourly", func(ctx context.Context) error {
+     fmt.Println("tick")
+     return nil
+ })
```

## 3. Start / Run / Stop — context-driven lifecycle

### Start

```diff
- c.Start()
+ c.Start(ctx)
```

The scheduler exits when `ctx` is cancelled.

### Run (blocking)

```diff
- c.Run()
+ err := c.Run(ctx)
```

Returns `cron.ErrAlreadyRunning` if a scheduler is already active.

### Stop

```diff
- ctx := c.Stop()    // old: returned a context
- <-ctx.Done()
+ err := c.Stop(ctx) // new: accepts a deadline context
```

`Stop` cancels the scheduler and waits for in-flight jobs, bounded by `ctx`. It
returns `ctx.Err()` if the deadline elapses before all jobs finish.

## 4. Logger — `log/slog` replaces custom interface

The custom `Logger` interface, `PrintfLogger`, and `VerbosePrintfLogger` are
removed. Cron now uses `*slog.Logger` directly.

```diff
- cron.New(cron.WithLogger(
-     cron.VerbosePrintfLogger(log.New(os.Stdout, "cron: ", log.LstdFlags))))
+ cron.New(cron.WithLogger(
+     slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))))
```

The default logger writes to stdout at `slog.LevelWarn`, keeping the scheduler
quiet unless you opt in to verbose logging.

## 5. Parser rename — `ScheduleParser` -> `Parser`, struct `Parser` -> `SpecParser`

| v3 name            | v4 name          |
|--------------------|------------------|
| `ScheduleParser`   | `Parser`         |
| `Parser` (struct)  | `SpecParser`     |
| `NewParser(...)`   | `NewSpecParser(...)` |

`NewStandardParser()` is unchanged.

```diff
- p := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
+ p := cron.NewSpecParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
```

## 6. Clock interface (new)

A `Clock` interface enables deterministic testing. The default is
`cron.SystemClock()`. Inject a custom clock via `WithClock`:

```go
c := cron.New(cron.WithClock(myClock))
```

## 7. Entry.WrappedJob is unexported

`Entry.WrappedJob` is now `Entry.wrappedJob` (unexported). Use `Entry.Job` to
access the user-supplied job.

## 8. Recover wrapper returns ErrPanic

`Recover` now returns a wrapped `cron.ErrPanic` error instead of silently
swallowing panics:

```go
err := job.Run(ctx)
if errors.Is(err, cron.ErrPanic) {
    // handle recovered panic
}
```

## 9. Removed symbols

| Removed                  | Replacement                |
|--------------------------|----------------------------|
| `Logger` (interface)     | `*slog.Logger`             |
| `PrintfLogger`           | `slog.New(slog.NewTextHandler(...))` |
| `VerbosePrintfLogger`    | set `slog.LevelDebug`     |
| `ScheduleParser`         | `Parser` (interface)       |
| `Parser` (struct)        | `SpecParser`               |
| `NewParser`              | `NewSpecParser`            |
| `Entry.WrappedJob`       | unexported                 |

---

## Quick migration checklist

1. Update import path to `github.com/hyp3rd/cron/v4`.
2. Add `context.Context` parameter and `error` return to all `Job` implementations and `FuncJob` / `AddFunc` closures.
3. Pass a `context.Context` to `Start`, `Run`, and `Stop`.
4. Replace `Logger`/`PrintfLogger`/`VerbosePrintfLogger` with `*slog.Logger`.
5. Rename `NewParser` calls to `NewSpecParser`.
6. Replace `Entry.WrappedJob` usage with `Entry.Job`.
7. Handle `error` returns from `Run` and `Stop`.
