# cron

[![Go Reference](https://pkg.go.dev/badge/github.com/hyp3rd/cron/v4.svg)](https://pkg.go.dev/github.com/hyp3rd/cron/v4)
[![CI](https://github.com/hyp3rd/cron/actions/workflows/go.yml/badge.svg)](https://github.com/hyp3rd/cron/actions/workflows/go.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A fast, well-tested cron expression parser and job scheduler for Go.

This is a modernized fork of the abandoned
[`robfig/cron/v3`](https://github.com/robfig/cron). It ships an idiomatic
Go 1.26+ API with `context.Context`, `log/slog`, and a testable `Clock`
interface.

## Install

```bash
go get github.com/hyp3rd/cron/v4@latest
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"

    "github.com/hyp3rd/cron/v4"
)

func main() {
    c := cron.New()

    c.AddFunc("@every 5s", func(ctx context.Context) error {
        fmt.Println("tick")

        return nil
    })

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    c.Start(ctx)

    <-ctx.Done()
    c.Stop(context.Background())
}
```

## Features

- **Standard 5-field cron expressions** (minute, hour, dom, month, dow) plus
  optional seconds via `WithSeconds()`.
- **`context.Context` throughout** — `Start`, `Run`, `Stop`, and every `Job`
  receive a context for cancellation and deadlines.
- **`log/slog` logging** — structured, leveled logging out of the box. Default
  level is `slog.LevelWarn` to keep the scheduler quiet.
- **`Clock` interface** — inject a fake clock via `WithClock` for deterministic,
  zero-`time.Sleep` tests.
- **Job wrappers** — `Recover`, `SkipIfStillRunning`, `DelayIfStillRunning`,
  and custom `JobWrapper` chains.
- **Thread-safe** — add, remove, and inspect entries while the scheduler is
  running.

## Cron expressions

| Field | Allowed values | Special characters |
|---|---|---|
| Minutes | 0-59 | `*` `/` `,` `-` |
| Hours | 0-23 | `*` `/` `,` `-` |
| Day of month | 1-31 | `*` `/` `,` `-` `?` |
| Month | 1-12 or JAN-DEC | `*` `/` `,` `-` |
| Day of week | 0-6 or SUN-SAT | `*` `/` `,` `-` `?` |

### Predefined schedules

| Entry       | Equivalent |
|------------ |----------- |
| `@yearly`   | `0 0 1 1 *` |
| `@monthly`  | `0 0 1 * *` |
| `@weekly`   | `0 0 * * 0` |
| `@daily`    | `0 0 * * *` |
| `@hourly`   | `0 * * * *` |

### Intervals

```text
@every 1h30m
```

### Time zones

```go
cron.New(cron.WithLocation(time.UTC))
// or per-schedule:
c.AddFunc("CRON_TZ=Asia/Tokyo 0 6 * * ?", myJob)
```

## Job wrappers / Chain

```go
c := cron.New(cron.WithChain(
    cron.Recover(logger),
    cron.SkipIfStillRunning(logger),
))
```

Or per-job:

```go
wrapped := cron.NewChain(cron.Recover(logger)).Then(myJob)
```

## Testing with a fake clock

The `Clock` interface lets you drive the scheduler deterministically:

```go
c := cron.New(cron.WithClock(fakeClock))
```

See `clock.go` for the interface definition.

## Migration from robfig/cron/v3

See [MIGRATION.md](MIGRATION.md) for a step-by-step upgrade guide.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT - see [LICENSE](LICENSE).
