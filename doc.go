/*
Package cron implements a cron spec parser and job runner.

# Installation

To download the latest tagged release, run:

	go get github.com/hyp3rd/cron/v4

Import it in your program as:

	import "github.com/hyp3rd/cron/v4"

It requires Go 1.26 or later.

# Usage

Callers may register Funcs to be invoked on a given schedule. Cron will run
them in their own goroutines. All jobs receive a [context.Context] that is
cancelled when the scheduler is stopped.

	c := cron.New()
	c.AddFunc("30 * * * *", func(ctx context.Context) error {
		fmt.Println("Every hour on the half hour")
		return nil
	})
	c.AddFunc("@hourly", func(ctx context.Context) error {
		fmt.Println("Every hour, starting an hour from now")
		return nil
	})
	c.Start(context.Background())
	..
	// Funcs are invoked in their own goroutine, asynchronously.
	...
	// Funcs may also be added to a running Cron
	c.AddFunc("@daily", func(ctx context.Context) error {
		fmt.Println("Every day")
		return nil
	})
	..
	// Inspect the cron job entries' next and previous run times.
	inspect(c.Entries())
	..
	c.Stop(ctx)  // Stop the scheduler and wait for in-flight jobs.

# CRON Expression Format

A cron expression represents a set of times, using 5 space-separated fields.

	Field name   | Mandatory? | Allowed values  | Allowed special characters
	----------   | ---------- | --------------  | --------------------------
	Minutes      | Yes        | 0-59            | * / , -
	Hours        | Yes        | 0-23            | * / , -
	Day of month | Yes        | 1-31            | * / , - ?
	Month        | Yes        | 1-12 or JAN-DEC | * / , -
	Day of week  | Yes        | 0-6 or SUN-SAT  | * / , - ?

Month and Day-of-week field values are case insensitive. "SUN", "Sun", and
"sun" are equally accepted.

The specific interpretation of the format is based on the Cron Wikipedia page:
https://en.wikipedia.org/wiki/Cron

# Alternative Formats

Alternative Cron expression formats support other fields like seconds. You can
implement that by creating a custom [SpecParser] as follows.

	cron.New(
		cron.WithParser(
			cron.NewSpecParser(
				cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)))

Since adding Seconds is the most common modification to the standard cron spec,
cron provides a builtin function to do that, which is equivalent to the custom
parser you saw earlier, except that its seconds field is REQUIRED:

	cron.New(cron.WithSeconds())

That emulates Quartz, the most popular alternative Cron schedule format:
http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html

# Special Characters

Asterisk ( * )

The asterisk indicates that the cron expression will match for all values of the
field; e.g., using an asterisk in the 5th field (month) would indicate every
month.

Slash ( / )

Slashes are used to describe increments of ranges. For example 3-59/15 in the
1st field (minutes) would indicate the 3rd minute of the hour and every 15
minutes thereafter. The form "*\/..." is equivalent to the form "first-last/...",
that is, an increment over the largest possible range of the field.  The form
"N/..." is accepted as meaning "N-MAX/...", that is, starting at N, use the
increment until the end of that specific range. It does not wrap around.

Comma ( , )

Commas are used to separate items of a list. For example, using "MON,WED,FRI" in
the 5th field (day of week) would mean Mondays, Wednesdays and Fridays.

Hyphen ( - )

Hyphens are used to define ranges. For example, 9-17 would indicate every
hour between 9am and 5pm inclusive.

Question mark ( ? )

Question mark may be used instead of '*' for leaving either day-of-month or
day-of-week blank.

# Predefined schedules

You may use one of several pre-defined schedules in place of a cron expression.

	Entry                  | Description                                | Equivalent To
	-----                  | -----------                                | -------------
	@yearly (or @annually) | Run once a year, midnight, Jan. 1st        | 0 0 1 1 *
	@monthly               | Run once a month, midnight, first of month | 0 0 1 * *
	@weekly                | Run once a week, midnight between Sat/Sun  | 0 0 * * 0
	@daily (or @midnight)  | Run once a day, midnight                   | 0 0 * * *
	@hourly                | Run once an hour, beginning of hour        | 0 * * * *

# Intervals

You may also schedule a job to execute at fixed intervals, starting at the time
it's added or cron is run. This is supported by formatting the cron spec like
this:

	@every <duration>

where "duration" is a string accepted by time.ParseDuration
(http://golang.org/pkg/time/#ParseDuration).

For example, "@every 1h30m10s" would indicate a schedule that activates after
1 hour, 30 minutes, 10 seconds, and then every interval after that.

Note: The interval does not take the job runtime into account. For example,
if a job takes 3 minutes to run, and it is scheduled to run every 5 minutes,
it will have only 2 minutes of idle time between each run.

# Time zones

By default, all interpretation and scheduling is done in the machine's local
time zone (time.Local). You can specify a different time zone on construction:

	cron.New(
	    cron.WithLocation(time.UTC))

Individual cron schedules may also override the time zone they are to be
interpreted in by providing an additional space-separated field at the beginning
of the cron spec, of the form "CRON_TZ=Asia/Tokyo".

For example:

	// Runs at 6am in time.Local
	cron.New().AddFunc("0 6 * * ?", ...)

	// Runs at 6am in America/New_York
	nyc, _ := time.LoadLocation("America/New_York")
	c := cron.New(cron.WithLocation(nyc))
	c.AddFunc("0 6 * * ?", ...)

	// Runs at 6am in Asia/Tokyo
	cron.New().AddFunc("CRON_TZ=Asia/Tokyo 0 6 * * ?", ...)

The prefix "TZ=(TIME ZONE)" is also supported for legacy compatibility.

Be aware that jobs scheduled during daylight-savings leap-ahead transitions will
not be run!

# Job Wrappers

A Cron runner may be configured with a chain of job wrappers to add
cross-cutting functionality to all submitted jobs. For example, they may be used
to achieve the following effects:

  - Recover any panics from jobs (activated by default)
  - Delay a job's execution if the previous run hasn't completed yet
  - Skip a job's execution if the previous run hasn't completed yet
  - Log each job's invocations

Install wrappers for all jobs added to a cron using the [WithChain] option:

	cron.New(cron.WithChain(
		cron.SkipIfStillRunning(logger),
	))

Install wrappers for individual jobs by explicitly wrapping them:

	job = cron.NewChain(
		cron.SkipIfStillRunning(logger),
	).Then(job)

Additional built-in wrappers:

  - [Timeout] — cancels the job's context after a deadline
  - [MaxConcurrent] — allows up to N concurrent invocations (generalizes SkipIfStillRunning)
  - [RetryOnError] — retries failed jobs with configurable backoff

Wrapper ordering matters when composing. For example:

	cron.NewChain(cron.Timeout(5*time.Second), cron.RetryOnError(3, time.Second)).Then(job)

gives each retry attempt its own 5-second timeout, while reversing the order
shares a single timeout across all retries.

# Named entries

Use [Cron.AddNamedFunc], [Cron.AddNamedJob], or [Cron.ScheduleNamed] to assign
a human-readable name to an entry. The name appears in log messages,
[Entry.Name], and event hook callbacks.

# Event hooks

Register lifecycle callbacks via [WithEventHooks]:

	cron.New(cron.WithEventHooks(cron.EventHooks{
		OnJobStart:    func(id cron.EntryID, name string) { ... },
		OnJobComplete: func(id cron.EntryID, name string, elapsed time.Duration, err error) { ... },
	}))

For error-only callbacks, use [WithOnError]:

	cron.New(cron.WithOnError(func(id cron.EntryID, name string, err error) { ... }))

# Schedule inspection

[NextN] previews the next N fire times for any [Schedule]:

	times := cron.NextN(sched, time.Now(), 5)

[SpecSchedule.String] reconstructs a 6-field cron expression from a parsed
schedule, enabling serialization and debugging.

# Thread safety

Since the Cron service runs concurrently with the calling code, some amount of
care must be taken to ensure proper synchronization.

All cron methods are designed to be correctly synchronized as long as the caller
ensures that invocations have a clear happens-before ordering between them.

# Logging

Cron uses [log/slog] for structured logging. By default, the scheduler logs at
[slog.LevelWarn] and above, so routine scheduling events stay quiet. Pass a
custom [*slog.Logger] via [WithLogger] to control log level and destination:

	cron.New(
		cron.WithLogger(slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)))

# Implementation

Cron entries are stored in an array, sorted by their next activation time. Cron
sleeps until the next job is due to be run.

Upon waking:
  - it runs each entry that is active on that second
  - it calculates the next run times for the jobs that were run
  - it re-sorts the array of entries by next activation time.
  - it goes to sleep until the soonest job.
*/
package cron
