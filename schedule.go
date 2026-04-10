package cron

import "time"

// NextN returns the next n activation times for the given schedule, starting
// after the given time. If the schedule cannot produce n times (e.g., the
// schedule is unsatisfiable), the returned slice will have fewer than n
// elements.
func NextN(sched Schedule, after time.Time, count int) []time.Time {
	times := make([]time.Time, 0, max(count, 0))

	for range count {
		next := sched.Next(after)
		if next.IsZero() {
			break
		}

		times = append(times, next)
		after = next
	}

	return times
}
