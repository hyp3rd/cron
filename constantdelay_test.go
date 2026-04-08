package cron

import (
	"testing"
	"time"
)

const (
	quarterHourDelay         = 15 * time.Minute
	roundingNanosecondDelay  = 50 * time.Nanosecond
	thirtyFiveMinuteDelay    = 35 * time.Minute
	fourteenMinuteDelay      = 14 * time.Minute
	fortyFourMinuteDelay     = 44 * time.Minute
	twentyFourSecondDelay    = 24 * time.Second
	twentyFiveHourDelay      = 25 * time.Hour
	ninetyOneDayDelay        = 91 * 24 * time.Hour
	twentyFiveMinuteDelay    = 25 * time.Minute
	fifteenSecondDelay       = 15 * time.Second
	fifteenMillisecondDelay  = 15 * time.Millisecond
	nextQuarterHourTimestamp = "Mon Jul 9 15:00 2012"
)

func TestConstantDelayNext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		time     string
		delay    time.Duration
		expected string
	}{
		// Simple cases
		{"Mon Jul 9 14:45 2012", quarterHourDelay + roundingNanosecondDelay, nextQuarterHourTimestamp},
		{"Mon Jul 9 14:59 2012", quarterHourDelay, "Mon Jul 9 15:14 2012"},
		{"Mon Jul 9 14:59:59 2012", quarterHourDelay, "Mon Jul 9 15:14:59 2012"},

		// Wrap around hours
		{"Mon Jul 9 15:45 2012", thirtyFiveMinuteDelay, "Mon Jul 9 16:20 2012"},

		// Wrap around days
		{"Mon Jul 9 23:46 2012", fourteenMinuteDelay, "Tue Jul 10 00:00 2012"},
		{"Mon Jul 9 23:45 2012", thirtyFiveMinuteDelay, "Tue Jul 10 00:20 2012"},
		{"Mon Jul 9 23:35:51 2012", fortyFourMinuteDelay + twentyFourSecondDelay, "Tue Jul 10 00:20:15 2012"},
		{"Mon Jul 9 23:35:51 2012", twentyFiveHourDelay + fortyFourMinuteDelay + twentyFourSecondDelay, "Thu Jul 11 01:20:15 2012"},

		// Wrap around months
		{"Mon Jul 9 23:35 2012", ninetyOneDayDelay + twentyFiveMinuteDelay, "Thu Oct 9 00:00 2012"},

		// Wrap around minute, hour, day, month, and year
		{"Mon Dec 31 23:59:45 2012", fifteenSecondDelay, "Tue Jan 1 00:00:00 2013"},

		// Round to nearest second on the delay
		{"Mon Jul 9 14:45 2012", quarterHourDelay + roundingNanosecondDelay, nextQuarterHourTimestamp},

		// Round up to 1 second if the duration is less.
		{"Mon Jul 9 14:45:00 2012", fifteenMillisecondDelay, "Mon Jul 9 14:45:01 2012"},

		// Round to nearest second when calculating the next time.
		{"Mon Jul 9 14:45:00.005 2012", quarterHourDelay, nextQuarterHourTimestamp},

		// Round to nearest second for both.
		{"Mon Jul 9 14:45:00.005 2012", quarterHourDelay + roundingNanosecondDelay, nextQuarterHourTimestamp},
	}

	for _, testCase := range tests {
		actual := Every(testCase.delay).Next(getTime(testCase.time))

		expected := getTime(testCase.expected)
		if actual != expected {
			t.Errorf("%s, \"%s\": (expected) %v != %v (actual)", testCase.time, testCase.delay, expected, actual)
		}
	}
}
