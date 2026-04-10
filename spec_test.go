package cron

import (
	"strings"
	"testing"
	"time"
)

const (
	sundayJulyFifteenthMidnight = "Sun Jul 15 00:00 2012"
	julyNinthThreePM            = "Mon Jul 9 15:00 2012"
	julyNinthLateNight          = "Mon Jul 9 23:35 2012"
	julyNinthLateNightSeconds   = "Mon Jul 9 23:35:51 2012"

	newYorkSpringMidnight = "2012-03-11T00:00:00-0500"
	newYorkSpringOneAM    = "2012-03-11T01:00:00-0500"
	newYorkSpringThreeAM  = "2012-03-11T03:00:00-0400"
	newYorkSpringFourAM   = "2012-03-11T04:00:00-0400"

	newYorkHourlySpec     = "TZ=America/New_York 0 0 * * * ?"
	newYorkCronHourlySpec = "CRON_TZ=America/New_York 0 0 * * * ?"
	newYorkOneAMSpec      = "TZ=America/New_York 0 0 1 * * ?"

	newYorkFallMidnight = "2012-11-04T00:00:00-0400"
	newYorkFallOneAMEDT = "2012-11-04T01:00:00-0400"
	newYorkFallOneAMEST = "2012-11-04T01:00:00-0500"
	newYorkFallTwoAMEST = "2012-11-04T02:00:00-0500"

	inputNewYorkFallMidnight = "TZ=America/New_York 2012-11-04T00:00:00-0400"
	kolkataExpectedTime      = "2016-01-03T14:14:00+0530"
)

type nextRunTestCase struct {
	time     string
	spec     string
	expected string
}

func TestActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		time, spec string
		expected   bool
	}{
		// Every fifteen minutes.
		{"Mon Jul 9 15:00 2012", "0/15 * * * *", true},
		{"Mon Jul 9 15:45 2012", "0/15 * * * *", true},
		{"Mon Jul 9 15:40 2012", "0/15 * * * *", false},

		// Every fifteen minutes, starting at 5 minutes.
		{"Mon Jul 9 15:05 2012", "5/15 * * * *", true},
		{"Mon Jul 9 15:20 2012", "5/15 * * * *", true},
		{"Mon Jul 9 15:50 2012", "5/15 * * * *", true},

		// Named months
		{"Sun Jul 15 15:00 2012", "0/15 * * Jul *", true},
		{"Sun Jul 15 15:00 2012", "0/15 * * Jun *", false},

		// Everything set.
		{"Sun Jul 15 08:30 2012", "30 08 ? Jul Sun", true},
		{"Sun Jul 15 08:30 2012", "30 08 15 Jul ?", true},
		{"Mon Jul 16 08:30 2012", "30 08 ? Jul Sun", false},
		{"Mon Jul 16 08:30 2012", "30 08 15 Jul ?", false},

		// Predefined schedules
		{julyNinthThreePM, "@hourly", true},
		{"Mon Jul 9 15:04 2012", "@hourly", false},
		{julyNinthThreePM, "@daily", false},
		{"Mon Jul 9 00:00 2012", "@daily", true},
		{"Mon Jul 9 00:00 2012", "@weekly", false},
		{"Sun Jul 8 00:00 2012", "@weekly", true},
		{"Sun Jul 8 01:00 2012", "@weekly", false},
		{"Sun Jul 8 00:00 2012", "@monthly", false},
		{"Sun Jul 1 00:00 2012", "@monthly", true},

		// Test interaction of DOW and DOM.
		// If both are restricted, then only one needs to match.
		{sundayJulyFifteenthMidnight, "* * 1,15 * Sun", true},
		{"Fri Jun 15 00:00 2012", "* * 1,15 * Sun", true},
		{"Wed Aug 1 00:00 2012", "* * 1,15 * Sun", true},
		{sundayJulyFifteenthMidnight, "* * */10 * Sun", true}, // verifies #70

		// However, if one has a star, then both need to match.
		{sundayJulyFifteenthMidnight, "* * * * Mon", false},
		{"Mon Jul 9 00:00 2012", "* * 1,15 * *", false},
		{sundayJulyFifteenthMidnight, "* * 1,15 * *", true},
		{sundayJulyFifteenthMidnight, "* * */2 * Sun", true},
	}

	for _, testCase := range tests {
		sched, err := ParseStandard(testCase.spec)
		if err != nil {
			t.Error(err)

			continue
		}

		actual := sched.Next(getTime(testCase.time).Add(-1 * time.Second))

		expected := getTime(testCase.time)
		if testCase.expected && expected != actual || !testCase.expected && expected.Equal(actual) {
			t.Errorf("Fail evaluating %s on %s: (expected) %s != %s (actual)",
				testCase.spec, testCase.time, expected, actual)
		}
	}
}

func TestNext(t *testing.T) {
	t.Parallel()

	t.Run("basic and wrap", func(t *testing.T) {
		t.Parallel()
		assertNextRunCases(t, nextRunBasicAndWrapCases())
	})
	t.Run("spring forward", func(t *testing.T) {
		t.Parallel()
		assertNextRunCases(t, nextRunSpringForwardCases())
	})
	t.Run("fall back with tz spec", func(t *testing.T) {
		t.Parallel()
		assertNextRunCases(t, nextRunFallBackSpecCases())
	})
	t.Run("fall back with tz input", func(t *testing.T) {
		t.Parallel()
		assertNextRunCases(t, nextRunFallBackInputCases())
	})
	t.Run("edge cases", func(t *testing.T) {
		t.Parallel()
		assertNextRunCases(t, nextRunEdgeCases())
	})
}

func TestErrors(t *testing.T) {
	t.Parallel()

	invalidSpecs := []string{
		"xyz",
		"60 0 * * *",
		"0 60 * * *",
		"0 0 * * XYZ",
	}
	for _, spec := range invalidSpecs {
		_, err := ParseStandard(spec)
		if err == nil {
			t.Error("expected an error parsing: ", spec)
		}
	}
}

func getTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}

	location := time.Local

	if strings.HasPrefix(value, "TZ=") {
		parts := strings.Fields(value)

		loc, err := time.LoadLocation(parts[0][len("TZ="):])
		if err != nil {
			panic("could not parse location:" + err.Error())
		}

		location = loc
		value = parts[1]
	}

	layouts := []string{
		"Mon Jan 2 15:04 2006",
		"Mon Jan 2 15:04:05 2006",
	}
	for _, layout := range layouts {
		parsedTime, err := time.ParseInLocation(layout, value, location)
		if err == nil {
			return parsedTime
		}
	}

	parsedTime, err := time.ParseInLocation("2006-01-02T15:04:05-0700", value, location)
	if err == nil {
		return parsedTime
	}

	panic("could not parse time value " + value)
}

func TestNextWithTz(t *testing.T) {
	t.Parallel()

	runs := []struct {
		time, spec string
		expected   string
	}{
		// Failing tests
		{"2016-01-03T13:09:03+0530", "14 14 * * *", kolkataExpectedTime},
		{"2016-01-03T04:09:03+0530", "14 14 * * ?", kolkataExpectedTime},

		// Passing tests
		{"2016-01-03T14:09:03+0530", "14 14 * * *", kolkataExpectedTime},
		{"2016-01-03T14:00:00+0530", "14 14 * * ?", kolkataExpectedTime},
	}
	for _, testCase := range runs {
		sched, err := ParseStandard(testCase.spec)
		if err != nil {
			t.Error(err)

			continue
		}

		actual := sched.Next(getTimeTZ(testCase.time))

		expected := getTimeTZ(testCase.expected)
		if !actual.Equal(expected) {
			t.Errorf("%s, \"%s\": (expected) %v != %v (actual)", testCase.time, testCase.spec, expected, actual)
		}
	}
}

func getTimeTZ(value string) time.Time {
	if value == "" {
		return time.Time{}
	}

	parsedTime, err := time.Parse("Mon Jan 2 15:04 2006", value)
	if err != nil {
		parsedTime, err = time.Parse("Mon Jan 2 15:04:05 2006", value)
		if err != nil {
			parsedTime, err = time.Parse("2006-01-02T15:04:05-0700", value)
			if err != nil {
				panic(err)
			}
		}
	}

	return parsedTime
}

// https://github.com/robfig/cron/issues/144
func TestSlash0NoHang(t *testing.T) {
	t.Parallel()

	schedule := "TZ=America/New_York 15/0 * * * *"

	_, err := ParseStandard(schedule)
	if err == nil {
		t.Error("expected an error on 0 increment")
	}
}

func assertNextRunCases(t *testing.T, testCases []nextRunTestCase) {
	t.Helper()

	parserWithSeconds := testParserWithSeconds()
	for _, testCase := range testCases {
		sched, err := parserWithSeconds.Parse(testCase.spec)
		if err != nil {
			t.Error(err)

			continue
		}

		actual := sched.Next(getTime(testCase.time))

		expected := getTime(testCase.expected)
		if !actual.Equal(expected) {
			t.Errorf("%s, \"%s\": (expected) %v != %v (actual)", testCase.time, testCase.spec, expected, actual)
		}
	}
}

func nextRunBasicAndWrapCases() []nextRunTestCase {
	return []nextRunTestCase{
		{"Mon Jul 9 14:45 2012", "0 0/15 * * * *", julyNinthThreePM},
		{"Mon Jul 9 14:59 2012", "0 0/15 * * * *", julyNinthThreePM},
		{"Mon Jul 9 14:59:59 2012", "0 0/15 * * * *", julyNinthThreePM},
		{"Mon Jul 9 15:45 2012", "0 20-35/15 * * * *", "Mon Jul 9 16:20 2012"},
		{"Mon Jul 9 23:46 2012", "0 */15 * * * *", "Tue Jul 10 00:00 2012"},
		{"Mon Jul 9 23:45 2012", "0 20-35/15 * * * *", "Tue Jul 10 00:20 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 * * * *", "Tue Jul 10 00:20:15 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 1/2 * * *", "Tue Jul 10 01:20:15 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 10-12 * * *", "Tue Jul 10 10:20:15 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 1/2 */2 * *", "Thu Jul 11 01:20:15 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 * 9-20 * *", "Wed Jul 10 00:20:15 2012"},
		{julyNinthLateNightSeconds, "15/35 20-35/15 * 9-20 Jul *", "Wed Jul 10 00:20:15 2012"},
		{julyNinthLateNight, "0 0 0 9 Apr-Oct ?", "Thu Aug 9 00:00 2012"},
		{julyNinthLateNight, "0 0 0 */5 Apr,Aug,Oct Mon", "Tue Aug 1 00:00 2012"},
		{julyNinthLateNight, "0 0 0 */5 Oct Mon", "Mon Oct 1 00:00 2012"},
		{julyNinthLateNight, "0 0 0 * Feb Mon", "Mon Feb 4 00:00 2013"},
		{julyNinthLateNight, "0 0 0 * Feb Mon/2", "Fri Feb 1 00:00 2013"},
		{"Mon Dec 31 23:59:45 2012", "0 * * * * *", "Tue Jan 1 00:00:00 2013"},
		{julyNinthLateNight, "0 0 0 29 Feb ?", "Mon Feb 29 00:00 2016"},
	}
}

func nextRunSpringForwardCases() []nextRunTestCase {
	return []nextRunTestCase{
		{newYorkSpringMidnight, "TZ=America/New_York 0 30 2 11 Mar ?", "2013-03-11T02:30:00-0400"},
		{newYorkSpringMidnight, newYorkHourlySpec, newYorkSpringOneAM},
		{newYorkSpringOneAM, newYorkHourlySpec, newYorkSpringThreeAM},
		{newYorkSpringThreeAM, newYorkHourlySpec, newYorkSpringFourAM},
		{newYorkSpringFourAM, newYorkHourlySpec, "2012-03-11T05:00:00-0400"},
		{newYorkSpringMidnight, newYorkCronHourlySpec, newYorkSpringOneAM},
		{newYorkSpringOneAM, newYorkCronHourlySpec, newYorkSpringThreeAM},
		{newYorkSpringThreeAM, newYorkCronHourlySpec, newYorkSpringFourAM},
		{newYorkSpringFourAM, newYorkCronHourlySpec, "2012-03-11T05:00:00-0400"},
		{newYorkSpringMidnight, newYorkOneAMSpec, newYorkSpringOneAM},
		{newYorkSpringOneAM, newYorkOneAMSpec, "2012-03-12T01:00:00-0400"},
		{newYorkSpringMidnight, "TZ=America/New_York 0 0 2 * * ?", "2012-03-12T02:00:00-0400"},
	}
}

func nextRunFallBackSpecCases() []nextRunTestCase {
	return []nextRunTestCase{
		{newYorkFallMidnight, "TZ=America/New_York 0 30 2 04 Nov ?", "2012-11-04T02:30:00-0500"},
		{"2012-11-04T01:45:00-0400", "TZ=America/New_York 0 30 1 04 Nov ?", "2012-11-04T01:30:00-0500"},
		{newYorkFallMidnight, newYorkHourlySpec, newYorkFallOneAMEDT},
		{newYorkFallOneAMEDT, newYorkHourlySpec, newYorkFallOneAMEST},
		{newYorkFallOneAMEST, newYorkHourlySpec, newYorkFallTwoAMEST},
		{newYorkFallMidnight, newYorkOneAMSpec, newYorkFallOneAMEDT},
		{newYorkFallOneAMEDT, newYorkOneAMSpec, newYorkFallOneAMEST},
		{newYorkFallOneAMEST, newYorkOneAMSpec, "2012-11-05T01:00:00-0500"},
		{newYorkFallMidnight, "TZ=America/New_York 0 0 2 * * ?", newYorkFallTwoAMEST},
		{newYorkFallTwoAMEST, "TZ=America/New_York 0 0 2 * * ?", "2012-11-05T02:00:00-0500"},
		{newYorkFallMidnight, "TZ=America/New_York 0 0 3 * * ?", "2012-11-04T03:00:00-0500"},
		{"2012-11-04T03:00:00-0500", "TZ=America/New_York 0 0 3 * * ?", "2012-11-05T03:00:00-0500"},
	}
}

func nextRunFallBackInputCases() []nextRunTestCase {
	return []nextRunTestCase{
		{inputNewYorkFallMidnight, "0 0 * * * ?", "2012-11-04T01:00:00-0400"},
		{"TZ=America/New_York 2012-11-04T01:00:00-0400", "0 0 * * * ?", newYorkFallOneAMEST},
		{"TZ=America/New_York 2012-11-04T01:00:00-0500", "0 0 * * * ?", newYorkFallTwoAMEST},
		{inputNewYorkFallMidnight, "0 0 1 * * ?", "2012-11-04T01:00:00-0400"},
		{"TZ=America/New_York 2012-11-04T01:00:00-0400", "0 0 1 * * ?", newYorkFallOneAMEST},
		{"TZ=America/New_York 2012-11-04T01:00:00-0500", "0 0 1 * * ?", "2012-11-05T01:00:00-0500"},
		{inputNewYorkFallMidnight, "0 0 2 * * ?", newYorkFallTwoAMEST},
		{"TZ=America/New_York 2012-11-04T02:00:00-0500", "0 0 2 * * ?", "2012-11-05T02:00:00-0500"},
		{inputNewYorkFallMidnight, "0 0 3 * * ?", "2012-11-04T03:00:00-0500"},
		{"TZ=America/New_York 2012-11-04T03:00:00-0500", "0 0 3 * * ?", "2012-11-05T03:00:00-0500"},
	}
}

func nextRunEdgeCases() []nextRunTestCase {
	return []nextRunTestCase{
		{julyNinthLateNight, "0 0 0 30 Feb ?", ""},
		{julyNinthLateNight, "0 0 0 31 Apr ?", ""},
		{"TZ=America/New_York 2012-11-04T00:00:00-0400", "0 0 3 3 * ?", "2012-12-03T03:00:00-0500"},
		{"2018-10-17T05:00:00-0400", "TZ=America/Sao_Paulo 0 0 9 10 * ?", "2018-11-10T06:00:00-0500"},
		{"2018-02-14T05:00:00-0500", "TZ=America/Sao_Paulo 0 0 9 22 * ?", "2018-02-22T07:00:00-0500"},
	}
}
