package cron

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	minValueZero  uint = 0
	minValueOne   uint = 1
	maxValueThree uint = 3
	maxValueFour  uint = 4
	valueFive     uint = 5
	valueSix      uint = 6
	maxValueSeven uint = 7

	fieldValueZero     = "0"
	fieldValueFive     = "5"
	fieldValueFifteen  = "15"
	fieldValueWildcard = "*"

	fiveMinuteDelay = 5 * time.Minute

	failedToParseIntText = "failed to parse int from"
	unexpectedErrorText  = "%s => unexpected error %v"

	bitMaskZeroToFiftyNine  uint64 = 0xfffffffffffffff
	bitMaskZeroToTwentyFour uint64 = 0xffffff
	bitMaskOneToThirtyOne   uint64 = 0xfffffffe
	bitMaskOneToTwelve      uint64 = 0x1ffe
	bitMaskZeroToSix        uint64 = 0x7f

	singleBitZero         uint64 = 0x1
	singleBitOne          uint64 = 0x2
	alternatingBitsToFive uint64 = 0x2a
	alternatingBitsToFour uint64 = 0xa
)

func TestRange(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	ranges := []struct {
		expr     string
		min, max uint
		expected uint64
		err      string
	}{
		{fieldValueFive, minValueZero, maxValueSeven, 1 << valueFive, ""},
		{fieldValueZero, minValueZero, maxValueSeven, 1 << minValueZero, ""},
		{"7", minValueZero, maxValueSeven, 1 << maxValueSeven, ""},

		{"5-5", minValueZero, maxValueSeven, 1 << valueFive, ""},
		{"5-6", minValueZero, maxValueSeven, 1<<valueFive | 1<<valueSix, ""},
		{"5-7", minValueZero, maxValueSeven, 1<<valueFive | 1<<valueSix | 1<<maxValueSeven, ""},

		{"5-6/2", minValueZero, maxValueSeven, 1 << valueFive, ""},
		{"5-7/2", minValueZero, maxValueSeven, 1<<valueFive | 1<<maxValueSeven, ""},
		{"5-7/1", minValueZero, maxValueSeven, 1<<valueFive | 1<<valueSix | 1<<maxValueSeven, ""},

		{fieldValueWildcard, minValueOne, maxValueThree, 1<<minValueOne | 1<<2 | 1<<maxValueThree | starBit, ""},
		{"*/2", minValueOne, maxValueThree, 1<<minValueOne | 1<<maxValueThree, ""},

		{"5--5", minValueZero, minValueZero, zero, "too many hyphens"},
		{"jan-x", minValueZero, minValueZero, zero, failedToParseIntText},
		{"2-x", minValueOne, valueFive, zero, failedToParseIntText},
		{"*/-12", minValueZero, minValueZero, zero, "negative number"},
		{"*//2", minValueZero, minValueZero, zero, "too many slashes"},
		{"1", maxValueThree, valueFive, zero, "below minimum"},
		{"6", maxValueThree, valueFive, zero, "above maximum"},
		{"5-3", maxValueThree, valueFive, zero, "beyond end of range"},
		{"*/0", minValueZero, minValueZero, zero, "should be a positive number"},
	}

	for _, testCase := range ranges {
		actual, err := getRange(testCase.expr, bounds{testCase.min, testCase.max, nil})
		if len(testCase.err) != 0 && (err == nil || !strings.Contains(err.Error(), testCase.err)) {
			t.Errorf("%s => expected %v, got %v", testCase.expr, testCase.err, err)
		}

		if len(testCase.err) == 0 && err != nil {
			t.Errorf(unexpectedErrorText, testCase.expr, err)
		}

		if actual != testCase.expected {
			t.Errorf("%s => expected %d, got %d", testCase.expr, testCase.expected, actual)
		}
	}
}

func TestField(t *testing.T) {
	t.Parallel()

	fields := []struct {
		expr     string
		min, max uint
		expected uint64
	}{
		{fieldValueFive, minValueOne, maxValueSeven, 1 << valueFive},
		{"5,6", minValueOne, maxValueSeven, 1<<valueFive | 1<<valueSix},
		{"5,6,7", minValueOne, maxValueSeven, 1<<valueFive | 1<<valueSix | 1<<maxValueSeven},
		{"1,5-7/2,3", minValueOne, maxValueSeven, 1<<minValueOne | 1<<valueFive | 1<<maxValueSeven | 1<<maxValueThree},
	}

	for _, testCase := range fields {
		actual, err := getField(testCase.expr, bounds{testCase.min, testCase.max, nil})
		if err != nil {
			t.Errorf(unexpectedErrorText, testCase.expr, err)
		}

		if actual != testCase.expected {
			t.Errorf("%s => expected %d, got %d", testCase.expr, testCase.expected, actual)
		}
	}
}

func TestAll(t *testing.T) {
	t.Parallel()

	minuteRange := minuteBounds()
	hourRange := hourBounds()
	domRange := dayOfMonthBounds()
	monthRange := monthBounds()
	dowRange := dayOfWeekBounds()

	allBits := []struct {
		r        bounds
		expected uint64
	}{
		{minuteRange, bitMaskZeroToFiftyNine},
		{hourRange, bitMaskZeroToTwentyFour},
		{domRange, bitMaskOneToThirtyOne},
		{monthRange, bitMaskOneToTwelve},
		{dowRange, bitMaskZeroToSix},
	}

	for _, testCase := range allBits {
		actual := all(testCase.r)
		if testCase.expected|starBit != actual {
			t.Errorf("%d-%d/%d => expected %b, got %b",
				testCase.r.min, testCase.r.max, 1, testCase.expected|starBit, actual)
		}
	}
}

func TestBits(t *testing.T) {
	t.Parallel()

	bits := []struct {
		min, max, step uint
		expected       uint64
	}{
		{minValueZero, minValueZero, minValueOne, singleBitZero},
		{minValueOne, minValueOne, minValueOne, singleBitOne},
		{minValueOne, valueFive, 2, alternatingBitsToFive},
		{minValueOne, maxValueFour, 2, alternatingBitsToFour},
	}

	for _, testCase := range bits {
		actual := getBits(testCase.min, testCase.max, testCase.step)
		if testCase.expected != actual {
			t.Errorf("%d-%d/%d => expected %b, got %b",
				testCase.min, testCase.max, testCase.step, testCase.expected, actual)
		}
	}
}

func TestParseScheduleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct{ expr, err string }{
		{"* 5 j * * *", failedToParseIntText},
		{"@every Xm", "failed to parse duration"},
		{"@every 0s", "interval must be greater than zero"},
		{"@every -5m", "interval must be greater than zero"},
		{"@unrecognized", "unrecognized descriptor"},
		{"CRON_TZ=UTC", "missing schedule after timezone prefix"},
		{"CRON_TZ=", "missing timezone location"},
		{"TZ=", "missing timezone location"},
		{"* * * *", "expected 5 to 6 fields"},
		{"", "empty spec string"},
	}
	parserWithSeconds := testParserWithSeconds()

	for _, testCase := range tests {
		actual, err := parserWithSeconds.Parse(testCase.expr)
		if err == nil || !strings.Contains(err.Error(), testCase.err) {
			t.Errorf("%s => expected %v, got %v", testCase.expr, testCase.err, err)
		}

		if actual != nil {
			t.Errorf("expected nil schedule on error, got %v", actual)
		}
	}
}

func TestParseSchedule(t *testing.T) {
	t.Parallel()

	tokyo := mustLoadLocation(t, "Asia/Tokyo")
	parserWithSeconds := testParserWithSeconds()

	entries := []struct {
		parser   Parser
		expr     string
		expected Schedule
	}{
		{parserWithSeconds, "0 5 * * * *", every5min(time.Local)},
		{NewStandardParser(), "5 * * * *", every5min(time.Local)},
		{parserWithSeconds, "CRON_TZ=UTC  0 5 * * * *", every5min(time.UTC)},
		{NewStandardParser(), "CRON_TZ=UTC  5 * * * *", every5min(time.UTC)},
		{parserWithSeconds, "CRON_TZ=Asia/Tokyo 0 5 * * * *", every5min(tokyo)},
		{parserWithSeconds, "@every 5m", ConstantDelaySchedule{fiveMinuteDelay}},
		{parserWithSeconds, "@every 500ms", ConstantDelaySchedule{time.Second}},
		{parserWithSeconds, "@midnight", midnight(time.Local)},
		{parserWithSeconds, "TZ=UTC  @midnight", midnight(time.UTC)},
		{parserWithSeconds, "TZ=Asia/Tokyo @midnight", midnight(tokyo)},
		{parserWithSeconds, "@yearly", annual(time.Local)},
		{parserWithSeconds, "@annually", annual(time.Local)},
		{
			parser: parserWithSeconds,
			expr:   "* 5 * * * *",
			expected: &SpecSchedule{
				Second:   all(secondBounds()),
				Minute:   1 << valueFive,
				Hour:     all(hourBounds()),
				Dom:      all(dayOfMonthBounds()),
				Month:    all(monthBounds()),
				Dow:      all(dayOfWeekBounds()),
				Location: time.Local,
			},
		},
	}

	for _, testCase := range entries {
		actual, err := testCase.parser.Parse(testCase.expr)
		if err != nil {
			t.Errorf(unexpectedErrorText, testCase.expr, err)
		}

		if !reflect.DeepEqual(actual, testCase.expected) {
			t.Errorf("%s => expected %b, got %b", testCase.expr, testCase.expected, actual)
		}
	}
}

func TestOptionalSecondSchedule(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor)
	entries := []struct {
		expr     string
		expected Schedule
	}{
		{"0 5 * * * *", every5min(time.Local)},
		{"5 5 * * * *", every5min5s(time.Local)},
		{"5 * * * *", every5min(time.Local)},
	}

	for _, testCase := range entries {
		actual, err := parser.Parse(testCase.expr)
		if err != nil {
			t.Errorf(unexpectedErrorText, testCase.expr, err)
		}

		if !reflect.DeepEqual(actual, testCase.expected) {
			t.Errorf("%s => expected %b, got %b", testCase.expr, testCase.expected, actual)
		}
	}
}

func TestNormalizeFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    []string
		options  ParseOption
		expected []string
	}{
		{
			"AllFields_NoOptional",
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
			Second | Minute | Hour | Dom | Month | Dow | Descriptor,
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
		},
		{
			"AllFields_SecondOptional_Provided",
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
			SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor,
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
		},
		{
			"AllFields_SecondOptional_NotProvided",
			[]string{fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
			SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor,
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
		},
		{
			"SubsetFields_NoOptional",
			[]string{fieldValueFive, fieldValueFifteen, fieldValueWildcard},
			Hour | Dom | Month,
			[]string{fieldValueZero, fieldValueZero, fieldValueFive, fieldValueFifteen, fieldValueWildcard, fieldValueWildcard},
		},
		{
			"SubsetFields_DowOptional_Provided",
			[]string{fieldValueFive, fieldValueFifteen, fieldValueWildcard, "4"},
			Hour | Dom | Month | DowOptional,
			[]string{fieldValueZero, fieldValueZero, fieldValueFive, fieldValueFifteen, fieldValueWildcard, "4"},
		},
		{
			"SubsetFields_DowOptional_NotProvided",
			[]string{fieldValueFive, fieldValueFifteen, fieldValueWildcard},
			Hour | Dom | Month | DowOptional,
			[]string{fieldValueZero, fieldValueZero, fieldValueFive, fieldValueFifteen, fieldValueWildcard, fieldValueWildcard},
		},
		{
			"SubsetFields_SecondOptional_NotProvided",
			[]string{fieldValueFive, fieldValueFifteen, fieldValueWildcard},
			SecondOptional | Hour | Dom | Month,
			[]string{fieldValueZero, fieldValueZero, fieldValueFive, fieldValueFifteen, fieldValueWildcard, fieldValueWildcard},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := normalizeFields(testCase.input, testCase.options)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(actual, testCase.expected) {
				t.Errorf("expected %v, got %v", testCase.expected, actual)
			}
		})
	}
}

func TestNormalizeFields_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   []string
		options ParseOption
		err     string
	}{
		{
			"TwoOptionals",
			[]string{fieldValueZero, fieldValueFive, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard, fieldValueWildcard},
			SecondOptional | Minute | Hour | Dom | Month | DowOptional,
			"",
		},
		{
			"TooManyFields",
			[]string{"0", "5", "*", "*"},
			SecondOptional | Minute | Hour,
			"",
		},
		{
			"NoFields",
			[]string{},
			SecondOptional | Minute | Hour,
			"",
		},
		{
			"TooFewFields",
			[]string{"*"},
			SecondOptional | Minute | Hour,
			"",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			actual, err := normalizeFields(testCase.input, testCase.options)
			if err == nil {
				t.Errorf("expected an error, got none. results: %v", actual)
			}

			if !strings.Contains(err.Error(), testCase.err) {
				t.Errorf("expected error %q, got %q", testCase.err, err.Error())
			}
		})
	}
}

func TestStandardSpecSchedule(t *testing.T) {
	t.Parallel()

	entries := []struct {
		expr     string
		expected Schedule
		err      string
	}{
		{
			expr: "5 * * * *",
			expected: &SpecSchedule{
				1 << secondBounds().min,
				1 << valueFive,
				all(hourBounds()),
				all(dayOfMonthBounds()),
				all(monthBounds()),
				all(dayOfWeekBounds()),
				time.Local,
			},
		},
		{
			expr:     "@every 5m",
			expected: ConstantDelaySchedule{fiveMinuteDelay},
		},
		{
			expr: "5 j * * *",
			err:  failedToParseIntText,
		},
		{
			expr: "* * * *",
			err:  "expected exactly 5 fields",
		},
	}

	for _, testCase := range entries {
		actual, err := ParseStandard(testCase.expr)
		if len(testCase.err) != 0 && (err == nil || !strings.Contains(err.Error(), testCase.err)) {
			t.Errorf("%s => expected %v, got %v", testCase.expr, testCase.err, err)
		}

		if len(testCase.err) == 0 && err != nil {
			t.Errorf(unexpectedErrorText, testCase.expr, err)
		}

		if !reflect.DeepEqual(actual, testCase.expected) {
			t.Errorf("%s => expected %b, got %b", testCase.expr, testCase.expected, actual)
		}
	}
}

func TestNoDescriptorParser(t *testing.T) {
	t.Parallel()

	parser := NewSpecParser(Minute | Hour)

	_, err := parser.Parse("@every 1m")
	if err == nil {
		t.Error("expected an error, got none")
	}
}

func every5min(loc *time.Location) *SpecSchedule {
	return &SpecSchedule{
		1 << secondBounds().min,
		1 << valueFive,
		all(hourBounds()),
		all(dayOfMonthBounds()),
		all(monthBounds()),
		all(dayOfWeekBounds()),
		loc,
	}
}

func every5min5s(loc *time.Location) *SpecSchedule {
	return &SpecSchedule{
		1 << valueFive,
		1 << valueFive,
		all(hourBounds()),
		all(dayOfMonthBounds()),
		all(monthBounds()),
		all(dayOfWeekBounds()),
		loc,
	}
}

func midnight(loc *time.Location) *SpecSchedule {
	return &SpecSchedule{1, 1, 1, all(dayOfMonthBounds()), all(monthBounds()), all(dayOfWeekBounds()), loc}
}

func annual(loc *time.Location) *SpecSchedule {
	return &SpecSchedule{
		Second:   1 << secondBounds().min,
		Minute:   1 << minuteBounds().min,
		Hour:     1 << hourBounds().min,
		Dom:      1 << dayOfMonthBounds().min,
		Month:    1 << monthBounds().min,
		Dow:      all(dayOfWeekBounds()),
		Location: loc,
	}
}
