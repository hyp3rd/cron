package cron

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var (
	errEmptySpec               = errors.New("empty spec string")
	errDescriptorNotAllowed    = errors.New("parser does not accept descriptors")
	errMultipleOptionals       = errors.New("multiple optionals may not be configured")
	errUnexpectedFieldCount    = errors.New("unexpected field count")
	errUnknownOptional         = errors.New("unknown optional field")
	errTooManySlashes          = errors.New("too many slashes")
	errTooManyHyphens          = errors.New("too many hyphens")
	errRangeBelowMinimum       = errors.New("beginning of range below minimum")
	errRangeAboveMaximum       = errors.New("end of range above maximum")
	errRangeStartBeyondEnd     = errors.New("beginning of range beyond end of range")
	errRangeStepMustBePositive = errors.New("step of range should be a positive number")
	errNegativeNumber          = errors.New("negative number not allowed")
	errUnrecognizedDescriptor  = errors.New("unrecognized descriptor")
)

const (
	splitPairCount              = 2
	wrappedErrorWithValueFormat = "%w: %s"
)

// ParseOption represents configuration options for creating a parser. Most options specify which
// fields should be included, while others enable features. If a field is not
// included the parser will assume a default value. These options do not change
// the order fields are parse in.
type ParseOption int

const (
	// Second field, default 0.
	Second ParseOption = 1 << iota
	// SecondOptional seconds field, default 0.
	SecondOptional
	// Minute field, default 0.
	Minute
	// Hour field, default 0.
	Hour
	// Dom - Day of month field, default *.
	Dom
	// Month field, default *.
	Month
	// Dow - Day of week field, default *.
	Dow
	// DowOptional - Optional day of week field, default *.
	DowOptional
	// Descriptor - Allow descriptors such as @monthly, @weekly, etc.
	Descriptor
)

func places() []ParseOption {
	return []ParseOption{
		Second,
		Minute,
		Hour,
		Dom,
		Month,
		Dow,
	}
}

func defaults() []string {
	return []string{
		"0",
		"0",
		"0",
		"*",
		"*",
		"*",
	}
}

// Parser that can be configured.
type Parser struct {
	options ParseOption
}

// NewParser creates a Parser with custom options.
//
// It panics if more than one Optional is given, since it would be impossible to
// correctly infer which optional is provided or missing in general.
//
// Examples
//
//	// Standard parser without descriptors
//	specParser := NewParser(Minute | Hour | Dom | Month | Dow)
//	sched, err := specParser.Parse("0 0 15 */3 *")
//
//	// Same as above, just excludes time fields
//	specParser := NewParser(Dom | Month | Dow)
//	sched, err := specParser.Parse("15 */3 *")
//
//	// Same as above, just makes Dow optional
//	specParser := NewParser(Dom | Month | DowOptional)
//	sched, err := specParser.Parse("15 */3")
func NewParser(options ParseOption) Parser {
	optionals := 0
	if options&DowOptional > 0 {
		optionals++
	}

	if options&SecondOptional > 0 {
		optionals++
	}

	if optionals > 1 {
		panic("multiple optionals may not be configured")
	}

	return Parser{options}
}

// NewStandardParser returns a Parser configured to parse standard 5-field crontab specs.
func NewStandardParser() Parser {
	return NewParser(
		Minute | Hour | Dom | Month | Dow | Descriptor,
	)
}

// Parse returns a new crontab schedule representing the given spec.
// It returns a descriptive error if the spec is not valid.
// It accepts crontab specs and features configured by NewParser.
func (p Parser) Parse(spec string) (Schedule, error) {
	if len(spec) == 0 {
		return nil, errEmptySpec
	}

	spec, loc, err := extractLocation(spec)
	if err != nil {
		return nil, err
	}

	// Handle named schedules (descriptors), if configured
	if strings.HasPrefix(spec, "@") {
		if p.options&Descriptor == 0 {
			return nil, fmt.Errorf(wrappedErrorWithValueFormat, errDescriptorNotAllowed, spec)
		}

		return parseDescriptor(spec, loc)
	}

	fields, err := normalizeFields(strings.Fields(spec), p.options)
	if err != nil {
		return nil, err
	}

	schedule, err := parseScheduleFields(fields, loc)
	if err != nil {
		return nil, err
	}

	return schedule, nil
}

// normalizeFields takes a subset set of the time fields and returns the full set
// with defaults (zeroes) populated for unset fields.
//
// As part of performing this function, it also validates that the provided
// fields are compatible with the configured options.
func normalizeFields(fields []string, options ParseOption) ([]string, error) {
	normalizedOptions, optionalCount, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}

	fieldCounts := fieldCountBounds(normalizedOptions, optionalCount)

	err = validateFieldCount(fields, fieldCounts.min, fieldCounts.max)
	if err != nil {
		return nil, err
	}

	fields, err = populateOptionalField(fields, normalizedOptions, fieldCounts.min, fieldCounts.max)
	if err != nil {
		return nil, err
	}

	return expandFields(fields, normalizedOptions), nil
}

// ParseStandard returns a new crontab schedule representing the given
// standardSpec (https://en.wikipedia.org/wiki/Cron). It requires 5 entries
// representing: minute, hour, day of month, month and day of week, in that
// order. It returns a descriptive error if the spec is not valid.
//
// It accepts
//   - Standard crontab specs, e.g. "* * * * ?"
//   - Descriptors, e.g. "@midnight", "@every 1h30m"
func ParseStandard(standardSpec string) (Schedule, error) {
	return NewStandardParser().Parse(standardSpec)
}

// getField returns an Int with the bits set representing all of the times that
// the field represents or error parsing field value.  A "field" is a comma-separated
// list of "ranges".
func getField(field string, r bounds) (uint64, error) {
	var bits uint64

	ranges := strings.FieldsFunc(field, func(r rune) bool { return r == ',' })
	for _, expr := range ranges {
		bit, err := getRange(expr, r)
		if err != nil {
			return bits, err
		}

		bits |= bit
	}

	return bits, nil
}

// getRange returns the bits indicated by the given expression:
//
//	number | number "-" number [ "/" number ]
//
// or error parsing range.
func getRange(expr string, valueBounds bounds) (uint64, error) {
	stepResult, err := parseStep(expr)
	if err != nil {
		return 0, err
	}

	parsedRange, singleValue, err := parseRangeExpr(stepResult.rangeExpr, valueBounds, expr)
	if err != nil {
		return 0, err
	}

	if stepResult.hasStep && singleValue {
		parsedRange.end = valueBounds.max
	}

	if stepResult.hasStep && stepResult.step > 1 {
		parsedRange.extra = 0
	}

	err = validateRange(parsedRange, stepResult.step, valueBounds, expr)
	if err != nil {
		return 0, err
	}

	return getBits(parsedRange.start, parsedRange.end, stepResult.step) | parsedRange.extra, nil
}

func normalizeOptions(options ParseOption) (ParseOption, int, error) {
	optionalCount := 0

	if options&SecondOptional > 0 {
		options |= Second
		optionalCount++
	}

	if options&DowOptional > 0 {
		options |= Dow
		optionalCount++
	}

	if optionalCount > 1 {
		return 0, 0, errMultipleOptionals
	}

	return options, optionalCount, nil
}

type fieldCountRange struct {
	min int
	max int
}

type stepParseResult struct {
	rangeExpr string
	step      uint
	hasStep   bool
}

func fieldCountBounds(options ParseOption, optionalCount int) fieldCountRange {
	maxFields := 0

	for _, place := range places() {
		if options&place > 0 {
			maxFields++
		}
	}

	return fieldCountRange{
		min: maxFields - optionalCount,
		max: maxFields,
	}
}

func validateFieldCount(fields []string, minFields, maxFields int) error {
	count := len(fields)
	if count >= minFields && count <= maxFields {
		return nil
	}

	if minFields == maxFields {
		return fmt.Errorf("%w: expected exactly %d fields, found %d: %v", errUnexpectedFieldCount, minFields, count, fields)
	}

	return fmt.Errorf("%w: expected %d to %d fields, found %d: %v", errUnexpectedFieldCount, minFields, maxFields, count, fields)
}

func extractLocation(spec string) (string, *time.Location, error) {
	loc := time.Local

	if !strings.HasPrefix(spec, "TZ=") && !strings.HasPrefix(spec, "CRON_TZ=") {
		return spec, loc, nil
	}

	i := strings.Index(spec, " ")
	eq := strings.Index(spec, "=")

	loc, err := time.LoadLocation(spec[eq+1 : i])
	if err != nil {
		return "", nil, fmt.Errorf("provided bad location %s: %w", spec[eq+1:i], err)
	}

	return strings.TrimSpace(spec[i:]), loc, nil
}

func parseScheduleFields(fields []string, loc *time.Location) (*SpecSchedule, error) {
	second, err := getField(fields[0], secondBounds())
	if err != nil {
		return nil, err
	}

	minute, err := getField(fields[1], minuteBounds())
	if err != nil {
		return nil, err
	}

	hour, err := getField(fields[2], hourBounds())
	if err != nil {
		return nil, err
	}

	dayofmonth, err := getField(fields[3], dayOfMonthBounds())
	if err != nil {
		return nil, err
	}

	month, err := getField(fields[4], monthBounds())
	if err != nil {
		return nil, err
	}

	dayofweek, err := getField(fields[5], dayOfWeekBounds())
	if err != nil {
		return nil, err
	}

	return &SpecSchedule{
		Second:   second,
		Minute:   minute,
		Hour:     hour,
		Dom:      dayofmonth,
		Month:    month,
		Dow:      dayofweek,
		Location: loc,
	}, nil
}

func populateOptionalField(fields []string, options ParseOption, minFields, maxFields int) ([]string, error) {
	if minFields == maxFields || len(fields) != minFields {
		return fields, nil
	}

	defaultFields := defaults()

	switch {
	case options&DowOptional > 0:
		return append(fields, defaultFields[5]), nil
	case options&SecondOptional > 0:
		return append([]string{defaultFields[0]}, fields...), nil
	default:
		return nil, errUnknownOptional
	}
}

func expandFields(fields []string, options ParseOption) []string {
	fieldIndex := 0
	expandedFields := make([]string, len(places()))
	copy(expandedFields, defaults())

	for i, place := range places() {
		if options&place > 0 {
			expandedFields[i] = fields[fieldIndex]
			fieldIndex++
		}
	}

	return expandedFields
}

type rangeBits struct {
	start uint
	end   uint
	extra uint64
}

func parseStep(expr string) (stepParseResult, error) {
	rangeAndStep := strings.Split(expr, "/")
	switch len(rangeAndStep) {
	case 1:
		return stepParseResult{rangeExpr: rangeAndStep[0], step: 1}, nil
	case splitPairCount:
		step, err := mustParseInt(rangeAndStep[1])
		if err != nil {
			return stepParseResult{}, err
		}

		return stepParseResult{
			rangeExpr: rangeAndStep[0],
			step:      step,
			hasStep:   true,
		}, nil
	default:
		return stepParseResult{}, fmt.Errorf(wrappedErrorWithValueFormat, errTooManySlashes, expr)
	}
}

func parseRangeExpr(rangeExpr string, valueBounds bounds, originalExpr string) (rangeBits, bool, error) {
	lowAndHigh := strings.Split(rangeExpr, "-")
	singleValue := len(lowAndHigh) == 1

	if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
		return rangeBits{
			start: valueBounds.min,
			end:   valueBounds.max,
			extra: starBit,
		}, singleValue, nil
	}

	start, err := parseIntOrName(lowAndHigh[0], valueBounds.names)
	if err != nil {
		return rangeBits{}, false, err
	}

	switch len(lowAndHigh) {
	case 1:
		return rangeBits{start: start, end: start}, true, nil
	case splitPairCount:
		end, err := parseIntOrName(lowAndHigh[1], valueBounds.names)
		if err != nil {
			return rangeBits{}, false, err
		}

		return rangeBits{start: start, end: end}, false, nil
	default:
		return rangeBits{}, false, fmt.Errorf(wrappedErrorWithValueFormat, errTooManyHyphens, originalExpr)
	}
}

func validateRange(parsedRange rangeBits, step uint, valueBounds bounds, expr string) error {
	if parsedRange.start < valueBounds.min {
		return fmt.Errorf("%w: (%d) below minimum (%d): %s", errRangeBelowMinimum, parsedRange.start, valueBounds.min, expr)
	}

	if parsedRange.end > valueBounds.max {
		return fmt.Errorf("%w: (%d) above maximum (%d): %s", errRangeAboveMaximum, parsedRange.end, valueBounds.max, expr)
	}

	if parsedRange.start > parsedRange.end {
		return fmt.Errorf("%w: (%d) beyond end of range (%d): %s", errRangeStartBeyondEnd, parsedRange.start, parsedRange.end, expr)
	}

	if step == 0 {
		return fmt.Errorf(wrappedErrorWithValueFormat, errRangeStepMustBePositive, expr)
	}

	return nil
}

// parseIntOrName returns the (possibly-named) integer contained in expr.
func parseIntOrName(expr string, names map[string]uint) (uint, error) {
	if names != nil {
		if namedInt, ok := names[strings.ToLower(expr)]; ok {
			return namedInt, nil
		}
	}

	return mustParseInt(expr)
}

// mustParseInt parses the given expression as an int or returns an error.
func mustParseInt(expr string) (uint, error) {
	num, err := strconv.Atoi(expr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int from %s: %w", expr, err)
	}

	if num < 0 {
		return 0, fmt.Errorf("%w: (%d): %s", errNegativeNumber, num, expr)
	}

	return uint(num), nil
}

// getBits sets all bits in the range [lower, upper], modulo the given step size.
func getBits(lower, upper, step uint) uint64 {
	var bits uint64

	// If step is 1, use shifts.
	if step == 1 {
		return ^(math.MaxUint64 << (upper + 1)) & (math.MaxUint64 << lower)
	}

	// Else, use a simple loop.
	for i := lower; i <= upper; i += step {
		bits |= 1 << i
	}

	return bits
}

// all returns all bits within the given bounds.  (plus the star bit).
func all(r bounds) uint64 {
	return getBits(r.min, r.max, 1) | starBit
}

// parseDescriptor returns a predefined schedule for the expression, or error if none matches.
func parseDescriptor(descriptor string, loc *time.Location) (Schedule, error) {
	secondRange := secondBounds()
	minuteRange := minuteBounds()
	hourRange := hourBounds()
	domRange := dayOfMonthBounds()
	monthRange := monthBounds()
	dowRange := dayOfWeekBounds()

	switch descriptor {
	case "@yearly", "@annually":
		return &SpecSchedule{
			Second:   1 << secondRange.min,
			Minute:   1 << minuteRange.min,
			Hour:     1 << hourRange.min,
			Dom:      1 << domRange.min,
			Month:    1 << monthRange.min,
			Dow:      all(dowRange),
			Location: loc,
		}, nil

	case "@monthly":
		return &SpecSchedule{
			Second:   1 << secondRange.min,
			Minute:   1 << minuteRange.min,
			Hour:     1 << hourRange.min,
			Dom:      1 << domRange.min,
			Month:    all(monthRange),
			Dow:      all(dowRange),
			Location: loc,
		}, nil

	case "@weekly":
		return &SpecSchedule{
			Second:   1 << secondRange.min,
			Minute:   1 << minuteRange.min,
			Hour:     1 << hourRange.min,
			Dom:      all(domRange),
			Month:    all(monthRange),
			Dow:      1 << dowRange.min,
			Location: loc,
		}, nil

	case "@daily", "@midnight":
		return &SpecSchedule{
			Second:   1 << secondRange.min,
			Minute:   1 << minuteRange.min,
			Hour:     1 << hourRange.min,
			Dom:      all(domRange),
			Month:    all(monthRange),
			Dow:      all(dowRange),
			Location: loc,
		}, nil

	case "@hourly":
		return &SpecSchedule{
			Second:   1 << secondRange.min,
			Minute:   1 << minuteRange.min,
			Hour:     all(hourRange),
			Dom:      all(domRange),
			Month:    all(monthRange),
			Dow:      all(dowRange),
			Location: loc,
		}, nil
	}

	const every = "@every "
	if strings.HasPrefix(descriptor, every) {
		duration, err := time.ParseDuration(descriptor[len(every):])
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration %s: %w", descriptor, err)
		}

		return Every(duration), nil
	}

	return nil, fmt.Errorf(wrappedErrorWithValueFormat, errUnrecognizedDescriptor, descriptor)
}
