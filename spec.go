package cron

import "time"

// SpecSchedule specifies a duty cycle (to the second granularity), based on a
// traditional crontab specification. It is computed initially and stored as bit sets.
type SpecSchedule struct {
	Second, Minute, Hour, Dom, Month, Dow uint64

	// Override location for this schedule.
	Location *time.Location
}

// bounds provides a range of acceptable values (plus a map of name to value).
type bounds struct {
	min, max uint
	names    map[string]uint
}

const (
	// Set the top bit if a star was included in the expression.
	starBit = 1 << 63

	middayHour     = 12
	hoursPerDay    = 24
	yearSearchSpan = 5

	minSecondValue     uint = 0
	maxSecondValue     uint = 59
	minMinuteValue     uint = 0
	maxMinuteValue     uint = 59
	minHourValue       uint = 0
	maxHourValue       uint = 23
	minDayOfMonthValue uint = 1
	maxDayOfMonthValue uint = 31
	minMonthValue      uint = 1
	maxMonthValue      uint = 12
	minDayOfWeekValue  uint = 0
	maxDayOfWeekValue  uint = 6

	monthJanuaryValue   uint = 1
	monthFebruaryValue  uint = 2
	monthMarchValue     uint = 3
	monthAprilValue     uint = 4
	monthMayValue       uint = 5
	monthJuneValue      uint = 6
	monthJulyValue      uint = 7
	monthAugustValue    uint = 8
	monthSeptemberValue uint = 9
	monthOctoberValue   uint = 10
	monthNovemberValue  uint = 11
	monthDecemberValue  uint = 12

	weekdaySundayValue    uint = 0
	weekdayMondayValue    uint = 1
	weekdayTuesdayValue   uint = 2
	weekdayWednesdayValue uint = 3
	weekdayThursdayValue  uint = 4
	weekdayFridayValue    uint = 5
	weekdaySaturdayValue  uint = 6
)

func secondBounds() bounds {
	return bounds{minSecondValue, maxSecondValue, nil}
}

func minuteBounds() bounds {
	return bounds{minMinuteValue, maxMinuteValue, nil}
}

func hourBounds() bounds {
	return bounds{minHourValue, maxHourValue, nil}
}

func dayOfMonthBounds() bounds {
	return bounds{minDayOfMonthValue, maxDayOfMonthValue, nil}
}

func monthBounds() bounds {
	return bounds{minMonthValue, maxMonthValue, map[string]uint{
		"jan": monthJanuaryValue,
		"feb": monthFebruaryValue,
		"mar": monthMarchValue,
		"apr": monthAprilValue,
		"may": monthMayValue,
		"jun": monthJuneValue,
		"jul": monthJulyValue,
		"aug": monthAugustValue,
		"sep": monthSeptemberValue,
		"oct": monthOctoberValue,
		"nov": monthNovemberValue,
		"dec": monthDecemberValue,
	}}
}

func dayOfWeekBounds() bounds {
	return bounds{minDayOfWeekValue, maxDayOfWeekValue, map[string]uint{
		"sun": weekdaySundayValue,
		"mon": weekdayMondayValue,
		"tue": weekdayTuesdayValue,
		"wed": weekdayWednesdayValue,
		"thu": weekdayThursdayValue,
		"fri": weekdayFridayValue,
		"sat": weekdaySaturdayValue,
	}}
}

// Next returns the next time this schedule is activated, greater than the given
// time. If no time can be found to satisfy the schedule, return the zero time.
func (s *SpecSchedule) Next(candidate time.Time) time.Time {
	origLocation := candidate.Location()

	nextActivation, ok := s.nextActivation(candidate)
	if !ok {
		return time.Time{}
	}

	return nextActivation.In(origLocation)
}

type nextActivationState struct {
	schedule  *SpecSchedule
	added     bool
	location  *time.Location
	yearLimit int
}

func (s *SpecSchedule) nextActivation(candidate time.Time) (time.Time, bool) {
	candidate, location := s.prepareNext(candidate)

	state := nextActivationState{
		schedule:  s,
		location:  location,
		yearLimit: candidate.Year() + yearSearchSpan,
	}

	return state.find(candidate)
}

func (state *nextActivationState) find(candidate time.Time) (time.Time, bool) {
	for candidate.Year() <= state.yearLimit {
		var wrapped bool

		candidate, wrapped = state.advance(candidate)
		if !wrapped {
			return candidate, true
		}
	}

	return time.Time{}, false
}

func (state *nextActivationState) advance(candidate time.Time) (time.Time, bool) {
	steps := [...]func(time.Time) (time.Time, bool){
		func(candidate time.Time) (time.Time, bool) {
			return state.schedule.advanceMonth(candidate, &state.added, state.location)
		},
		func(candidate time.Time) (time.Time, bool) {
			return state.schedule.advanceDay(candidate, &state.added, state.location)
		},
		func(candidate time.Time) (time.Time, bool) {
			return state.schedule.advanceHour(candidate, &state.added, state.location)
		},
		func(candidate time.Time) (time.Time, bool) {
			return state.schedule.advanceMinute(candidate, &state.added)
		},
		func(candidate time.Time) (time.Time, bool) {
			return state.schedule.advanceSecond(candidate, &state.added)
		},
	}

	for _, step := range steps {
		var wrapped bool

		candidate, wrapped = step(candidate)
		if wrapped {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) prepareNext(candidate time.Time) (time.Time, *time.Location) {
	loc := s.Location
	if loc == time.Local {
		loc = candidate.Location()
	}

	if s.Location != time.Local {
		candidate = candidate.In(s.Location)
	}

	return candidate.Add(time.Second - time.Duration(candidate.Nanosecond())*time.Nanosecond), loc
}

func (s *SpecSchedule) advanceMonth(candidate time.Time, added *bool, loc *time.Location) (time.Time, bool) {
	for !s.monthMatches(candidate) {
		candidate = resetMonth(candidate, added, loc)
		candidate = candidate.AddDate(0, 1, 0)

		if candidate.Month() == time.January {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) advanceDay(candidate time.Time, added *bool, loc *time.Location) (time.Time, bool) {
	for !dayMatches(s, candidate) {
		candidate = resetDay(candidate, added, loc)
		candidate = normalizeMidnight(candidate.AddDate(0, 0, 1))

		if candidate.Day() == 1 {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) advanceHour(candidate time.Time, added *bool, loc *time.Location) (time.Time, bool) {
	for !s.hourMatches(candidate) {
		candidate = resetHour(candidate, added, loc)
		candidate = candidate.Add(time.Hour)

		if candidate.Hour() == 0 {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) advanceMinute(candidate time.Time, added *bool) (time.Time, bool) {
	for !s.minuteMatches(candidate) {
		candidate = truncateMinute(candidate, added)
		candidate = candidate.Add(time.Minute)

		if candidate.Minute() == 0 {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) advanceSecond(candidate time.Time, added *bool) (time.Time, bool) {
	for !s.secondMatches(candidate) {
		candidate = truncateSecond(candidate, added)
		candidate = candidate.Add(time.Second)

		if candidate.Second() == 0 {
			return candidate, true
		}
	}

	return candidate, false
}

func (s *SpecSchedule) monthMatches(candidate time.Time) bool {
	return hasBit(s.Month, int(candidate.Month()))
}

func (s *SpecSchedule) hourMatches(candidate time.Time) bool {
	return hasBit(s.Hour, candidate.Hour())
}

func (s *SpecSchedule) minuteMatches(candidate time.Time) bool {
	return hasBit(s.Minute, candidate.Minute())
}

func (s *SpecSchedule) secondMatches(candidate time.Time) bool {
	return hasBit(s.Second, candidate.Second())
}

func resetMonth(candidate time.Time, added *bool, loc *time.Location) time.Time {
	if *added {
		return candidate
	}

	*added = true

	return time.Date(candidate.Year(), candidate.Month(), 1, 0, 0, 0, 0, loc)
}

func resetDay(candidate time.Time, added *bool, loc *time.Location) time.Time {
	if *added {
		return candidate
	}

	*added = true

	return time.Date(candidate.Year(), candidate.Month(), candidate.Day(), 0, 0, 0, 0, loc)
}

func resetHour(candidate time.Time, added *bool, loc *time.Location) time.Time {
	if *added {
		return candidate
	}

	*added = true

	return time.Date(candidate.Year(), candidate.Month(), candidate.Day(), candidate.Hour(), 0, 0, 0, loc)
}

func truncateMinute(candidate time.Time, added *bool) time.Time {
	if *added {
		return candidate
	}

	*added = true

	return candidate.Truncate(time.Minute)
}

func truncateSecond(candidate time.Time, added *bool) time.Time {
	if *added {
		return candidate
	}

	*added = true

	return candidate.Truncate(time.Second)
}

// normalizeMidnight handles DST regimes where local midnight does not exist.
func normalizeMidnight(candidate time.Time) time.Time {
	if candidate.Hour() == 0 {
		return candidate
	}

	if candidate.Hour() > middayHour {
		return candidate.Add(time.Duration(hoursPerDay-candidate.Hour()) * time.Hour)
	}

	return candidate.Add(time.Duration(-candidate.Hour()) * time.Hour)
}

// dayMatches returns true if the schedule's day-of-week and day-of-month
// restrictions are satisfied by the given time.
func dayMatches(s *SpecSchedule, candidate time.Time) bool {
	var (
		domMatch = hasBit(s.Dom, candidate.Day())
		dowMatch = hasBit(s.Dow, int(candidate.Weekday()))
	)
	if s.Dom&starBit > 0 || s.Dow&starBit > 0 {
		return domMatch && dowMatch
	}

	return domMatch || dowMatch
}

func hasBit(bitset uint64, position int) bool {
	return uint64(1)<<position&bitset > 0
}
