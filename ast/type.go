package ast

import (
	"time"
)

// DataType represents the primitive data-types available in CnosQL.
type DataType int

const (
	// Unknown primitive data-type.
	Unknown DataType = 0
	// Float means the data-type is a float.
	Float DataType = 1
	// Integer means the data-type is an integer.
	Integer DataType = 2
	// String means the data-type is a string of text.
	String DataType = 3
	// Boolean means the data-type is a boolean.
	Boolean DataType = 4
	// Time means the data-type is a time.
	Time DataType = 5
	// Duration means the data-type is a duration of time.
	Duration DataType = 6
	// Tag means the data-type is a tag.
	Tag DataType = 7
	// AnyField means the data-type is any field.
	AnyField DataType = 8
	// Unsigned means the data-type is an unsigned integer.
	Unsigned DataType = 9
)

// InspectDataType returns the data-type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Float
	case int64, int32, int:
		return Integer
	case string:
		return String
	case bool:
		return Boolean
	case uint64:
		return Unsigned
	case time.Time:
		return Time
	case time.Duration:
		return Duration
	default:
		return Unknown
	}
}

// DataTypeFromString returns a data-type given the string representation of that
// data-type.
func DataTypeFromString(s string) DataType {
	switch s {
	case "float":
		return Float
	case "integer":
		return Integer
	case "unsigned":
		return Unsigned
	case "string":
		return String
	case "boolean":
		return Boolean
	case "time":
		return Time
	case "duration":
		return Duration
	case "tag":
		return Tag
	case "field":
		return AnyField
	default:
		return Unknown
	}
}

// LessThan returns true if the other DataType has greater precedence than the
// current data-type. Unknown has the lowest precedence.
//
// NOTE: This is not the same as using the `<` or `>` operator because the
// integers used decrease with higher precedence, but Unknown is the lowest
// precedence at the zero value.
func (d DataType) LessThan(other DataType) bool {
	if d == Unknown {
		return true
	} else if d == Unsigned {
		return other != Unknown && other <= Integer
	} else if other == Unsigned {
		return d >= String
	}
	return other != Unknown && other < d
}

var (
	zeroFloat64  interface{} = float64(0)
	zeroInt64    interface{} = int64(0)
	zeroUint64   interface{} = uint64(0)
	zeroString   interface{} = ""
	zeroBoolean  interface{} = false
	zeroTime     interface{} = time.Time{}
	zeroDuration interface{} = time.Duration(0)
)

// Zero returns the zero value for the DataType.
// The return value of this method, when sent back to InspectDataType,
// may not produce the same value.
func (d DataType) Zero() interface{} {
	switch d {
	case Float:
		return zeroFloat64
	case Integer:
		return zeroInt64
	case Unsigned:
		return zeroUint64
	case String, Tag:
		return zeroString
	case Boolean:
		return zeroBoolean
	case Time:
		return zeroTime
	case Duration:
		return zeroDuration
	}
	return nil
}

// String returns the human-readable string representation of the DataType.
func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case Integer:
		return "integer"
	case Unsigned:
		return "unsigned"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	case Time:
		return "time"
	case Duration:
		return "duration"
	case Tag:
		return "tag"
	case AnyField:
		return "field"
	}
	return "unknown"
}

// TimeRange represents a range of time from Min to Max. The times are inclusive.
type TimeRange struct {
	Min, Max time.Time
}

// Intersect joins this TimeRange with another TimeRange.
func (t TimeRange) Intersect(other TimeRange) TimeRange {
	if !other.Min.IsZero() {
		if t.Min.IsZero() || other.Min.After(t.Min) {
			t.Min = other.Min
		}
	}
	if !other.Max.IsZero() {
		if t.Max.IsZero() || other.Max.Before(t.Max) {
			t.Max = other.Max
		}
	}
	return t
}

// IsZero is true if the min and max of the time range are zero.
func (t TimeRange) IsZero() bool {
	return t.Min.IsZero() && t.Max.IsZero()
}

// Used by TimeRange methods.
var minTime = time.Unix(0, MinTime)
var maxTime = time.Unix(0, MaxTime)

// MinTime returns the minimum time of the TimeRange.
// If the minimum time is zero, this returns the minimum possible time.
func (t TimeRange) MinTime() time.Time {
	if t.Min.IsZero() {
		return minTime
	}
	return t.Min
}

// MaxTime returns the maximum time of the TimeRange.
// If the maximum time is zero, this returns the maximum possible time.
func (t TimeRange) MaxTime() time.Time {
	if t.Max.IsZero() {
		return maxTime
	}
	return t.Max
}

// MinTimeNano returns the minimum time in nanoseconds since the epoch.
// If the minimum time is zero, this returns the minimum possible time.
func (t TimeRange) MinTimeNano() int64 {
	if t.Min.IsZero() {
		return MinTime
	}
	return t.Min.UnixNano()
}

// MaxTimeNano returns the maximum time in nanoseconds since the epoch.
// If the maximum time is zero, this returns the maximum possible time.
func (t TimeRange) MaxTimeNano() int64 {
	if t.Max.IsZero() {
		return MaxTime
	}
	return t.Max.UnixNano()
}
