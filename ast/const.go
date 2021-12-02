package ast

import (
	"errors"
	"math"
)

const (
	// MinTime is the minumum time that can be represented.
	//
	// 1677-09-21 00:12:43.145224194 +0000 UTC
	//
	// The two lowest minimum integers are used as sentinel values.  The
	// minimum value needs to be used as a value lower than any other value for
	// comparisons and another separate value is needed to act as a sentinel
	// default value that is unusable by the user, but usable internally.
	// Because these two values need to be used for a special purpose, we do
	// not allow users to write points at these two times.
	MinTime = int64(math.MinInt64) + 2

	// MaxTime is the maximum time that can be represented.
	//
	// 2262-04-11 23:47:16.854775806 +0000 UTC
	//
	// The highest time represented by a nanosecond needs to be used for an
	// exclusive range in the region, so the maximum time needs to be one
	// less than the possible maximum number of nanoseconds representable by an
	// int64 so that we don't lose a point at that one time.
	MaxTime = int64(math.MaxInt64) - 1
)

var (
	// ErrInvalidTime is returned when the timestamp string used to
	// compare against time field is invalid.
	ErrInvalidTime = errors.New("invalid timestamp string")
)
