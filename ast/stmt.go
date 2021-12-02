package ast

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var _ Statement = &SelectStatement{}

// Statement represents a single command in CnosQL.
type Statement interface {
	Node
	// stmt is unexported to ensure implementations of Statement
	// can only originate in this package.
	stmt()
}

func (*SelectStatement) stmt() {}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Target (destination) for the result of a SELECT INTO query.
	Target *Target

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	// Data sources (metrics) that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by.
	SortFields SortFields

	// Maximum number of rows to be returned. Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// Maxiumum number of series to be returned. Unlimited if zero.
	SLimit int

	// Returns series starting at an offset from the first one.
	SOffset int

	// Memoized group by interval from GroupBy().
	groupByInterval time.Duration

	// Whether it's a query for raw data values (i.e. not an aggregate).
	IsRawQuery bool

	// What fill option the select statement uses, if any.
	Fill FillOption

	// The value to fill empty aggregate buckets with, if any.
	FillValue interface{}

	// The timezone for the query, if any.
	Location *time.Location

	// Renames the implicit time field name.
	TimeAlias string

	// Removes the "time" column from the output.
	OmitTime bool

	// Removes metric name from resulting query. Useful for meta queries.
	StripName bool

	// Overrides the output metric name.
	EmitName string

	// Removes duplicate rows from raw queries.
	Dedupe bool
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf strings.Builder
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(s.Target.String())
	}
	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	switch s.Fill {
	case NoFill:
		_, _ = buf.WriteString(" fill(none)")
	case NumberFill:
		_, _ = buf.WriteString(fmt.Sprintf(" fill(%v)", s.FillValue))
	case LinearFill:
		_, _ = buf.WriteString(" fill(linear)")
	case PreviousFill:
		_, _ = buf.WriteString(" fill(previous)")
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.SLimit > 0 {
		_, _ = fmt.Fprintf(&buf, " SLIMIT %d", s.SLimit)
	}
	if s.SOffset > 0 {
		_, _ = fmt.Fprintf(&buf, " SOFFSET %d", s.SOffset)
	}
	if s.Location != nil {
		_, _ = fmt.Fprintf(&buf, ` TZ('%s')`, s.Location)
	}
	return buf.String()
}
