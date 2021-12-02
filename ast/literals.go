package ast

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"sql/tools"
)

const (
	// DateFormat represents the format for date literals.
	DateFormat = "2006-01-02"

	// DateTimeFormat represents the format for date time literals.
	DateTimeFormat = "2006-01-02 15:04:05.999999"
)

// Literal represents a static literal.
type Literal interface {
	Expr
	// literal is unexported to ensure implementations of Literal
	// can only originate in this package.
	literal()
}

func (*BooleanLiteral) literal()  {}
func (*BoundParameter) literal()  {}
func (*DurationLiteral) literal() {}
func (*IntegerLiteral) literal()  {}
func (*UnsignedLiteral) literal() {}
func (*NilLiteral) literal()      {}
func (*NumberLiteral) literal()   {}
func (*RegexLiteral) literal()    {}
func (*ListLiteral) literal()     {}
func (*StringLiteral) literal()   {}
func (*TimeLiteral) literal()     {}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string { return strconv.FormatFloat(l.Val, 'f', 3, 64) }

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Val int64
}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string { return fmt.Sprintf("%d", l.Val) }

// UnsignedLiteral represents an unsigned literal. The parser will only use an unsigned literal if the parsed
// integer is greater than math.MaxInt64.
type UnsignedLiteral struct {
	Val uint64
}

// String returns a string representation of the literal.
func (l *UnsignedLiteral) String() string { return strconv.FormatUint(l.Val, 10) }

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// isTrueLiteral returns true if the expression is a literal "true" value.
func isTrueLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == true
	}
	return false
}

// isFalseLiteral returns true if the expression is a literal "false" value.
func isFalseLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == false
	}
	return false
}

// ListLiteral represents a list of tag key literals.
type ListLiteral struct {
	Vals []string
}

// String returns a string representation of the literal.
func (s *ListLiteral) String() string {
	var buf strings.Builder
	_, _ = buf.WriteString("(")
	for idx, tagKey := range s.Vals {
		if idx != 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(tools.QuoteIdent(tagKey))
	}
	_, _ = buf.WriteString(")")
	return buf.String()
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return tools.QuoteString(l.Val) }

// IsTimeLiteral returns if this string can be interpreted as a time literal.
func (l *StringLiteral) IsTimeLiteral() bool {
	return tools.IsDateTimeString(l.Val) || tools.IsDateString(l.Val)
}

// ToTimeLiteral returns a time literal if this string can be converted to a time literal.
func (l *StringLiteral) ToTimeLiteral(loc *time.Location) (*TimeLiteral, error) {
	if loc == nil {
		loc = time.UTC
	}

	if tools.IsDateTimeString(l.Val) {
		t, err := time.ParseInLocation(DateTimeFormat, l.Val, loc)
		if err != nil {
			// try to parse it as an RFCNano time
			t, err = time.ParseInLocation(time.RFC3339Nano, l.Val, loc)
			if err != nil {
				return nil, ErrInvalidTime
			}
		}
		return &TimeLiteral{Val: t}, nil
	} else if tools.IsDateString(l.Val) {
		t, err := time.ParseInLocation(DateFormat, l.Val, loc)
		if err != nil {
			return nil, ErrInvalidTime
		}
		return &TimeLiteral{Val: t}, nil
	}
	return nil, ErrInvalidTime
}

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string {
	return `'` + l.Val.UTC().Format(time.RFC3339Nano) + `'`
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

// String returns a string representation of the literal.
func (l *DurationLiteral) String() string { return tools.FormatDuration(l.Val) }

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string {
	if r.Val != nil {
		return fmt.Sprintf("/%s/", strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}
	return ""
}

// NilLiteral represents a nil literal.
// This is not available to the query language itself. It's only used internally.
type NilLiteral struct{}

// String returns a string representation of the literal.
func (l *NilLiteral) String() string { return `nil` }
