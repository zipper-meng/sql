package ast

import (
	"fmt"
	"strings"
	"time"

	"sql/tools"
)

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements.
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise, uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *BinaryExpr:
		return expr.Name()
	case *ParenExpr:
		f := Field{Expr: expr.Expr}
		return f.Name()
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	str := f.Expr.String()

	if f.Alias == "" {
		return str
	}
	return fmt.Sprintf("%s AS %s", str, tools.QuoteIdent(f.Alias))
}

// Fields represents a list of fields.
type Fields []*Field

// AliasNames returns a list of calculated field names in
// order of alias, function name, then field.
func (a Fields) AliasNames() []string {
	names := []string{}
	for _, f := range a {
		names = append(names, f.Name())
	}
	return names
}

// Names returns a list of field names.
func (a Fields) Names() []string {
	names := []string{}
	for _, f := range a {
		switch expr := f.Expr.(type) {
		case *Call:
			names = append(names, expr.Name)
		case *VarRef:
			names = append(names, expr.Val)
		case *BinaryExpr:
			names = append(names, walkNames(expr)...)
		case *ParenExpr:
			names = append(names, walkNames(expr)...)
		}
	}
	return names
}

func (a Fields) FieldExprByName(name string) (int, Expr) {
	if a != nil {
		for i, f := range a {
			if f.Name() == name {
				return i, f.Expr
			} else if call, ok := f.Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") && len(call.Args) > 2 {
				for _, arg := range call.Args[1 : len(call.Args)-1] {
					if arg, ok := arg.(*VarRef); ok && arg.Val == name {
						return i, arg
					}
				}
			}
		}
	}
	return -1, nil
}

// String returns a string representation of the fields.
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Len implements sort.Interface.
func (a Fields) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Fields) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

// Swap implements sort.Interface.
func (a Fields) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Target represents a target (destination) ttl, metric, and DB.
type Target struct {
	// Metric to write into.
	Metric *Metric
}

// String returns a string representation of the Target.
func (t *Target) String() string {
	if t == nil {
		return ""
	}

	var buf strings.Builder
	_, _ = buf.WriteString("INTO ")
	_, _ = buf.WriteString(t.Metric.String())
	if t.Metric.Name == "" {
		_, _ = buf.WriteString(":METRIC")
	}

	return buf.String()
}

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.Expr.String() }

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// String returns a string representation of the dimensions.
func (a Dimensions) String() string {
	var str []string
	for _, d := range a {
		str = append(str, d.String())
	}
	return strings.Join(str, ", ")
}

// Normalize returns the interval and tag dimensions separately.
// Returns 0 if no time interval is specified.
func (a Dimensions) Normalize() (time.Duration, []string) {
	var dur time.Duration
	var tags []string

	for _, dim := range a {
		switch expr := dim.Expr.(type) {
		case *Call:
			lit, _ := expr.Args[0].(*DurationLiteral)
			dur = lit.Val
		case *VarRef:
			tags = append(tags, expr.Val)
		}
	}

	return dur, tags
}

// SortField represents a field to sort results by.
type SortField struct {
	// Name of the field.
	Name string

	// Sort order.
	Ascending bool
}

// String returns a string representation of a sort field.
func (field *SortField) String() string {
	var buf strings.Builder
	if field.Name != "" {
		_, _ = buf.WriteString(field.Name)
		_, _ = buf.WriteString(" ")
	}
	if field.Ascending {
		_, _ = buf.WriteString("ASC")
	} else {
		_, _ = buf.WriteString("DESC")
	}
	return buf.String()
}

// SortFields represents an ordered list of ORDER BY fields.
type SortFields []*SortField

// String returns a string representation of sort fields.
func (a SortFields) String() string {
	fields := make([]string, 0, len(a))
	for _, field := range a {
		fields = append(fields, field.String())
	}
	return strings.Join(fields, ", ")
}

// FillOption represents different options for filling aggregate windows.
type FillOption int

const (
	// NullFill means that empty aggregate windows will just have null values.
	NullFill FillOption = iota
	// NoFill means that empty aggregate windows will be purged from the result.
	NoFill
	// NumberFill means that empty aggregate windows will be filled with a provided number.
	NumberFill
	// PreviousFill means that empty aggregate windows will be filled with whatever the previous aggregate window had.
	PreviousFill
	// LinearFill means that empty aggregate windows will be filled with whatever a linear value between non null windows.
	LinearFill
)

// BoundParameter represents a bound parameter literal.
// This is not available to the query language itself, but can be used when
// constructing a query string from an AST.
type BoundParameter struct {
	Name string
}

// String returns a string representation of the bound parameter.
func (bp *BoundParameter) String() string {
	return fmt.Sprintf("$%s", tools.QuoteIdent(bp.Name))
}
