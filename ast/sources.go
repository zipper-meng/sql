package ast

import (
	"fmt"
	"strings"

	"sql/tools"
)

// Source represents a source of data for a statement.
type Source interface {
	Node
	// source is unexported to ensure implementations of Source
	// can only originate in this package.
	source()
}

func (*Metric) source()   {}
func (*SubQuery) source() {}

// Metric represents a single metric used as a datasource.
type Metric struct {
	Database   string
	TimeToLive string
	Name       string
	Regex      *RegexLiteral
	IsTarget   bool

	// This field indicates that the metric should read be read from the
	// specified system iterator.
	SystemIterator string
}

// Clone returns a deep clone of the Metric.
func (m *Metric) Clone() *Metric {
	var regexp *RegexLiteral
	if m.Regex != nil && m.Regex.Val != nil {
		regexp = &RegexLiteral{Val: m.Regex.Val.Copy()}
	}
	return &Metric{
		Database:       m.Database,
		TimeToLive:     m.TimeToLive,
		Name:           m.Name,
		Regex:          regexp,
		IsTarget:       m.IsTarget,
		SystemIterator: m.SystemIterator,
	}
}

// String returns a string representation of the metric.
func (m *Metric) String() string {
	var buf strings.Builder
	if m.Database != "" {
		_, _ = buf.WriteString(tools.QuoteIdent(m.Database))
		_, _ = buf.WriteString(".")
	}

	if m.TimeToLive != "" {
		_, _ = buf.WriteString(tools.QuoteIdent(m.TimeToLive))
	}

	if m.Database != "" || m.TimeToLive != "" {
		_, _ = buf.WriteString(`.`)
	}

	if m.Name != "" && m.SystemIterator == "" {
		_, _ = buf.WriteString(tools.QuoteIdent(m.Name))
	} else if m.SystemIterator != "" {
		_, _ = buf.WriteString(tools.QuoteIdent(m.SystemIterator))
	} else if m.Regex != nil {
		_, _ = buf.WriteString(m.Regex.String())
	}

	return buf.String()
}

// SubQuery is a source with a SelectStatement as the backing store.
type SubQuery struct {
	Statement *SelectStatement
}

// String returns a string representation of the subquery.
func (s *SubQuery) String() string {
	return fmt.Sprintf("(%s)", s.Statement.String())
}

// Sources represents a list of sources.
type Sources []Source

// String returns a string representation of a Sources array.
func (a Sources) String() string {
	var buf strings.Builder

	ubound := len(a) - 1
	for i, src := range a {
		_, _ = buf.WriteString(src.String())
		if i < ubound {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String()
}

// Metrics returns all metrics including ones embedded in subqueries.
func (a Sources) Metrics() []*Metric {
	mms := make([]*Metric, 0, len(a))
	for _, src := range a {
		switch src := src.(type) {
		case *Metric:
			mms = append(mms, src)
		case *SubQuery:
			mms = append(mms, src.Statement.Sources.Metrics()...)
		}
	}
	return mms
}

// Metrics represents a list of metrics.
type Metrics []*Metric

// String returns a string representation of the metrics.
func (a Metrics) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
}
