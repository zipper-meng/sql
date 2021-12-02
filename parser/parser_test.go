package parser_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"sql/ast"
	"sql/parser"
	"sql/token"
)

func TestParseQuery(t *testing.T) {
	s := `SELECT a FROM b; SELECT c FROM d`
	q, err := parser.NewParser(strings.NewReader(s)).ParseQuery()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(q.Statements) != 2 {
		t.Fatalf("unexpected statement count: %d", len(q.Statements))
	}
}

func TestParseStatement(t *testing.T) {
	now := time.Now()

	var tests = []struct {
		skip   bool
		s      string
		params map[string]interface{}
		stmt   ast.Statement
	}{
		{
			s: `SELECT * FROM ma`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.Wildcard{}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "ma"}},
			},
		},
		{
			s: `SELECT * FROM ma GROUP BY *`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.Wildcard{}},
				},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
				Dimensions: []*ast.Dimension{{Expr: &ast.Wildcard{}}},
			},
		},
		{
			s: `SELECT field1, * FROM ma GROUP BY *`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.VarRef{Val: "field1"}},
					{Expr: &ast.Wildcard{}},
				},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
				Dimensions: []*ast.Dimension{{Expr: &ast.Wildcard{}}},
			},
		},
		{
			s: `SELECT *, field1 FROM ma GROUP BY *`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.Wildcard{}},
					{Expr: &ast.VarRef{Val: "field1"}},
				},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
				Dimensions: []*ast.Dimension{{Expr: &ast.Wildcard{}}},
			},
		},

		// SELECT statement
		{
			s: fmt.Sprintf(`SELECT mean(field1), sum(field2), count(field3) AS field_x FROM ma WHERE host = 'hosta.org' and time > '%s' GROUP BY time(10h) ORDER BY DESC LIMIT 20 OFFSET 10;`, now.UTC().Format(time.RFC3339Nano)),
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "mean", Args: []ast.Expr{&ast.VarRef{Val: "field1"}}}},
					{Expr: &ast.Call{Name: "sum", Args: []ast.Expr{&ast.VarRef{Val: "field2"}}}},
					{Expr: &ast.Call{Name: "count", Args: []ast.Expr{&ast.VarRef{Val: "field3"}}}, Alias: "field_x"},
				},
				Sources: []ast.Source{&ast.Metric{Name: "ma"}},
				Condition: &ast.BinaryExpr{
					Op: token.AND,
					LHS: &ast.BinaryExpr{
						Op:  token.EQ,
						LHS: &ast.VarRef{Val: "host"},
						RHS: &ast.StringLiteral{Val: "hosta.org"},
					},
					RHS: &ast.BinaryExpr{
						Op:  token.GT,
						LHS: &ast.VarRef{Val: "time"},
						RHS: &ast.StringLiteral{Val: now.UTC().Format(time.RFC3339Nano)},
					},
				},
				Dimensions: []*ast.Dimension{{Expr: &ast.Call{Name: "time", Args: []ast.Expr{&ast.DurationLiteral{Val: 10 * time.Hour}}}}},
				SortFields: []*ast.SortField{
					{Ascending: false},
				},
				Limit:  20,
				Offset: 10,
			},
		},
		{
			s: `SELECT "foo.bar.baz" AS foo FROM ma`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.VarRef{Val: "foo.bar.baz"}, Alias: "foo"},
				},
				Sources: []ast.Source{&ast.Metric{Name: "ma"}},
			},
		},
		{
			s: `SELECT "foo.bar.baz" AS foo FROM foo`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{Expr: &ast.VarRef{Val: "foo.bar.baz"}, Alias: "foo"},
				},
				Sources: []ast.Source{&ast.Metric{Name: "foo"}},
			},
		},

		// SELECT statement (lowercase)
		{
			s: `select my_field FROM ma`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.VarRef{Val: "my_field"}}},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
			},
		},

		// SELECT statement (lowercase) with quoted field
		{
			s: `select 'my_field' FROM ma`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.StringLiteral{Val: "my_field"}}},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
			},
		},

		// SELECT statement with multiple ORDER BY fields
		{
			skip: true,
			s:    `SELECT field1 FROM ma ORDER BY ASC, field1, field2 DESC LIMIT 10`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.VarRef{Val: "field1"}}},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
				SortFields: []*ast.SortField{
					{Ascending: true},
					{Name: "field1"},
					{Name: "field2"},
				},
				Limit: 10,
			},
		},

		// SELECT statement with SLIMIT and SOFFSET
		{
			s: `SELECT field1 FROM ma SLIMIT 10 SOFFSET 5`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.VarRef{Val: "field1"}}},
				Sources:    []ast.Source{&ast.Metric{Name: "ma"}},
				SLimit:     10,
				SOffset:    5,
			},
		},

		// SELECT * FROM cpu WHERE host = 'serverC' AND region =~ /.*west.*/
		{
			s: `SELECT * FROM cpu WHERE host = 'serverC' AND region =~ /.*west.*/`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op: token.AND,
					LHS: &ast.BinaryExpr{
						Op:  token.EQ,
						LHS: &ast.VarRef{Val: "host"},
						RHS: &ast.StringLiteral{Val: "serverC"},
					},
					RHS: &ast.BinaryExpr{
						Op:  token.EQREGEX,
						LHS: &ast.VarRef{Val: "region"},
						RHS: &ast.RegexLiteral{Val: regexp.MustCompile(".*west.*")},
					},
				},
			},
		},

		// select percentile statements
		{
			s: `select percentile("field1", 2.0) from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "percentile", Args: []ast.Expr{&ast.VarRef{Val: "field1"}, &ast.NumberLiteral{Val: 2.0}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},
		{
			s: `select percentile("field1", 2.0), field2 from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "percentile", Args: []ast.Expr{&ast.VarRef{Val: "field1"}, &ast.NumberLiteral{Val: 2.0}}}},
					{Expr: &ast.VarRef{Val: "field2"}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},

		// select top statements
		{
			s: `select top("field1", 2) from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "top", Args: []ast.Expr{&ast.VarRef{Val: "field1"}, &ast.IntegerLiteral{Val: 2}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},

		{
			s: `select top(field1, 2) from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "top", Args: []ast.Expr{&ast.VarRef{Val: "field1"}, &ast.IntegerLiteral{Val: 2}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},
		{
			s: `select top(field1, tag1, 2), tag1 from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "top", Args: []ast.Expr{&ast.VarRef{Val: "field1"}, &ast.VarRef{Val: "tag1"}, &ast.IntegerLiteral{Val: 2}}}},
					{Expr: &ast.VarRef{Val: "tag1"}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},

		// select distinct statements
		{
			s: `select distinct(field1) from cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "distinct", Args: []ast.Expr{&ast.VarRef{Val: "field1"}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},
		{
			s: `select count(distinct field3) from metrics`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "count", Args: []ast.Expr{&ast.Distinct{Val: "field3"}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "metrics"}},
			},
		},
		{
			s: `select count(distinct field3), sum(field4) from metrics`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "count", Args: []ast.Expr{&ast.Distinct{Val: "field3"}}}},
					{Expr: &ast.Call{Name: "sum", Args: []ast.Expr{&ast.VarRef{Val: "field4"}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "metrics"}},
			},
		},

		{
			s: `select count(distinct(field3)), sum(field4) from metrics`,
			stmt: &ast.SelectStatement{
				IsRawQuery: false,
				Fields: []*ast.Field{
					{Expr: &ast.Call{Name: "count", Args: []ast.Expr{&ast.Call{Name: "distinct", Args: []ast.Expr{&ast.VarRef{Val: "field3"}}}}}},
					{Expr: &ast.Call{Name: "sum", Args: []ast.Expr{&ast.VarRef{Val: "field4"}}}},
				},
				Sources: []ast.Source{&ast.Metric{Name: "metrics"}},
			},
		},

		// SELECT * FROM WHERE time
		{
			s: fmt.Sprintf(`SELECT * FROM cpu WHERE time > '%s'`, now.UTC().Format(time.RFC3339Nano)),
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.GT,
					LHS: &ast.VarRef{Val: "time"},
					RHS: &ast.StringLiteral{Val: now.UTC().Format(time.RFC3339Nano)},
				},
			},
		},

		// SELECT * FROM WHERE field comparisons
		{
			s: `SELECT * FROM cpu WHERE load > 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.GT,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load >= 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.GTE,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load = 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.EQ,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load <= 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.LTE,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load < 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.LT,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load != 100`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources:    []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.NEQ,
					LHS: &ast.VarRef{Val: "load"},
					RHS: &ast.IntegerLiteral{Val: 100},
				},
			},
		},

		// SELECT * FROM /<regex>/
		{
			s: `SELECT * FROM /cpu.*/`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources: []ast.Source{&ast.Metric{
					Regex: &ast.RegexLiteral{Val: regexp.MustCompile("cpu.*")}},
				},
			},
		},

		// SELECT * FROM "db"."ttl"./<regex>/
		{
			s: `SELECT * FROM "db"."ttl"./cpu.*/`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources: []ast.Source{&ast.Metric{
					Database:   `db`,
					TimeToLive: `ttl`,
					Regex:      &ast.RegexLiteral{Val: regexp.MustCompile("cpu.*")}},
				},
			},
		},

		// SELECT * FROM "db"../<regex>/
		{
			s: `SELECT * FROM "db"../cpu.*/`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources: []ast.Source{&ast.Metric{
					Database: `db`,
					Regex:    &ast.RegexLiteral{Val: regexp.MustCompile("cpu.*")}},
				},
			},
		},

		// SELECT * FROM "ttl"./<regex>/
		{
			s: `SELECT * FROM "ttl"./cpu.*/`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields:     []*ast.Field{{Expr: &ast.Wildcard{}}},
				Sources: []ast.Source{&ast.Metric{
					TimeToLive: `ttl`,
					Regex:      &ast.RegexLiteral{Val: regexp.MustCompile("cpu.*")}},
				},
			},
		},

		// SELECT statement with fill
		{
			s: fmt.Sprintf(`SELECT mean(value) FROM cpu where time < '%s' GROUP BY time(5m) fill(1)`, now.UTC().Format(time.RFC3339Nano)),
			stmt: &ast.SelectStatement{
				Fields: []*ast.Field{{
					Expr: &ast.Call{
						Name: "mean",
						Args: []ast.Expr{&ast.VarRef{Val: "value"}}}}},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.LT,
					LHS: &ast.VarRef{Val: "time"},
					RHS: &ast.StringLiteral{Val: now.UTC().Format(time.RFC3339Nano)},
				},
				Dimensions: []*ast.Dimension{{Expr: &ast.Call{Name: "time", Args: []ast.Expr{&ast.DurationLiteral{Val: 5 * time.Minute}}}}},
				Fill:       ast.NumberFill,
				FillValue:  int64(1),
			},
		},

		// SELECT statement with FILL(none) -- check case insensitivity
		{
			s: fmt.Sprintf(`SELECT mean(value) FROM cpu where time < '%s' GROUP BY time(5m) FILL(none)`, now.UTC().Format(time.RFC3339Nano)),
			stmt: &ast.SelectStatement{
				Fields: []*ast.Field{{
					Expr: &ast.Call{
						Name: "mean",
						Args: []ast.Expr{&ast.VarRef{Val: "value"}}}}},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.LT,
					LHS: &ast.VarRef{Val: "time"},
					RHS: &ast.StringLiteral{Val: now.UTC().Format(time.RFC3339Nano)},
				},
				Dimensions: []*ast.Dimension{{Expr: &ast.Call{Name: "time", Args: []ast.Expr{&ast.DurationLiteral{Val: 5 * time.Minute}}}}},
				Fill:       ast.NoFill,
			},
		},

		// SELECT statement with previous fill
		{
			s: fmt.Sprintf(`SELECT mean(value) FROM cpu where time < '%s' GROUP BY time(5m) FILL(previous)`, now.UTC().Format(time.RFC3339Nano)),
			stmt: &ast.SelectStatement{
				Fields: []*ast.Field{{
					Expr: &ast.Call{
						Name: "mean",
						Args: []ast.Expr{&ast.VarRef{Val: "value"}}}}},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.LT,
					LHS: &ast.VarRef{Val: "time"},
					RHS: &ast.StringLiteral{Val: now.UTC().Format(time.RFC3339Nano)},
				},
				Dimensions: []*ast.Dimension{{Expr: &ast.Call{Name: "time", Args: []ast.Expr{&ast.DurationLiteral{Val: 5 * time.Minute}}}}},
				Fill:       ast.PreviousFill,
			},
		},

		// SELECT casts
		{
			s: `SELECT field1::float, field2::integer, field3::string, field4::boolean, field5::field, tag1::tag FROM cpu`,
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{
					{
						Expr: &ast.VarRef{
							Val:  "field1",
							Type: ast.Float,
						},
					},
					{
						Expr: &ast.VarRef{
							Val:  "field2",
							Type: ast.Integer,
						},
					},
					{
						Expr: &ast.VarRef{
							Val:  "field3",
							Type: ast.String,
						},
					},
					{
						Expr: &ast.VarRef{
							Val:  "field4",
							Type: ast.Boolean,
						},
					},
					{
						Expr: &ast.VarRef{
							Val:  "field5",
							Type: ast.AnyField,
						},
					},
					{
						Expr: &ast.VarRef{
							Val:  "tag1",
							Type: ast.Tag,
						},
					},
				},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
			},
		},

		// SELECT statement with a bound parameter
		{
			s: `SELECT value FROM cpu WHERE value > $value`,
			params: map[string]interface{}{
				"value": int64(2),
			},
			stmt: &ast.SelectStatement{
				IsRawQuery: true,
				Fields: []*ast.Field{{
					Expr: &ast.VarRef{Val: "value"}}},
				Sources: []ast.Source{&ast.Metric{Name: "cpu"}},
				Condition: &ast.BinaryExpr{
					Op:  token.GT,
					LHS: &ast.VarRef{Val: "value"},
					RHS: &ast.IntegerLiteral{Val: 2},
				},
			},
		},
	}

	for i, tt := range tests {
		if tt.skip {
			continue
		}
		p := parser.NewParser(strings.NewReader(tt.s))
		if tt.params != nil {
			p.SetParams(tt.params)
		}
		stmt, _ := p.ParseStatement()

		if !reflect.DeepEqual(tt.stmt, stmt) {
			t.Logf("\n# %s\nexp=%s\ngot=%s\n", tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt))
			t.Logf("\nSQL exp=%s\nSQL got=%s\n", tt.stmt.String(), stmt.String())
			t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
		}
	}
}

func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
