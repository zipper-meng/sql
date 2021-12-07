package main

import (
	"sql/ast"
	"time"
)

func main() {
	v := &visitor{}

	n := &ast.SelectStatement{
		Fields: []*ast.Field{
			{Expr: &ast.VarRef{Val: "ta"}},
			{Expr: &ast.VarRef{Val: "tb"}},
			{Expr: &ast.VarRef{Val: "fa"}},
		},
		Sources: []ast.Source{
			&ast.Metric{Name: "ma"},
		},
		Dimensions: []*ast.Dimension{
			{
				Expr: &ast.Call{
					Name: "time",
					Args: []ast.Expr{
						&ast.DurationLiteral{Val: 10 * time.Minute},
					},
				},
			},
		},
	}

	Walk(v, n, func(node ast.Node) {
		v.n = v.n - 1
	})
}

func Walk(v ast.Visitor, node ast.Node, leaveFn func(ast.Node)) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		leaveFn(node)
		return
	}

	switch n := node.(type) {
	case *ast.BinaryExpr:
		Walk(v, n.LHS, leaveFn)
		Walk(v, n.RHS, leaveFn)

	case *ast.Call:
		for _, expr := range n.Args {
			Walk(v, expr, leaveFn)
		}

	case *ast.Dimension:
		Walk(v, n.Expr, leaveFn)

	case ast.Dimensions:
		for _, c := range n {
			Walk(v, c, leaveFn)
		}

	case *ast.Field:
		Walk(v, n.Expr, leaveFn)

	case ast.Fields:
		for _, c := range n {
			Walk(v, c, leaveFn)
		}

	case *ast.ParenExpr:
		Walk(v, n.Expr, leaveFn)

	case *ast.Query:
		Walk(v, n.Statements, leaveFn)

	case *ast.SelectStatement:
		Walk(v, n.Fields, leaveFn)
		Walk(v, n.Target, leaveFn)
		Walk(v, n.Dimensions, leaveFn)
		Walk(v, n.Sources, leaveFn)
		Walk(v, n.Condition, leaveFn)
		Walk(v, n.SortFields, leaveFn)

	case ast.SortFields:
		for _, sf := range n {
			Walk(v, sf, leaveFn)
		}

	case ast.Sources:
		for _, s := range n {
			Walk(v, s, leaveFn)
		}

	case *ast.SubQuery:
		Walk(v, n.Statement, leaveFn)

	case ast.Statements:
		for _, s := range n {
			Walk(v, s, leaveFn)
		}

	case *ast.Target:
		if n != nil {
			Walk(v, n.Metric, leaveFn)
		}
	}
	leaveFn(node)
}
