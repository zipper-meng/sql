package ast

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"sql/token"
	"sql/tools"
)

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	// expr is unexported to ensure implementations of Expr
	// can only originate in this package.
	expr()
}

func (*BinaryExpr) expr() {}
func (*Call) expr()       {}
func (*Distinct) expr()   {}
func (*ParenExpr) expr()  {}
func (*VarRef) expr()     {}
func (*Wildcard) expr()   {}

func (*BooleanLiteral) expr()  {}
func (*BoundParameter) expr()  {}
func (*DurationLiteral) expr() {}
func (*IntegerLiteral) expr()  {}
func (*UnsignedLiteral) expr() {}
func (*NilLiteral) expr()      {}
func (*NumberLiteral) expr()   {}
func (*RegexLiteral) expr()    {}
func (*ListLiteral) expr()     {}
func (*StringLiteral) expr()   {}
func (*TimeLiteral) expr()     {}

// ExprNames returns a list of non-"time" field names from an expression.
func ExprNames(expr Expr) []VarRef {
	m := make(map[VarRef]struct{})
	for _, ref := range walkRefs(expr) {
		if ref.Val == "time" {
			continue
		}
		m[ref] = struct{}{}
	}

	a := make([]VarRef, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Sort(VarRefs(a))

	return a
}

// walkNames will walk the Expr and return the identifier names used.
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		var a []string
		for _, expr := range expr.Args {
			if ref, ok := expr.(*VarRef); ok {
				a = append(a, ref.Val)
			}
		}
		return a
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// walkRefs will walk the Expr and return the var refs used.
func walkRefs(exp Expr) []VarRef {
	refs := make(map[VarRef]struct{})
	var walk func(exp Expr)
	walk = func(exp Expr) {
		switch expr := exp.(type) {
		case *VarRef:
			refs[*expr] = struct{}{}
		case *Call:
			for _, expr := range expr.Args {
				if ref, ok := expr.(*VarRef); ok {
					refs[*ref] = struct{}{}
				}
			}
		case *BinaryExpr:
			walk(expr.LHS)
			walk(expr.RHS)
		case *ParenExpr:
			walk(expr.Expr)
		}
	}
	walk(exp)

	// Turn the map into a slice.
	a := make([]VarRef, 0, len(refs))
	for ref := range refs {
		a = append(a, ref)
	}
	return a
}

type containsVarRefVisitor struct {
	contains bool
}

func (v *containsVarRefVisitor) Visit(n Node) Visitor {
	switch n.(type) {
	case *Call:
		return nil
	case *VarRef:
		v.contains = true
	}
	return v
}

// ContainsVarRef returns true if expr is a VarRef or contains one.
func ContainsVarRef(expr Expr) bool {
	var v containsVarRefVisitor
	Walk(&v, expr)
	return v.contains
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  token.Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
}

// Name returns the name of a binary expression by concatenating
// the variables in the binary expression with underscores.
func (e *BinaryExpr) Name() string {
	v := binaryExprNameVisitor{}
	Walk(&v, e)
	return strings.Join(v.names, "_")
}

type binaryExprNameVisitor struct {
	names []string
}

func (v *binaryExprNameVisitor) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *VarRef:
		v.names = append(v.names, n.Val)
	case *Call:
		v.names = append(v.names, n.Name)
		return nil
	}
	return v
}

// Call represents a function call.
type Call struct {
	Name string
	Args []Expr
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
}

// Distinct represents a DISTINCT expression.
type Distinct struct {
	// Identifier following DISTINCT
	Val string
}

// String returns a string representation of the expression.
func (d *Distinct) String() string {
	return fmt.Sprintf("DISTINCT %s", d.Val)
}

// NewCall returns a new call expression from this expressions.
func (d *Distinct) NewCall() *Call {
	return &Call{
		Name: "distinct",
		Args: []Expr{
			&VarRef{Val: d.Val},
		},
	}
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// VarRef represents a reference to a variable.
type VarRef struct {
	Val  string
	Type DataType
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string {
	buf := bytes.NewBufferString(tools.QuoteIdent(r.Val))
	if r.Type != Unknown {
		buf.WriteString("::")
		buf.WriteString(r.Type.String())
	}
	return buf.String()
}

// VarRefs represents a slice of VarRef types.
type VarRefs []VarRef

// Len implements sort.Interface.
func (a VarRefs) Len() int { return len(a) }

// Less implements sort.Interface.
func (a VarRefs) Less(i, j int) bool {
	if a[i].Val != a[j].Val {
		return a[i].Val < a[j].Val
	}
	return a[i].Type < a[j].Type
}

// Swap implements sort.Interface.
func (a VarRefs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Strings returns a slice of the variable names.
func (a VarRefs) Strings() []string {
	s := make([]string, len(a))
	for i, ref := range a {
		s[i] = ref.Val
	}
	return s
}

// Wildcard represents a wild card expression.
type Wildcard struct {
	Type token.Token
}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string {
	switch e.Type {
	case token.FIELD:
		return "*::field"
	case token.TAG:
		return "*::tag"
	default:
		return "*"
	}
}
