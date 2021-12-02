package ast

// Node represents a node in the CnosDB abstract syntax tree.
type Node interface {
	// node is unexported to ensure implementations of Node
	// can only originate in this package.
	node()
	String() string
}

func (*Query) node()     {}
func (Statements) node() {}

func (*SelectStatement) node() {}

func (*Metric) node()   {}
func (*SubQuery) node() {}
func (Sources) node()   {}
func (Metrics) node()   {}

func (*Target) node()    {}
func (*Field) node()     {}
func (Fields) node()     {}
func (*SortField) node() {}
func (SortFields) node() {}
func (*Dimension) node() {}
func (Dimensions) node() {}

func (*BooleanLiteral) node()  {}
func (*BoundParameter) node()  {}
func (*DurationLiteral) node() {}
func (*IntegerLiteral) node()  {}
func (*UnsignedLiteral) node() {}
func (*NilLiteral) node()      {}
func (*NumberLiteral) node()   {}
func (*RegexLiteral) node()    {}
func (*ListLiteral) node()     {}
func (*StringLiteral) node()   {}
func (*TimeLiteral) node()     {}

func (*BinaryExpr) node() {}
func (*Call) node()       {}
func (*Distinct) node()   {}
func (*ParenExpr) node()  {}
func (*VarRef) node()     {}
func (*Wildcard) node()   {}
