package parser

import (
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"sql/ast"
	"sql/scanner"
	"sql/token"
	"sql/tools"
)

// Parser represents an CnosQL parser.
type Parser struct {
	s      scanner.Scanner
	params map[string]Value
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: scanner.NewScanner(r)}
}

// SetParams sets the parameters that will be used for any bound parameter substitutions.
func (p *Parser) SetParams(params map[string]interface{}) {
	p.params = make(map[string]Value, len(params))
	for name, param := range params {
		p.params[name] = BindValue(param)
	}
}

// ParseQuery parses a query string and returns its AST representation.
func ParseQuery(s string) (*ast.Query, error) {
	return NewParser(strings.NewReader(s)).ParseQuery()
}

// ParseStatement parses a statement string and returns its AST representation.
func ParseStatement(s string) (ast.Statement, error) {
	return NewParser(strings.NewReader(s)).ParseStatement()
}

// ParseExpr parses an expression string and returns its AST representation.
func ParseExpr(s string) (ast.Expr, error) { return NewParser(strings.NewReader(s)).ParseExpr() }

// ParseQuery parses an CnosQL string and returns a Query AST object.
func (p *Parser) ParseQuery() (*ast.Query, error) {
	var statements ast.Statements
	semi := true

	for {
		if pos, tok, lit := p.ScanIgnoreWhitespace(); tok == token.EOF {
			return &ast.Query{Statements: statements}, nil
		} else if tok == token.SEMICOLON {
			semi = true
		} else {
			if !semi {
				return nil, newParseError(tokstr(tok, lit), []string{";"}, pos)
			}
			p.s.Unscan()
			s, err := p.ParseStatement()
			if err != nil {
				return nil, err
			}
			statements = append(statements, s)
			semi = false
		}
	}
}

// ParseStatement parses an CnosQL string and returns a Statement AST object.
func (p *Parser) ParseStatement() (ast.Statement, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()

	switch tok {
	case token.SELECT:
		return p.parseSelectStatement(targetNotRequired)
	}

	// There were no registered handlers. Return the valid tokens in the order they were added.
	return nil, newParseError(tokstr(tok, lit), []string{token.SELECT.String()}, pos)
}

// parseInt parses a string representing a base 10 integer and returns the number.
// It returns an error if the parsed number is outside the range [min, max].
func (p *Parser) parseInt(min, max int) (int, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Convert string to int.
	n, err := strconv.Atoi(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	} else if min > n || n > max {
		return 0, &ParseError{
			Message: fmt.Sprintf("invalid value %d: must be %d <= n <= %d", n, min, max),
			Pos:     pos,
		}
	}

	return n, nil
}

// parseUInt64 parses a string and returns a 64-bit unsigned integer literal.
func (p *Parser) parseUInt64() (uint64, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Convert string to unsigned 64-bit integer
	n, err := strconv.ParseUint(lit, 10, 64)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return uint64(n), nil
}

// parseDuration parses a string and returns a duration literal.
// This function assumes the DURATION token has already been consumed.
func (p *Parser) parseDuration() (time.Duration, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.DURATIONVAL && tok != token.INF {
		return 0, newParseError(tokstr(tok, lit), []string{"duration"}, pos)
	}

	if tok == token.INF {
		return 0, nil
	}

	d, err := ParseDuration(lit)
	if err != nil {
		return 0, &ParseError{Message: err.Error(), Pos: pos}
	}

	return d, nil
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.IDENT {
		return "", newParseError(tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseIdentList parses a comma delimited list of identifiers.
func (p *Parser) parseIdentList() ([]string, error) {
	// Parse first (required) identifier.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.COMMA {
			p.s.Unscan()
			return idents, nil
		}

		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}
}

// parseSegmentedIdents parses a segmented identifiers.
// e.g.,  "db"."ttl".metric  or  "db"..metric
func (p *Parser) parseSegmentedIdents() ([]string, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	idents := []string{ident}

	// Parse remaining (optional) identifiers.
	for {
		if _, tok, _ := p.scan(); tok != token.DOT {
			// No more segments so we're done.
			p.s.Unscan()
			break
		}

		if ch := p.s.Peek(); ch == '/' {
			// Next segment is a regex so we're done.
			break
		} else if ch == ':' {
			// Next segment is context-specific so let caller handle it.
			break
		} else if ch == '.' {
			// Add an empty identifier.
			idents = append(idents, "")
			continue
		}

		// Parse the next identifier.
		if ident, err = p.parseIdent(); err != nil {
			return nil, err
		}

		idents = append(idents, ident)
	}

	if len(idents) > 3 {
		msg := fmt.Sprintf("too many segments in %s", QuoteIdent(idents...))
		return nil, &ParseError{Message: msg}
	}

	return idents, nil
}

// parseString parses a string.
func (p *Parser) parseString() (string, error) {
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.STRING {
		return "", newParseError(tokstr(tok, lit), []string{"string"}, pos)
	}
	return lit, nil
}

// parseStringList parses a list of strings separated by commas.
func (p *Parser) parseStringList() ([]string, error) {
	// Parse first (required) string.
	str, err := p.parseString()
	if err != nil {
		return nil, err
	}
	strs := []string{str}

	// Parse remaining (optional) strings.
	for {
		if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.COMMA {
			p.s.Unscan()
			return strs, nil
		}

		if str, err = p.parseString(); err != nil {
			return nil, err
		}

		strs = append(strs, str)
	}
}

// parseSelectStatement parses a select string and returns a Statement AST object.
// This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement(tr targetRequirement) (*ast.SelectStatement, error) {
	stmt := &ast.SelectStatement{}
	var err error

	// Parse fields: "FIELD+".
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse target: "INTO"
	if stmt.Target, err = p.parseTarget(tr); err != nil {
		return nil, err
	}

	// Parse source: "FROM".
	if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != token.FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	if stmt.Sources, err = p.parseSources(true); err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(); err != nil {
		return nil, err
	}

	// Parse dimensions: "GROUP BY DIMENSION+".
	if stmt.Dimensions, err = p.parseDimensions(); err != nil {
		return nil, err
	}

	// Parse fill options: "fill(<option>)"
	if stmt.Fill, stmt.FillValue, err = p.parseFill(); err != nil {
		return nil, err
	}

	// Parse sort: "ORDER BY FIELD+".
	if stmt.SortFields, err = p.parseOrderBy(); err != nil {
		return nil, err
	}

	// Parse limit: "LIMIT <n>".
	if stmt.Limit, err = p.ParseOptionalTokenAndInt(token.LIMIT); err != nil {
		return nil, err
	}

	// Parse offset: "OFFSET <n>".
	if stmt.Offset, err = p.ParseOptionalTokenAndInt(token.OFFSET); err != nil {
		return nil, err
	}

	// Parse series limit: "SLIMIT <n>".
	if stmt.SLimit, err = p.ParseOptionalTokenAndInt(token.SLIMIT); err != nil {
		return nil, err
	}

	// Parse series offset: "SOFFSET <n>".
	if stmt.SOffset, err = p.ParseOptionalTokenAndInt(token.SOFFSET); err != nil {
		return nil, err
	}

	// Parse timezone: "TZ(<timezone>)".
	if stmt.Location, err = p.parseLocation(); err != nil {
		return nil, err
	}

	// Set if the query is a raw data query or one with an aggregate
	stmt.IsRawQuery = true
	ast.WalkFunc(stmt.Fields, func(n ast.Node) {
		if _, ok := n.(*ast.Call); ok {
			stmt.IsRawQuery = false
		}
	})

	return stmt, nil
}

// targetRequirement specifies whether a target clause is required.
type targetRequirement int

const (
	targetRequired targetRequirement = iota
	targetNotRequired
	targetSubquery
)

// parseTarget parses a string and returns a Target.
func (p *Parser) parseTarget(tr targetRequirement) (*ast.Target, error) {
	if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != token.INTO {
		if tr == targetRequired {
			return nil, newParseError(tokstr(tok, lit), []string{"INTO"}, pos)
		}
		p.s.Unscan()
		return nil, nil
	}

	// db, ttl, and / or metric
	idents, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	if len(idents) < 3 {
		// Check for source metric reference.
		if ch := p.s.Peek(); ch == ':' {
			if err := p.parseTokens([]token.Token{token.COLON, token.METRIC}); err != nil {
				return nil, err
			}
			// Append empty metric name.
			idents = append(idents, "")
		}
	}

	t := &ast.Target{Metric: &ast.Metric{IsTarget: true}}

	switch len(idents) {
	case 1:
		t.Metric.Name = idents[0]
	case 2:
		t.Metric.TimeToLive = idents[0]
		t.Metric.Name = idents[1]
	case 3:
		t.Metric.Database = idents[0]
		t.Metric.TimeToLive = idents[1]
		t.Metric.Name = idents[2]
	}

	return t, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (ast.Fields, error) {
	var fields ast.Fields

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if _, tok, _ := p.scan(); tok != token.COMMA {
			p.s.Unscan()
			break
		}
	}
	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*ast.Field, error) {
	f := &ast.Field{}

	// Attempt to parse a regex.
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		f.Expr = re
	} else {
		pos, _, _ := p.ScanIgnoreWhitespace()
		p.s.Unscan()
		// Parse the expression first.
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		var c validateField
		ast.Walk(&c, expr)
		if c.foundInvalid {
			return nil, fmt.Errorf("invalid operator %s in SELECT clause at line %d, char %d; operator is intended for WHERE clause", c.badToken, pos.Line+1, pos.Char+1)
		}
		f.Expr = expr
	}

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// validateField checks if the Expr is a valid field. We disallow all binary expression
// that return a boolean.
type validateField struct {
	foundInvalid bool
	badToken     token.Token
}

func (c *validateField) Visit(n ast.Node) ast.Visitor {
	e, ok := n.(*ast.BinaryExpr)
	if !ok {
		return c
	}

	switch e.Op {
	case token.EQ, token.NEQ, token.EQREGEX,
		token.NEQREGEX, token.LT, token.LTE, token.GT, token.GTE,
		token.AND, token.OR:
		c.foundInvalid = true
		c.badToken = e.Op
		return nil
	}
	return c
}

// parseAlias parses the "AS IDENT" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then Unscan and exit.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.AS {
		p.s.Unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	lit, err := p.parseIdent()
	if err != nil {
		return "", err
	}
	return lit, nil
}

// parseSources parses a comma delimited list of sources.
func (p *Parser) parseSources(subqueries bool) (ast.Sources, error) {
	var sources ast.Sources

	for {
		s, err := p.parseSource(subqueries)
		if err != nil {
			return nil, err
		}
		sources = append(sources, s)

		if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.COMMA {
			p.s.Unscan()
			break
		}
	}

	return sources, nil
}

func (p *Parser) parseSource(subqueries bool) (ast.Source, error) {
	m := &ast.Metric{}

	// Attempt to parse a regex.
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		m.Regex = re
		// Regex is always last so we're done.
		return m, nil
	}

	// If there is no regular expression, this might be a subquery.
	// Parse the subquery if we are in a query that allows them as a source.
	if m.Regex == nil && subqueries {
		if _, tok, _ := p.ScanIgnoreWhitespace(); tok == token.LPAREN {
			if err := p.parseTokens([]token.Token{token.SELECT}); err != nil {
				return nil, err
			}

			stmt, err := p.parseSelectStatement(targetSubquery)
			if err != nil {
				return nil, err
			}

			if err := p.parseTokens([]token.Token{token.RPAREN}); err != nil {
				return nil, err
			}
			return &ast.SubQuery{Statement: stmt}, nil
		} else {
			p.s.Unscan()
		}
	}

	// Didn't find a regex so parse segmented identifiers.
	idents, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	// If we already have the max allowed idents, we're done.
	if len(idents) == 3 {
		m.Database, m.TimeToLive, m.Name = idents[0], idents[1], idents[2]
		return m, nil
	}
	// Check again for regex.
	re, err = p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		m.Regex = re
	}

	// Assign identifiers to their proper locations.
	switch len(idents) {
	case 1:
		if re != nil {
			m.TimeToLive = idents[0]
		} else {
			m.Name = idents[0]
		}
	case 2:
		if re != nil {
			m.Database, m.TimeToLive = idents[0], idents[1]
		} else {
			m.TimeToLive, m.Name = idents[0], idents[1]
		}
	}

	return m, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (ast.Expr, error) {
	// Check if the WHERE token exists.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.WHERE {
		p.s.Unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseDimensions parses the "GROUP BY" clause of the query, if it exists.
func (p *Parser) parseDimensions() (ast.Dimensions, error) {
	// If the next token is not GROUP then exit.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.GROUP {
		p.s.Unscan()
		return nil, nil
	}

	// Now the next token should be "BY".
	if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != token.BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	var dimensions ast.Dimensions
	for {
		// Parse the dimension.
		d, err := p.parseDimension()
		if err != nil {
			return nil, err
		}

		// Add new dimension.
		dimensions = append(dimensions, d)

		// If there's not a comma next then stop parsing dimensions.
		if _, tok, _ := p.scan(); tok != token.COMMA {
			p.s.Unscan()
			break
		}
	}
	return dimensions, nil
}

// parseDimension parses a single dimension.
func (p *Parser) parseDimension() (*ast.Dimension, error) {
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		return &ast.Dimension{Expr: re}, nil
	}

	// Parse the expression first.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return &ast.Dimension{Expr: expr}, nil
}

// parseFill parses the fill call and its options.
func (p *Parser) parseFill() (ast.FillOption, interface{}, error) {
	// Parse the expression first.
	_, tok, lit := p.ScanIgnoreWhitespace()
	p.s.Unscan()
	if tok != token.IDENT || strings.ToLower(lit) != "fill" {
		return ast.NullFill, nil, nil
	}

	expr, err := p.ParseExpr()
	if err != nil {
		return ast.NullFill, nil, err
	}
	fill, ok := expr.(*ast.Call)
	if !ok {
		return ast.NullFill, nil, errors.New("fill must be a function call")
	} else if len(fill.Args) != 1 {
		return ast.NullFill, nil, errors.New("fill requires an argument, e.g.: 0, null, none, previous, linear")
	}
	switch fill.Args[0].String() {
	case "null":
		return ast.NullFill, nil, nil
	case "none":
		return ast.NoFill, nil, nil
	case "previous":
		return ast.PreviousFill, nil, nil
	case "linear":
		return ast.LinearFill, nil, nil
	default:
		switch num := fill.Args[0].(type) {
		case *ast.IntegerLiteral:
			return ast.NumberFill, num.Val, nil
		case *ast.NumberLiteral:
			return ast.NumberFill, num.Val, nil
		default:
			return ast.NullFill, nil, fmt.Errorf("expected number argument in fill()")
		}
	}
}

// parseLocation parses the timezone call and its arguments.
func (p *Parser) parseLocation() (*time.Location, error) {
	// Parse the expression first.
	_, tok, lit := p.ScanIgnoreWhitespace()
	p.s.Unscan()
	if tok != token.IDENT || strings.ToLower(lit) != "tz" {
		return nil, nil
	}

	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}
	tz, ok := expr.(*ast.Call)
	if !ok {
		return nil, errors.New("tz must be a function call")
	} else if len(tz.Args) != 1 {
		return nil, errors.New("tz requires exactly one argument")
	}

	tzname, ok := tz.Args[0].(*ast.StringLiteral)
	if !ok {
		return nil, errors.New("expected string argument in tz()")
	}

	loc, err := time.LoadLocation(tzname.Val)
	if err != nil {
		// Do not pass the same error message as the error may contain sensitive pathnames.
		return nil, fmt.Errorf("unable to find time zone %s", tzname.Val)
	}
	return loc, nil
}

// ParseOptionalTokenAndInt parses the specified token followed
// by an int, if it exists.
func (p *Parser) ParseOptionalTokenAndInt(t token.Token) (int, error) {
	// Check if the token exists.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok != t {
		p.s.Unscan()
		return 0, nil
	}

	// Scan the number.
	pos, tok, lit := p.ScanIgnoreWhitespace()
	if tok != token.INTEGER {
		return 0, newParseError(tokstr(tok, lit), []string{"integer"}, pos)
	}

	// Parse number.
	n, _ := strconv.ParseInt(lit, 10, 64)
	if n < 0 {
		msg := fmt.Sprintf("%s must be >= 0", t.String())
		return 0, &ParseError{Message: msg, Pos: pos}
	}

	return int(n), nil
}

// parseOrderBy parses the "ORDER BY" clause of a query, if it exists.
func (p *Parser) parseOrderBy() (ast.SortFields, error) {
	// Return nil result and nil error if no ORDER token at this position.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.ORDER {
		p.s.Unscan()
		return nil, nil
	}

	// Parse the required BY token.
	if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != token.BY {
		return nil, newParseError(tokstr(tok, lit), []string{"BY"}, pos)
	}

	// Parse the ORDER BY fields.
	fields, err := p.parseSortFields()
	if err != nil {
		return nil, err
	}

	return fields, nil
}

// parseSortFields parses the sort fields for an ORDER BY clause.
func (p *Parser) parseSortFields() (ast.SortFields, error) {
	var fields ast.SortFields

	pos, tok, lit := p.ScanIgnoreWhitespace()

	switch tok {
	// The first field after an order by may not have a field name (e.g. ORDER BY ASC)
	case token.ASC, token.DESC:
		fields = append(fields, &ast.SortField{Ascending: (tok == token.ASC)})
	// If it's a token, parse it as a sort field.  At least one is required.
	case token.IDENT:
		p.s.Unscan()
		field, err := p.parseSortField()
		if err != nil {
			return nil, err
		}

		if lit != "time" {
			return nil, errors.New("only ORDER BY time supported at this time")
		}

		fields = append(fields, field)
	// Parse error...
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "ASC", "DESC"}, pos)
	}

	// Parse additional fields.
	for {
		_, tok, _ := p.ScanIgnoreWhitespace()

		if tok != token.COMMA {
			p.s.Unscan()
			break
		}

		field, err := p.parseSortField()
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)
	}

	if len(fields) > 1 {
		return nil, errors.New("only ORDER BY time supported at this time")
	}

	return fields, nil
}

// parseSortField parses one field of an ORDER BY clause.
func (p *Parser) parseSortField() (*ast.SortField, error) {
	field := &ast.SortField{}

	// Parse sort field name.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	field.Name = ident

	// Check for optional ASC or DESC clause. Default is ASC.
	_, tok, _ := p.ScanIgnoreWhitespace()
	if tok != token.ASC && tok != token.DESC {
		p.s.Unscan()
		tok = token.ASC
	}
	field.Ascending = (tok == token.ASC)

	return field, nil
}

// ParseVarRef parses a reference to a metric or field.
func (p *Parser) ParseVarRef() (*ast.VarRef, error) {
	// Parse the segments of the variable ref.
	segments, err := p.parseSegmentedIdents()
	if err != nil {
		return nil, err
	}

	var dtype ast.DataType
	if _, tok, _ := p.scan(); tok == token.DOUBLECOLON {
		pos, tok, lit := p.scan()
		switch tok {
		case token.IDENT:
			switch strings.ToLower(lit) {
			case "float":
				dtype = ast.Float
			case "integer":
				dtype = ast.Integer
			case "unsigned":
				dtype = ast.Unsigned
			case "string":
				dtype = ast.String
			case "boolean":
				dtype = ast.Boolean
			default:
				return nil, newParseError(tokstr(tok, lit), []string{"float", "integer", "unsigned", "string", "boolean", "field", "tag"}, pos)
			}
		case token.FIELD:
			dtype = ast.AnyField
		case token.TAG:
			dtype = ast.Tag
		default:
			return nil, newParseError(tokstr(tok, lit), []string{"float", "integer", "string", "boolean", "field", "tag"}, pos)
		}
	} else {
		p.s.Unscan()
	}

	vr := &ast.VarRef{Val: strings.Join(segments, "."), Type: dtype}

	return vr, nil
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (ast.Expr, error) {
	var err error
	// Dummy root node.
	root := &ast.BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		_, op, _ := p.ScanIgnoreWhitespace()
		if !op.IsOperator() {
			p.s.Unscan()
			return root.RHS, nil
		}

		// Otherwise parse the next expression.
		var rhs ast.Expr
		if op.IsRegexOp() {
			// RHS of a regex operator must be a regular expression.
			if rhs, err = p.parseRegex(); err != nil {
				return nil, err
			}
			// parseRegex can return an empty type, but we need it to be present
			if rhs.(*ast.RegexLiteral) == nil {
				pos, tok, lit := p.ScanIgnoreWhitespace()
				return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
			}
		} else {
			if rhs, err = p.parseUnaryExpr(); err != nil {
				return nil, err
			}
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*ast.BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &ast.BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op}
				break
			}
			node = r
		}
	}
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (ast.Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped expression.
	if _, tok, _ := p.ScanIgnoreWhitespace(); tok == token.LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		// Expect an RPAREN at the end.
		if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != token.RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ast.ParenExpr{Expr: expr}, nil
	}
	p.s.Unscan()

	// Read next token.
	pos, tok, lit := p.ScanIgnoreWhitespace()
	switch tok {
	case token.IDENT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a variable reference.
		if _, tok0, _ := p.scan(); tok0 == token.LPAREN {
			return p.parseCall(lit)
		}

		p.s.Unscan() // Unscan the last token (wasn't an LPAREN)
		p.s.Unscan() // Unscan the IDENT token

		// Parse it as a VarRef.
		return p.ParseVarRef()
	case token.DISTINCT:
		// If the next immediate token is a left parentheses, parse as function call.
		// Otherwise parse as a Distinct expression.
		pos, tok0, lit := p.scan()
		if tok0 == token.LPAREN {
			return p.parseCall("distinct")
		} else if tok0 == token.WS {
			pos, tok1, lit := p.ScanIgnoreWhitespace()
			if tok1 != token.IDENT {
				return nil, newParseError(tokstr(tok1, lit), []string{"identifier"}, pos)
			}
			return &ast.Distinct{Val: lit}, nil
		}

		return nil, newParseError(tokstr(tok0, lit), []string{"(", "identifier"}, pos)
	case token.STRING:
		return &ast.StringLiteral{Val: lit}, nil
	case token.NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &ast.NumberLiteral{Val: v}, nil
	case token.INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is, use an unsigned integer.
			// The check for negative numbers is handled somewhere else so this should always be a positive number.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return &ast.UnsignedLiteral{Val: v}, nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		return &ast.IntegerLiteral{Val: v}, nil
	case token.TRUE, token.FALSE:
		return &ast.BooleanLiteral{Val: tok == token.TRUE}, nil
	case token.DURATIONVAL:
		v, err := ParseDuration(lit)
		if err != nil {
			return nil, err
		}
		return &ast.DurationLiteral{Val: v}, nil
	case token.MUL:
		wc := &ast.Wildcard{}
		if _, tok, _ := p.scan(); tok == token.DOUBLECOLON {
			pos, tok, lit := p.scan()
			switch tok {
			case token.FIELD, token.TAG:
				wc.Type = tok
			default:
				return nil, newParseError(tokstr(tok, lit), []string{"field", "tag"}, pos)
			}
		} else {
			p.s.Unscan()
		}
		return wc, nil
	case token.REGEX:
		re, err := regexp.Compile(lit)
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return &ast.RegexLiteral{Val: re}, nil
	case token.BOUNDPARAM:
		// If we have a BOUNDPARAM in the token stream,
		// it wasn't resolved by the parser to another
		// token type which means it is invalid.
		// Figure out what is wrong with it.
		k := strings.TrimPrefix(lit, "$")
		if len(k) == 0 {
			return nil, errors.New("empty bound parameter")
		}

		v, ok := p.params[k]
		if !ok {
			return nil, fmt.Errorf("missing parameter: %s", k)
		}

		// The value must be an ErrorValue.
		// Return the value as an error. A non-error value
		// would have been substituted as something else.
		return nil, errors.New(v.Value())
	case token.ADD, token.SUB:
		mul := 1
		if tok == token.SUB {
			mul = -1
		}

		pos0, tok0, lit0 := p.ScanIgnoreWhitespace()
		switch tok0 {
		case token.NUMBER, token.INTEGER, token.DURATIONVAL, token.LPAREN, token.IDENT:
			// Unscan the token and use parseUnaryExpr.
			p.s.Unscan()

			lit, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}

			switch lit := lit.(type) {
			case *ast.NumberLiteral:
				lit.Val *= float64(mul)
			case *ast.IntegerLiteral:
				lit.Val *= int64(mul)
			case *ast.UnsignedLiteral:
				if tok == token.SUB {
					// Because of twos-complement integers and the method we parse, math.MinInt64 will be parsed
					// as an UnsignedLiteral because it overflows an int64, but it fits into int64 if it were parsed
					// as a negative number instead.
					if lit.Val == uint64(math.MaxInt64+1) {
						return &ast.IntegerLiteral{Val: int64(-lit.Val)}, nil
					}
					return nil, fmt.Errorf("constant -%d underflows int64", lit.Val)
				}
			case *ast.DurationLiteral:
				lit.Val *= time.Duration(mul)
			case *ast.VarRef, *ast.Call, *ast.ParenExpr:
				// Multiply the variable.
				return &ast.BinaryExpr{
					Op:  token.MUL,
					LHS: &ast.IntegerLiteral{Val: int64(mul)},
					RHS: lit,
				}, nil
			default:
				panic(fmt.Sprintf("unexpected literal: %T", lit))
			}
			return lit, nil
		default:
			return nil, newParseError(tokstr(tok0, lit0), []string{"identifier", "number", "duration", "("}, pos0)
		}
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseRegex parses a regular expression.
func (p *Parser) parseRegex() (*ast.RegexLiteral, error) {
	nextRune := p.s.Peek()
	if tools.IsWhitespace(nextRune) {
		p.consumeWhitespace()
	}

	// If the next character is not a '/', then return nils.
	nextRune = p.s.Peek()
	if nextRune == '$' {
		// This might be a bound parameter and it might
		// resolve to a regex.
		_, tok, _ := p.scan()
		p.s.Unscan()
		if tok != token.REGEX {
			// It was not a regular expression so return.
			return nil, nil
		}
	} else if nextRune != '/' {
		return nil, nil
	}

	pos, tok, lit := p.s.ScanRegex()

	if tok == token.BADESCAPE {
		msg := fmt.Sprintf("bad escape: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok == token.BADREGEX {
		msg := fmt.Sprintf("bad regex: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok != token.REGEX {
		return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
	}

	re, err := regexp.Compile(lit)
	if err != nil {
		return nil, &ParseError{Message: err.Error(), Pos: pos}
	}

	return &ast.RegexLiteral{Val: re}, nil
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*ast.Call, error) {
	name = strings.ToLower(name)

	// Parse first function argument if one exists.
	var args []ast.Expr
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		args = append(args, re)
	} else {
		// If there's a right paren then just return immediately.
		if _, tok, _ := p.scan(); tok == token.RPAREN {
			return &ast.Call{Name: name}, nil
		}
		p.s.Unscan()

		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// Parse additional function arguments if there is a comma.
	for {
		// If there's not a comma, stop parsing arguments.
		if _, tok, _ := p.ScanIgnoreWhitespace(); tok != token.COMMA {
			p.s.Unscan()
			break
		}

		re, err := p.parseRegex()
		if err != nil {
			return nil, err
		} else if re != nil {
			args = append(args, re)
			continue
		}

		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// There should be a right parentheses at the end.
	if pos, tok, lit := p.scan(); tok != token.RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &ast.Call{Name: name, Args: args}, nil
}

func (p *Parser) scan() (pos token.Pos, tok token.Token, lit string) {
	pos, tok, lit = p.s.Scan()
	if tok == token.BOUNDPARAM {
		k := strings.TrimPrefix(lit, "$")
		if len(k) != 0 {
			if v, ok := p.params[k]; ok {
				tok, lit = v.TokenType(), v.Value()
			}
		}
	}
	return pos, tok, lit
}

func (p *Parser) scanRegex() (pos token.Pos, tok token.Token, lit string) {
	pos, tok, lit = p.s.ScanRegex()
	if tok == token.BOUNDPARAM {
		k := strings.TrimPrefix(lit, "$")
		if len(k) != 0 {
			if v, ok := p.params[k]; ok {
				tok, lit = v.TokenType(), v.Value()
			}
		}
	}
	return pos, tok, lit
}

// ScanIgnoreWhitespace scans the next non-whitespace and non-comment token.
func (p *Parser) ScanIgnoreWhitespace() (pos token.Pos, tok token.Token, lit string) {
	for {
		pos, tok, lit = p.scan()
		if tok == token.WS || tok == token.COMMENT {
			continue
		}
		return
	}
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if _, tok, _ := p.scan(); tok != token.WS {
		p.s.Unscan()
	}
}

// ParseDuration parses a time duration from a string.
// This is needed instead of time.ParseDuration because this will support
// the full syntax that CnosQL supports for specifying durations
// including weeks and days.
func ParseDuration(s string) (time.Duration, error) {
	// Return an error if the string is blank or one character
	if len(s) < 2 {
		return 0, ErrInvalidDuration
	}

	// Split string into individual runes.
	a := []rune(s)

	// Start with a zero duration.
	var d time.Duration
	i := 0

	// Check for a negative.
	isNegative := false
	if a[i] == '-' {
		isNegative = true
		i++
	}

	var measure int64
	var unit string

	// Parsing loop.
	for i < len(a) {
		// Find the number portion.
		start := i
		for ; i < len(a) && tools.IsDigit(a[i]); i++ {
			// Scan for the digits.
		}

		// Check if we reached the end of the string prematurely.
		if i >= len(a) || i == start {
			return 0, ErrInvalidDuration
		}

		// Parse the numeric part.
		n, err := strconv.ParseInt(string(a[start:i]), 10, 64)
		if err != nil {
			return 0, ErrInvalidDuration
		}
		measure = n

		// Extract the unit of measure.
		// If the last two characters are "ms" then parse as milliseconds.
		// Otherwise just use the last character as the unit of measure.
		unit = string(a[i])
		switch a[i] {
		case 'n':
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				d += time.Duration(n)
				i += 2
				continue
			}
			return 0, ErrInvalidDuration
		case 'u', 'µ':
			d += time.Duration(n) * time.Microsecond
		case 'm':
			if i+1 < len(a) && a[i+1] == 's' {
				unit = string(a[i : i+2])
				d += time.Duration(n) * time.Millisecond
				i += 2
				continue
			}
			d += time.Duration(n) * time.Minute
		case 's':
			d += time.Duration(n) * time.Second
		case 'h':
			d += time.Duration(n) * time.Hour
		case 'd':
			d += time.Duration(n) * 24 * time.Hour
		case 'w':
			d += time.Duration(n) * 7 * 24 * time.Hour
		default:
			return 0, ErrInvalidDuration
		}
		i++
	}

	// Check to see if we overflowed a duration
	if d < 0 && !isNegative {
		return 0, fmt.Errorf("overflowed duration %d%s: choose a smaller duration or INF", measure, unit)
	}

	if isNegative {
		d = -d
	}
	return d, nil
}

// FormatDuration formats a duration to a string.
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	} else if d%(7*24*time.Hour) == 0 {
		return fmt.Sprintf("%dw", d/(7*24*time.Hour))
	} else if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	} else if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	} else if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	} else if d%time.Second == 0 {
		return fmt.Sprintf("%ds", d/time.Second)
	} else if d%time.Millisecond == 0 {
		return fmt.Sprintf("%dms", d/time.Millisecond)
	} else if d%time.Microsecond == 0 {
		// Although we accept both "u" and "µ" when reading microsecond durations,
		// we output with "u", which can be represented in 1 byte,
		// instead of "µ", which requires 2 bytes.
		return fmt.Sprintf("%du", d/time.Microsecond)
	}
	return fmt.Sprintf("%dns", d)
}

// parseTokens consumes an expected sequence of tokens.
func (p *Parser) parseTokens(toks []token.Token) error {
	for _, expected := range toks {
		if pos, tok, lit := p.ScanIgnoreWhitespace(); tok != expected {
			return newParseError(tokstr(tok, lit), []string{expected.String()}, pos)
		}
	}
	return nil
}

var (
	// Quote String replacer.
	qsReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`)

	// Quote Ident replacer.
	qiReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

// QuoteString returns a quoted string.
func QuoteString(s string) string {
	return `'` + qsReplacer.Replace(s) + `'`
}

// QuoteIdent returns a quoted identifier from multiple bare identifiers.
func QuoteIdent(segments ...string) string {
	var buf strings.Builder
	for i, segment := range segments {
		needQuote := IdentNeedsQuotes(segment) ||
			((i < len(segments)-1) && segment != "") || // not last segment && not ""
			((i == 0 || i == len(segments)-1) && segment == "") // the first or last segment and an empty string

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(qiReplacer.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// IdentNeedsQuotes returns true if the ident string given would require quotes.
func IdentNeedsQuotes(ident string) bool {
	// check if this identifier is a keyword
	tok := token.Lookup(ident)
	if tok != token.IDENT {
		return true
	}
	for i, r := range ident {
		if i == 0 && !tools.IsIdentFirstChar(r) {
			return true
		} else if i > 0 && !tools.IsIdentChar(r) {
			return true
		}
	}
	return false
}

// isDateString returns true if the string looks like a date-only time literal.
func isDateString(s string) bool { return dateStringRegexp.MatchString(s) }

// isDateTimeString returns true if the string looks like a date+time time literal.
func isDateTimeString(s string) bool { return dateTimeStringRegexp.MatchString(s) }

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok token.Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

var dateStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}.+`)

// ErrInvalidDuration is returned when parsing a malformed duration.
var ErrInvalidDuration = errors.New("invalid duration")

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      token.Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos token.Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d", e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
