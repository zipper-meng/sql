package token

import (
	"strings"
)

// Token is the set of lexical tokens of the language.
type Token int

// The list of tokens.
const (
	// Special tokens

	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	literal_beg // Identifiers and basic type literals
	IDENT       // main
	BOUNDPARAM  // $param
	NUMBER      // 12345.67
	INTEGER     // 12345i
	DURATIONVAL // 13h
	STRING      // "abc"
	BADSTRING   // "abc
	BADESCAPE   // \q
	TRUE        // true
	FALSE       // false
	REGEX       // Regular expressions
	BADREGEX    // `.*
	literal_end

	operator_beg // Operators
	ADD          // +
	SUB          // -
	MUL          // *
	DIV          // /
	MOD          // %
	BITAND       // &
	BITOR        // |
	BITXOR       // ^

	AND // AND
	OR  // OR

	EQ       // =
	NEQ      // !=
	EQREGEX  // =~
	NEQREGEX // !~
	LT       // <
	LTE      // <=
	GT       // >
	GTE      // >=
	operator_end

	LPAREN      // (
	RPAREN      // )
	COMMA       // ,
	COLON       // :
	DOUBLECOLON // ::
	SEMICOLON   // ;
	DOT         // .

	keyword_beg // Keywords
	ALL
	ANALYZE
	ANY
	AS
	ASC
	BEGIN
	BY
	DESC
	DISTINCT
	EXPLAIN
	FIELD
	FROM
	GROUP
	IN
	INF
	INSERT
	INTO
	LIMIT
	METRIC
	OFFSET
	ORDER
	SELECT
	SLIMIT
	SOFFSET
	TAG
	WHERE
	keyword_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:       "IDENT",
	NUMBER:      "NUMBER",
	DURATIONVAL: "DURATIONVAL",
	STRING:      "STRING",
	BADSTRING:   "BADSTRING",
	BADESCAPE:   "BADESCAPE",
	TRUE:        "TRUE",
	FALSE:       "FALSE",
	REGEX:       "REGEX",

	ADD:    "+",
	SUB:    "-",
	MUL:    "*",
	DIV:    "/",
	MOD:    "%",
	BITAND: "&",
	BITOR:  "|",
	BITXOR: "^",

	AND: "AND",
	OR:  "OR",

	EQ:       "=",
	NEQ:      "!=",
	EQREGEX:  "=~",
	NEQREGEX: "!~",
	LT:       "<",
	LTE:      "<=",
	GT:       ">",
	GTE:      ">=",

	LPAREN:      "(",
	RPAREN:      ")",
	COMMA:       ",",
	COLON:       ":",
	DOUBLECOLON: "::",
	SEMICOLON:   ";",
	DOT:         ".",

	ALL:      "ALL",
	ANALYZE:  "ANALYZE",
	ANY:      "ANY",
	AS:       "AS",
	ASC:      "ASC",
	BEGIN:    "BEGIN",
	BY:       "BY",
	DESC:     "DESC",
	DISTINCT: "DISTINCT",
	EXPLAIN:  "EXPLAIN",
	FIELD:    "FIELD",
	FROM:     "FROM",
	GROUP:    "GROUP",
	IN:       "IN",
	INF:      "INF",
	INSERT:   "INSERT",
	INTO:     "INTO",
	LIMIT:    "LIMIT",
	METRIC:   "METRIC",
	OFFSET:   "OFFSET",
	ORDER:    "ORDER",
	SELECT:   "SELECT",
	SLIMIT:   "SLIMIT",
	SOFFSET:  "SOFFSET",
	TAG:      "TAG",
	WHERE:    "WHERE",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keyword_beg + 1; tok < keyword_end; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string corresponding to the token tok.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB, BITOR, BITXOR:
		return 4
	case MUL, DIV, MOD, BITAND:
		return 5
	}
	return 0
}

// IsOperator returns true for operator tokens.
func (tok Token) IsOperator() bool {
	return tok > operator_beg && tok < operator_end
}

// IsRegexOp returns true if the operator accepts a regex operand.
func (tok Token) IsRegexOp() bool {
	return tok == EQREGEX || tok == NEQREGEX
}

// Lookup maps an identifier to its keyword token or IDENT (if not a keyword).
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int // line number, starting at 0
	Char int // offset, starting at 0
}
