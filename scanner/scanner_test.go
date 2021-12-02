package scanner_test

import (
	"reflect"
	"sql/scanner"
	"sql/token"
	"strings"
	"testing"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok token.Token
		lit string
		pos token.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: token.EOF},
		{s: `#`, tok: token.ILLEGAL, lit: `#`},
		{s: ` `, tok: token.WS, lit: " "},
		{s: "\t", tok: token.WS, lit: "\t"},
		{s: "\n", tok: token.WS, lit: "\n"},
		{s: "\r", tok: token.WS, lit: "\n"},
		{s: "\r\n", tok: token.WS, lit: "\n"},
		{s: "\rX", tok: token.WS, lit: "\n"},
		{s: "\n\r", tok: token.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: token.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: token.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: token.ADD},
		{s: `-`, tok: token.SUB},
		{s: `*`, tok: token.MUL},
		{s: `/`, tok: token.DIV},
		{s: `%`, tok: token.MOD},

		// Logical operators
		{s: `AND`, tok: token.AND},
		{s: `and`, tok: token.AND},
		{s: `OR`, tok: token.OR},
		{s: `or`, tok: token.OR},

		{s: `=`, tok: token.EQ},
		{s: `<>`, tok: token.NEQ},
		{s: `! `, tok: token.ILLEGAL, lit: "!"},
		{s: `<`, tok: token.LT},
		{s: `<=`, tok: token.LTE},
		{s: `>`, tok: token.GT},
		{s: `>=`, tok: token.GTE},

		// Misc tokens
		{s: `(`, tok: token.LPAREN},
		{s: `)`, tok: token.RPAREN},
		{s: `,`, tok: token.COMMA},
		{s: `;`, tok: token.SEMICOLON},
		{s: `.`, tok: token.DOT},
		{s: `=~`, tok: token.EQREGEX},
		{s: `!~`, tok: token.NEQREGEX},
		{s: `:`, tok: token.COLON},
		{s: `::`, tok: token.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: token.IDENT, lit: `foo`},
		{s: `_foo`, tok: token.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: token.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: token.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: token.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: token.BADESCAPE, lit: `\b`, pos: token.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: token.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: token.BADSTRING, lit: "", pos: token.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: token.BADSTRING, lit: `test`},
		{s: `$host`, tok: token.BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: token.BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: token.TRUE},
		{s: `false`, tok: token.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: token.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: token.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: token.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: token.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: token.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: token.BADESCAPE, lit: `\g`, pos: token.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: token.INTEGER, lit: `100`},
		{s: `100.23`, tok: token.NUMBER, lit: `100.23`},
		{s: `.23`, tok: token.NUMBER, lit: `.23`},
		//{s: `.`, tok: token.ILLEGAL, lit: `.`},
		{s: `10.3s`, tok: token.NUMBER, lit: `10.3`},

		// Durations
		{s: `10u`, tok: token.DURATIONVAL, lit: `10u`},
		{s: `10µ`, tok: token.DURATIONVAL, lit: `10µ`},
		{s: `10ms`, tok: token.DURATIONVAL, lit: `10ms`},
		{s: `1s`, tok: token.DURATIONVAL, lit: `1s`},
		{s: `10m`, tok: token.DURATIONVAL, lit: `10m`},
		{s: `10h`, tok: token.DURATIONVAL, lit: `10h`},
		{s: `10d`, tok: token.DURATIONVAL, lit: `10d`},
		{s: `10w`, tok: token.DURATIONVAL, lit: `10w`},
		{s: `10x`, tok: token.DURATIONVAL, lit: `10x`}, // non-duration unit, but scanned as a duration value

		// Keywords
		{s: `ALL`, tok: token.ALL},
		{s: `AS`, tok: token.AS},
		{s: `ASC`, tok: token.ASC},
		{s: `BEGIN`, tok: token.BEGIN},
		{s: `BY`, tok: token.BY},
		{s: `DESC`, tok: token.DESC},
		{s: `EXPLAIN`, tok: token.EXPLAIN},
		{s: `FIELD`, tok: token.FIELD},
		{s: `FROM`, tok: token.FROM},
		{s: `GROUP`, tok: token.GROUP},
		{s: `INSERT`, tok: token.INSERT},
		{s: `INTO`, tok: token.INTO},
		{s: `LIMIT`, tok: token.LIMIT},
		{s: `METRIC`, tok: token.METRIC},
		{s: `OFFSET`, tok: token.OFFSET},
		{s: `ORDER`, tok: token.ORDER},
		{s: `SELECT`, tok: token.SELECT},
		{s: `TAG`, tok: token.TAG},
		{s: `WHERE`, tok: token.WHERE},
		{s: `explain`, tok: token.EXPLAIN}, // case insensitive
		{s: `seLECT`, tok: token.SELECT},   // case insensitive
	}

	for i, tt := range tests {
		s := scanner.NewScanner(strings.NewReader(tt.s))
		pos, tok, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		pos token.Pos
		tok token.Token
		lit string
	}
	exp := []result{
		{pos: token.Pos{Line: 0, Char: 0}, tok: token.SELECT, lit: ""},
		{pos: token.Pos{Line: 0, Char: 6}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 7}, tok: token.IDENT, lit: "value"},
		{pos: token.Pos{Line: 0, Char: 12}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 13}, tok: token.FROM, lit: ""},
		{pos: token.Pos{Line: 0, Char: 17}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 18}, tok: token.IDENT, lit: "ma"},
		{pos: token.Pos{Line: 0, Char: 20}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 21}, tok: token.WHERE, lit: ""},
		{pos: token.Pos{Line: 0, Char: 26}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 27}, tok: token.IDENT, lit: "a"},
		{pos: token.Pos{Line: 0, Char: 28}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 29}, tok: token.EQ, lit: ""},
		{pos: token.Pos{Line: 0, Char: 30}, tok: token.WS, lit: " "},
		{pos: token.Pos{Line: 0, Char: 30}, tok: token.STRING, lit: "b"},
		{pos: token.Pos{Line: 0, Char: 34}, tok: token.EOF, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from ma WHERE a = 'b'`
	s := scanner.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		pos, tok, lit := s.Scan()
		act = append(act, result{pos, tok, lit})
		if tok == token.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := scanner.ScanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok token.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: token.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: token.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: token.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: token.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: token.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := scanner.NewScanner(strings.NewReader(tt.in))
		_, tok, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}

func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
