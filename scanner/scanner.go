package scanner

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"sql/token"
	"sql/tools"
)

type Scanner interface {
	// Scan reads the next token from the scanner.
	Scan() (pos token.Pos, tok token.Token, lit string)
	// ScanRegex reads a regex token from the scanner.
	ScanRegex() (pos token.Pos, tok token.Token, lit string)
	// Peek returns the next rune that would be read by the scanner.
	Peek() rune
	// Unscan pushes the previously token back onto the buffer.
	Unscan()
}

// bufScanner represents a wrapper for scanner to add a buffer.
// It provides a fixed-length circular buffer that can be unread.
type bufScanner struct {
	s   *scanner
	i   int // buffer index
	n   int // buffer size
	buf [3]struct {
		tok token.Token
		pos token.Pos
		lit string
	}
}

// NewScanner returns a new buffered scanner for a reader.
func NewScanner(r io.Reader) Scanner {
	return &bufScanner{s: newScanner(r)}
}

// Scan reads the next token from the scanner.
func (s *bufScanner) Scan() (pos token.Pos, tok token.Token, lit string) {
	return s.ScanFunc(s.s.Scan)
}

// ScanRegex reads a regex token from the scanner.
func (s *bufScanner) ScanRegex() (pos token.Pos, tok token.Token, lit string) {
	return s.ScanFunc(s.s.ScanRegex)
}

// ScanFunc uses the provided function to scan the next token.
func (s *bufScanner) ScanFunc(scan func() (token.Pos, token.Token, string)) (pos token.Pos, tok token.Token, lit string) {
	// If we have unread tokens then read them off the buffer first.
	if s.n > 0 {
		s.n--
		return s.curr()
	}

	// Move buffer position forward and save the token.
	s.i = (s.i + 1) % len(s.buf)
	buf := &s.buf[s.i]
	buf.pos, buf.tok, buf.lit = scan()

	return s.curr()
}

func (s *bufScanner) Peek() rune {
	r, _, _ := s.s.r.ReadRune()
	if r != EOF {
		_ = s.s.r.UnreadRune()
	}

	return r
}

// Unscan pushes the previously token back onto the buffer.
func (s *bufScanner) Unscan() { s.n++ }

// curr returns the last read token.
func (s *bufScanner) curr() (pos token.Pos, tok token.Token, lit string) {
	buf := &s.buf[(s.i-s.n+len(s.buf))%len(s.buf)]
	return buf.pos, buf.tok, buf.lit
}

// scanner represents a lexical scanner for CnosQL.
type scanner struct {
	r *reader
}

// newScanner returns a new instance of scanner.
func newScanner(r io.Reader) *scanner {
	return &scanner{r: &reader{r: bufio.NewReader(r)}}
}

// Scan returns the next token and position from the underlying reader.
// Also returns the literal text read for strings, numbers, and duration tokens
// since these token types can have different literal representations.
func (s *scanner) Scan() (pos token.Pos, tok token.Token, lit string) {
	// Read next code point.
	ch0, pos := s.r.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter, or certain acceptable special characters, then consume
	// as an ident or reserved word.
	if tools.IsWhitespace(ch0) {
		return s.scanWhitespace()
	} else if tools.IsLetter(ch0) || ch0 == '_' {
		s.r.unread()
		return s.scanIdent(true)
	} else if tools.IsDigit(ch0) {
		return s.scanNumber()
	}

	// Otherwise, parse individual characters.
	switch ch0 {
	case EOF:
		return pos, token.EOF, ""
	case '"':
		s.r.unread()
		return s.scanIdent(true)
	case '\'':
		return s.scanString()
	case '.':
		ch1, _ := s.r.read()
		s.r.unread()
		if tools.IsDigit(ch1) {
			return s.scanNumber()
		}
		return pos, token.DOT, ""
	case '$':
		_, tok, lit = s.scanIdent(false)
		if tok != token.IDENT {
			return pos, tok, "$" + lit
		}
		return pos, token.BOUNDPARAM, "$" + lit
	case '+':
		return pos, token.ADD, ""
	case '-':
		ch1, _ := s.r.read()
		if ch1 == '-' {
			s.skipUntilNewline()
			return pos, token.COMMENT, ""
		}
		s.r.unread()
		return pos, token.SUB, ""
	case '*':
		return pos, token.MUL, ""
	case '/':
		ch1, _ := s.r.read()
		if ch1 == '*' {
			if err := s.skipUntilEndComment(); err != nil {
				return pos, token.ILLEGAL, ""
			}
			return pos, token.COMMENT, ""
		} else {
			s.r.unread()
		}
		return pos, token.DIV, ""
	case '%':
		return pos, token.MOD, ""
	case '&':
		return pos, token.BITAND, ""
	case '|':
		return pos, token.BITOR, ""
	case '^':
		return pos, token.BITXOR, ""
	case '=':
		if ch1, _ := s.r.read(); ch1 == '~' {
			return pos, token.EQREGEX, ""
		}
		s.r.unread()
		return pos, token.EQ, ""
	case '!':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return pos, token.NEQ, ""
		} else if ch1 == '~' {
			return pos, token.NEQREGEX, ""
		}
		s.r.unread()
	case '>':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return pos, token.GTE, ""
		}
		s.r.unread()
		return pos, token.GT, ""
	case '<':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return pos, token.LTE, ""
		} else if ch1 == '>' {
			return pos, token.NEQ, ""
		}
		s.r.unread()
		return pos, token.LT, ""
	case '(':
		return pos, token.LPAREN, ""
	case ')':
		return pos, token.RPAREN, ""
	case ',':
		return pos, token.COMMA, ""
	case ';':
		return pos, token.SEMICOLON, ""
	case ':':
		if ch1, _ := s.r.read(); ch1 == ':' {
			return pos, token.DOUBLECOLON, ""
		}
		s.r.unread()
		return pos, token.COLON, ""
	}

	return pos, token.ILLEGAL, string(ch0)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *scanner) scanWhitespace() (pos token.Pos, tok token.Token, lit string) {
	// Create a buffer and read the current character into it.
	var buf strings.Builder
	ch, pos := s.r.curr()
	_, _ = buf.WriteRune(ch)

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		ch, _ = s.r.read()
		if ch == EOF {
			break
		} else if !tools.IsWhitespace(ch) {
			s.r.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return pos, token.WS, buf.String()
}

// skipUntilNewline skips characters until it reaches a newline.
func (s *scanner) skipUntilNewline() {
	for {
		if ch, _ := s.r.read(); ch == '\n' || ch == EOF {
			return
		}
	}
}

// skipUntilEndComment skips characters until it reaches a '*/' symbol.
func (s *scanner) skipUntilEndComment() error {
	for {
		if ch1, _ := s.r.read(); ch1 == '*' {
			// We might be at the end.
		star:
			ch2, _ := s.r.read()
			if ch2 == '/' {
				return nil
			} else if ch2 == '*' {
				// We are back in the state machine since we see a star.
				goto star
			} else if ch2 == EOF {
				return io.EOF
			}
		} else if ch1 == EOF {
			return io.EOF
		}
	}
}

func (s *scanner) scanIdent(lookup bool) (pos token.Pos, tok token.Token, lit string) {
	// Save the starting position of the identifier.
	_, pos = s.r.read()
	s.r.unread()

	var buf strings.Builder
	for {
		if ch, _ := s.r.read(); ch == EOF {
			break
		} else if ch == '"' {
			pos0, tok0, lit0 := s.scanString()
			if tok0 == token.BADSTRING || tok0 == token.BADESCAPE {
				return pos0, tok0, lit0
			}
			return pos, token.IDENT, lit0
		} else if tools.IsIdentChar(ch) {
			s.r.unread()
			buf.WriteString(ScanBareIdent(s.r))
		} else {
			s.r.unread()
			break
		}
	}
	lit = buf.String()

	// If the literal matches a keyword then return that keyword.
	if lookup {
		if tok = token.Lookup(lit); tok != token.IDENT {
			return pos, tok, ""
		}
	}
	return pos, token.IDENT, lit
}

// scanString consumes a contiguous string of non-quote characters.
// Quote characters can be consumed if they're first escaped with a backslash.
func (s *scanner) scanString() (pos token.Pos, tok token.Token, lit string) {
	s.r.unread()
	_, pos = s.r.curr()

	var err error
	lit, err = ScanString(s.r)
	if err == errBadString {
		return pos, token.BADSTRING, lit
	} else if err == errBadEscape {
		_, pos = s.r.curr()
		return pos, token.BADESCAPE, lit
	}
	return pos, token.STRING, lit
}

// ScanRegex consumes a token to find escapes
func (s *scanner) ScanRegex() (pos token.Pos, tok token.Token, lit string) {
	_, pos = s.r.curr()

	// Start & end sentinels.
	start, end := '/', '/'
	// Valid escape chars.
	escapes := map[rune]rune{'/': '/'}

	b, err := ScanDelimited(s.r, start, end, escapes, true)

	if err == errBadEscape {
		_, pos = s.r.curr()
		return pos, token.BADESCAPE, lit
	} else if err != nil {
		return pos, token.BADREGEX, lit
	}
	return pos, token.REGEX, string(b)
}

// scanNumber consumes anything that looks like the start of a number.
func (s *scanner) scanNumber() (pos token.Pos, tok token.Token, lit string) {
	var buf strings.Builder

	// Check if the initial rune is a ".".
	ch, pos := s.r.curr()
	if ch == '.' {
		// Peek and see if the next rune is a digit.
		ch1, _ := s.r.read()
		s.r.unread()
		if !tools.IsDigit(ch1) {
			return pos, token.ILLEGAL, "."
		}

		// Unread the full stop so we can read it later.
		s.r.unread()
	} else {
		s.r.unread()
	}

	// Read as many digits as possible.
	_, _ = buf.WriteString(s.scanDigits())

	// If next code points are a full stop and digit then consume them.
	isDecimal := false
	if ch0, _ := s.r.read(); ch0 == '.' {
		isDecimal = true
		if ch1, _ := s.r.read(); tools.IsDigit(ch1) {
			_, _ = buf.WriteRune(ch0)
			_, _ = buf.WriteRune(ch1)
			_, _ = buf.WriteString(s.scanDigits())
		} else {
			s.r.unread()
		}
	} else {
		s.r.unread()
	}

	// Read as a duration or integer if it doesn't have a fractional part.
	if !isDecimal {
		// If the next rune is a letter then this is a duration token.
		if ch0, _ := s.r.read(); tools.IsLetter(ch0) || ch0 == 'µ' {
			_, _ = buf.WriteRune(ch0)
			for {
				ch1, _ := s.r.read()
				if !tools.IsLetter(ch1) && ch1 != 'µ' {
					s.r.unread()
					break
				}
				_, _ = buf.WriteRune(ch1)
			}

			// Continue reading digits and letters as part of this token.
			for {
				if ch0, _ := s.r.read(); tools.IsLetter(ch0) || ch0 == 'µ' || tools.IsDigit(ch0) {
					_, _ = buf.WriteRune(ch0)
				} else {
					s.r.unread()
					break
				}
			}
			return pos, token.DURATIONVAL, buf.String()
		} else {
			s.r.unread()
			return pos, token.INTEGER, buf.String()
		}
	}
	return pos, token.NUMBER, buf.String()
}

// scanDigits consumes a contiguous series of digits.
func (s *scanner) scanDigits() string {
	var buf strings.Builder
	for {
		ch, _ := s.r.read()
		if !tools.IsDigit(ch) {
			s.r.unread()
			break
		}
		_, _ = buf.WriteRune(ch)
	}
	return buf.String()
}

// reader represents a buffered rune reader used by the scanner.
// It provides a fixed-length circular buffer that can be unread.
type reader struct {
	r   io.RuneScanner
	i   int       // buffer index
	n   int       // buffer char count
	pos token.Pos // last read rune position
	buf [3]struct {
		ch  rune
		pos token.Pos
	}
	eof bool // true if reader has ever seen eof.
}

// ReadRune reads the next rune from the reader.
// This is a wrapper function to implement the io.RuneReader interface.
// Note that this function does not return size.
func (r *reader) ReadRune() (ch rune, size int, err error) {
	ch, _ = r.read()
	if ch == EOF {
		err = io.EOF
	}
	return
}

// UnreadRune pushes the previously read rune back onto the buffer.
// This is a wrapper function to implement the io.RuneScanner interface.
func (r *reader) UnreadRune() error {
	r.unread()
	return nil
}

// read reads the next rune from the reader.
func (r *reader) read() (ch rune, pos token.Pos) {
	// If we have unread characters then read them off the buffer first.
	if r.n > 0 {
		r.n--
		return r.curr()
	}

	// Read next rune from underlying reader.
	// Any error (including io.EOF) should return as EOF.
	ch, _, err := r.r.ReadRune()
	if err != nil {
		ch = EOF
	} else if ch == '\r' {
		if ch, _, err := r.r.ReadRune(); err != nil {
			// nop
		} else if ch != '\n' {
			_ = r.r.UnreadRune()
		}
		ch = '\n'
	}

	// Save character and position to the buffer.
	r.i = (r.i + 1) % len(r.buf)
	buf := &r.buf[r.i]
	buf.ch, buf.pos = ch, r.pos

	// Update position.
	// Only count EOF once.
	if ch == '\n' {
		r.pos.Line++
		r.pos.Char = 0
	} else if !r.eof {
		r.pos.Char++
	}

	// Mark the reader as EOF.
	// This is used so we don't double count EOF characters.
	if ch == EOF {
		r.eof = true
	}

	return r.curr()
}

// unread pushes the previously read rune back onto the buffer.
func (r *reader) unread() {
	r.n++
}

// curr returns the last read character and position.
func (r *reader) curr() (ch rune, pos token.Pos) {
	i := (r.i - r.n + len(r.buf)) % len(r.buf)
	buf := &r.buf[i]
	return buf.ch, buf.pos
}

// EOF is a marker code point to signify that the reader can't read any more.
const EOF = rune(0)

// ScanDelimited reads a delimited set of runes
func ScanDelimited(r io.RuneScanner, start, end rune, escapes map[rune]rune, escapesPassThru bool) ([]byte, error) {
	// Scan start delimiter.
	if ch, _, err := r.ReadRune(); err != nil {
		return nil, err
	} else if ch != start {
		return nil, fmt.Errorf("expected %s; found %s", string(start), string(ch))
	}

	var buf bytes.Buffer
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == end {
			return buf.Bytes(), nil
		} else if err != nil {
			return buf.Bytes(), err
		} else if ch0 == '\n' {
			return nil, errors.New("delimited text contains new line")
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, err := r.ReadRune()
			if err != nil {
				return nil, err
			}

			c, ok := escapes[ch1]
			if !ok {
				if escapesPassThru {
					// Unread ch1 (char after the \)
					_ = r.UnreadRune()
					// Write ch0 (\) to the output buffer.
					_, _ = buf.WriteRune(ch0)
					continue
				} else {
					buf.Reset()
					_, _ = buf.WriteRune(ch0)
					_, _ = buf.WriteRune(ch1)
					return buf.Bytes(), errBadEscape
				}
			}

			_, _ = buf.WriteRune(c)
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

// ScanString reads a quoted string from a rune reader.
func ScanString(r io.RuneScanner) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	var buf strings.Builder
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == ending {
			return buf.String(), nil
		} else if err != nil || ch0 == '\n' {
			return buf.String(), errBadString
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, _ := r.ReadRune()
			if ch1 == 'n' {
				_, _ = buf.WriteRune('\n')
			} else if ch1 == '\\' {
				_, _ = buf.WriteRune('\\')
			} else if ch1 == '"' {
				_, _ = buf.WriteRune('"')
			} else if ch1 == '\'' {
				_, _ = buf.WriteRune('\'')
			} else {
				return string(ch0) + string(ch1), errBadEscape
			}
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

var errBadString = errors.New("bad string")
var errBadEscape = errors.New("bad escape")

// ScanBareIdent reads bare identifier from a rune reader.
func ScanBareIdent(r io.RuneScanner) string {
	// Read every ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	var buf strings.Builder
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			break
		} else if !tools.IsIdentChar(ch) {
			_ = r.UnreadRune()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}
	return buf.String()
}
