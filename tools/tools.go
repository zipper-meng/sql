package tools

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"sql/token"
)

// IsWhitespace returns true if the rune is a space, tab, or newline.
func IsWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// IsLetter returns true if the rune is a letter.
func IsLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// IsDigit returns true if the rune is a digit.
func IsDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// IsIdentChar returns true if the rune can be used in an unquoted identifier.
func IsIdentChar(ch rune) bool { return IsLetter(ch) || IsDigit(ch) || ch == '_' }

// IsIdentFirstChar returns true if the rune can be used as the first char in an unquoted identifer.
func IsIdentFirstChar(ch rune) bool { return IsLetter(ch) || ch == '_' }

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
		if i == 0 && !IsIdentFirstChar(r) {
			return true
		} else if i > 0 && !IsIdentChar(r) {
			return true
		}
	}
	return false
}

// IsDateString returns true if the string looks like a date-only time literal.
func IsDateString(s string) bool { return dateStringRegexp.MatchString(s) }

// IsDateTimeString returns true if the string looks like a date+time time literal.
func IsDateTimeString(s string) bool { return dateTimeStringRegexp.MatchString(s) }

var dateStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeStringRegexp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}.+`)

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
