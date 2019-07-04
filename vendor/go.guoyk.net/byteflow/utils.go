package byteflow

import (
	"unicode"
	"unicode/utf8"
)

func utf8EndsWithSpace(buf []byte) bool {
	r, _ := utf8.DecodeLastRune(buf)
	if r == utf8.RuneError {
		return false
	}
	return unicode.IsSpace(r)
}

func utf8IndexOfRune(buf []byte, u rune) int {
	var i int
	for {
		if r, s := utf8.DecodeRune(buf[i:]); r == utf8.RuneError {
			return -1
		} else {
			if r == u {
				return i
			} else {
				i += s
			}
		}
	}
}
