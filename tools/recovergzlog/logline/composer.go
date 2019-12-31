package logline

import (
	"regexp"
	"strings"
)

var linePattern = regexp.MustCompile(`^\[\d\d\d\d[/\-]\d\d[/\-]\d\d \d\d:\d\d:\d\d`)

type Composer interface {
	Feed(line string) (message string)
	End() (message string)
}

type composer struct {
	message string
}

func NewComposer() Composer {
	return &composer{}
}

func (m *composer) Feed(line string) (message string) {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	if linePattern.MatchString(line) {
		message = m.message
		m.message = line
	} else {
		m.message = m.message + "\n" + line
	}
	return
}

func (m *composer) End() (message string) {
	message = m.message
	m.message = ""
	return
}
