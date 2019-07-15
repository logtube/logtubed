package egroup

import (
	"errors"
	"strings"
	"sync"
)

type ErrorGroup struct {
	l    *sync.Mutex
	errs []error
}

func New() *ErrorGroup {
	return &ErrorGroup{l: &sync.Mutex{}}
}

func (m *ErrorGroup) Put(e error) {
	m.l.Lock()
	defer m.l.Unlock()
	if e != nil {
		m.errs = append(m.errs, e)
	}
	return
}

func (m *ErrorGroup) Err() error {
	m.l.Lock()
	defer m.l.Unlock()
	if len(m.errs) == 0 {
		return nil
	}
	if len(m.errs) == 1 {
		return m.errs[0]
	}
	msgs := make([]string, 0, len(m.errs))
	for _, err := range m.errs {
		msgs = append(msgs, err.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}
