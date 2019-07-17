package errutil

import (
	"errors"
	"strings"
	"sync"
)

type Group interface {
	Add(e error)
	Err() error
}

type unsafeGroup struct {
	errs []error
}

func UnsafeGroup() Group {
	return &unsafeGroup{}
}

func (m *unsafeGroup) Add(e error) {
	if e != nil {
		m.errs = append(m.errs, e)
	}
	return
}

func (m *unsafeGroup) Err() error {
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

type safeGroup struct {
	unsafeGroup
	l *sync.Mutex
}

func SafeGroup() Group {
	return &safeGroup{l: &sync.Mutex{}}
}

func (m *safeGroup) Add(e error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.unsafeGroup.Add(e)
	return
}

func (m *safeGroup) Err() error {
	m.l.Lock()
	defer m.l.Unlock()
	return m.unsafeGroup.Err()
}
