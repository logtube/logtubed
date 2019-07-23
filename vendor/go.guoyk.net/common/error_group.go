package common

import (
	"errors"
	"sync"
)

type ErrorGroup interface {
	Add(e error)
	Err() error
}

type errorGroup struct {
	errs []error
}

func NewErrorGroup() ErrorGroup {
	return &errorGroup{}
}

func (m *errorGroup) Add(e error) {
	if e != nil {
		m.errs = append(m.errs, e)
	}
	return
}

func (m *errorGroup) Err() error {
	if len(m.errs) == 0 {
		return nil
	}
	if len(m.errs) == 1 {
		return m.errs[0]
	}
	var msg string
	for _, err := range m.errs {
		if len(msg) > 0 {
			msg = msg + "; "
		}
		msg = msg + err.Error()
	}
	return errors.New(msg)
}

type safeErrorGroup struct {
	errorGroup
	l *sync.RWMutex
}

func NewSafeErrorGroup() ErrorGroup {
	return &safeErrorGroup{l: &sync.RWMutex{}}
}

func (m *safeErrorGroup) Add(e error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.errorGroup.Add(e)
	return
}

func (m *safeErrorGroup) Err() error {
	m.l.RLock()
	defer m.l.RUnlock()
	return m.errorGroup.Err()
}
