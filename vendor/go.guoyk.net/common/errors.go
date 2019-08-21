package common

import (
	"sync"
)

type Errors []error

func (s Errors) Error() string {
	var msg string
	for _, err := range s {
		if len(msg) > 0 {
			msg = msg + "; "
		}
		msg = msg + err.Error()
	}
	return msg
}

type ErrorGroup interface {
	Add(e error)
	Err() error
}

type unsafeErrorGroup struct {
	errs Errors
}

func NewErrorGroup() ErrorGroup {
	return UnsafeErrorGroup()
}

func UnsafeErrorGroup() ErrorGroup {
	return &unsafeErrorGroup{}
}

func (m *unsafeErrorGroup) Add(e error) {
	if e != nil {
		m.errs = append(m.errs, e)
	}
	return
}

func (m *unsafeErrorGroup) Err() error {
	if len(m.errs) == 0 {
		return nil
	}
	if len(m.errs) == 1 {
		return m.errs[0]
	}
	return m.errs
}

type safeErrorGroup struct {
	unsafeErrorGroup
	l *sync.RWMutex
}

func NewSafeErrorGroup() ErrorGroup {
	return SafeErrorGroup()
}

func SafeErrorGroup() ErrorGroup {
	return &safeErrorGroup{l: &sync.RWMutex{}}
}

func (m *safeErrorGroup) Add(e error) {
	m.l.Lock()
	defer m.l.Unlock()
	m.unsafeErrorGroup.Add(e)
	return
}

func (m *safeErrorGroup) Err() error {
	m.l.RLock()
	defer m.l.RUnlock()
	return m.unsafeErrorGroup.Err()
}
