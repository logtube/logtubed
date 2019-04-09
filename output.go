package main

import "io"

type Output interface {
	io.Closer

	Put(e Event) error
}

func MultiOutput(outputs ...Output) Output {
	return &multiOutput{outputs: outputs}
}

type multiOutput struct {
	outputs []Output
}

func (m *multiOutput) Close() (err error) {
	var err1 error
	for _, o := range m.outputs {
		if err1 = o.Close(); err1 != nil {
			err = err1
		}
	}
	return
}

func (m *multiOutput) Put(e Event) (err error) {
	var err1 error
	for _, o := range m.outputs {
		if err1 = o.Put(e); err1 != nil {
			err = err1
		}
	}
	return
}
