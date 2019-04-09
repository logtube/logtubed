package main

import "io"

type Output interface {
	io.Closer

	Put(op Operation) error
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

func (m *multiOutput) Put(op Operation) (err error) {
	var err1 error
	for _, o := range m.outputs {
		if err1 = o.Put(op); err1 != nil {
			err = err1
		}
	}
	return
}
