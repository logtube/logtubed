package main

import (
	"io"
	"sync"
)

type Input interface {
	io.Closer

	Run(queue chan Event) error
}

func MultiInput(inputs ...Input) Input {
	return &multiInput{inputs: inputs}
}

type multiInput struct {
	inputs []Input
}

func (m *multiInput) Close() (err error) {
	var err1 error
	for _, i := range m.inputs {
		if err1 = i.Close(); err1 != nil {
			err = err1
		}
	}
	return
}

func (m *multiInput) Run(queue chan Event) (err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	once := &sync.Once{}

	for _, i := range m.inputs {
		var input = i
		go func() {
			var err1 error
			if err1 = input.Run(queue); err1 != nil {
				err = err1
			}
			once.Do(wg.Done)
		}()
	}

	wg.Wait()
	return
}
