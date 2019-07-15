package main

import (
	"context"
	"sync"
)

type Input interface {
	Run(ctx context.Context, ctxCancel context.CancelFunc, output Queue) error
}

type MultiInput []Input

func (m MultiInput) Run(ctx context.Context, ctxCancel context.CancelFunc, output Queue) (err error) {
	if len(m) == 0 {
		return
	}
	if len(m) == 1 {
		return m[0].Run(ctx, ctxCancel, output)
	}
	wg := &sync.WaitGroup{}
	for _, _input := range m {
		input := _input
		wg.Add(1)
		go func() {
			err = combineError(err, input.Run(ctx, ctxCancel, output))
			wg.Done()
		}()
	}
	wg.Wait()
	return
}
