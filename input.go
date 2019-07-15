package main

import (
	"context"
	"sync"
)

type Input interface {
	Run(ctx context.Context, cancelFunc context.CancelFunc, output Queue) error
}

type MultiInput []Input

func (m MultiInput) Run(ctx context.Context, cancelFunc context.CancelFunc, output Queue) (err error) {
	if len(m) == 0 {
		return
	}
	if len(m) == 1 {
		return m[0].Run(ctx, cancelFunc, output)
	}
	wg := &sync.WaitGroup{}
	for _, i := range m {
		wg.Add(1)
		go func() {
			err = combineError(err, i.Run(ctx, cancelFunc, output))
			wg.Done()
		}()
	}
	wg.Wait()
	return
}
