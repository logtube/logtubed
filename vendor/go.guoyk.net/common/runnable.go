package common

import (
	"context"
	"sync"
)

var (
	// DummyRunnable dummy runnable implements, does nothing but waits for ctx.Done()
	DummyRunnable Runnable = &dummyRunnable{}
)

// Runnable is similar with Java Runnable
type Runnable interface {
	// Run is the main method, accept a context for cancellation and returns error
	Run(ctx context.Context) error
}

type dummyRunnable struct{}

func (*dummyRunnable) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func Run(ctx context.Context, cancel context.CancelFunc, _rs ...Runnable) error {
	rs := make([]Runnable, 0, len(_rs))
	for _, r := range _rs {
		if r != nil {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		return nil
	}
	if len(rs) == 1 {
		if cancel != nil {
			defer cancel()
		}
		return rs[0].Run(ctx)
	}
	eg := NewSafeErrorGroup()
	wg := &sync.WaitGroup{}
	wg.Add(len(rs))
	for _, _r := range rs {
		r := _r
		go func() {
			eg.Add(r.Run(ctx))
			wg.Done()
			if cancel != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	return eg.Err()
}

func RunAsync(ctx context.Context, cancel context.CancelFunc, done chan error, rs ...Runnable) {
	if done == nil {
		go Run(ctx, cancel, rs...)
	} else {
		go func() { done <- Run(ctx, cancel, rs...) }()
	}
}
