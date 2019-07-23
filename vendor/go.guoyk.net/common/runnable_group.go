package common

import (
	"context"
	"sync"
)

type RunnableGroup interface {
	Add(r Runnable)
	Run(ctx context.Context, cancel context.CancelFunc, done chan interface{}) error
}

func NewRunnableGroup(rs ...Runnable) RunnableGroup {
	g := &runnableGroup{}
	for _, r := range rs {
		g.Add(r)
	}
	return g
}

type runnableGroup struct {
	rs []Runnable
}

func (g *runnableGroup) Add(r Runnable) {
	if r == nil {
		return
	}
	g.rs = append(g.rs, r)
}

func (g *runnableGroup) Run(ctx context.Context, cancel context.CancelFunc, done chan interface{}) error {
	defer close(done)

	if len(g.rs) == 0 {
		return nil
	}

	if len(g.rs) == 1 {
		defer cancel()
		return g.rs[0].Run(ctx)
	}

	eg := NewSafeErrorGroup()
	wg := &sync.WaitGroup{}
	for _, _r := range g.rs {
		wg.Add(1)
		r := _r
		go func() {
			eg.Add(r.Run(ctx))
			wg.Done()
			cancel()
		}()
	}
	wg.Wait()
	return eg.Err()
}
