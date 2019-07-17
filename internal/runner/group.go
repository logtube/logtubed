package runner

import (
	"context"
	"github.com/logtube/logtubed/internal/errutil"
	"sync"
)

type Group interface {
	Add(r Runnable)
	Run(ctx context.Context, cancel context.CancelFunc, done chan interface{}) error
}

func NewGroup(rs ...Runnable) Group {
	g := &group{}
	for _, r := range rs {
		g.Add(r)
	}
	return g
}

type group struct {
	rs []Runnable
}

func (g *group) Add(r Runnable) {
	if r == nil {
		return
	}
	g.rs = append(g.rs, r)
}

func (g *group) Run(ctx context.Context, cancel context.CancelFunc, done chan interface{}) error {
	defer close(done)

	if len(g.rs) == 0 {
		return nil
	}

	if len(g.rs) == 1 {
		defer cancel()
		return g.rs[0].Run(ctx)
	}

	eg := errutil.SafeGroup()
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
