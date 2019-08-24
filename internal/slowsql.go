package internal

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/pkg/errors"
	"go.guoyk.net/common"
)

type slowSQL struct {
	optURL         string
	optTopic       string
	optThreshold   int
	optConcurrency int
}

type SlowSQL interface {
	types.EventConsumer
	common.Runnable
}

type SlowSQLOptions struct {
	URL         string
	Threshold   int    // duration in milliseconds
	Topic       string // topic to track
	Concurrency int
}

func NewSlowSQL(opts SlowSQLOptions) (SlowSQL, error) {
	if len(opts.URL) == 0 {
		return nil, errors.New("slowSQL: no url specified")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	if len(opts.Topic) == 0 {
		opts.Topic = "x-mybatis-track"
	}
	if opts.Threshold <= 0 {
		opts.Threshold = 3000
	}
	s := &slowSQL{
		optURL:         opts.URL,
		optThreshold:   opts.Threshold,
		optConcurrency: opts.Concurrency,
		optTopic:       opts.Topic,
	}
	return s, nil
}

func (s *slowSQL) runCommitter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (s *slowSQL) ConsumeEvent(e types.Event) (err error) {
	//TODO: implements
	return
}

func (s *slowSQL) Run(ctx context.Context) (err error) {
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()

	// start committer
	for i := 0; i < s.optConcurrency; i++ {
		go s.runCommitter(cctx)
	}

	<-ctx.Done()

	return
}
