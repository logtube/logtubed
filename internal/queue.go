package internal

import (
	"context"
	"errors"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"go.guoyk.net/diskqueue"
	"os"
	"time"
)

type QueueOptions struct {
	Dir       string
	Name      string
	SyncEvery int

	Next types.OpConsumer
}

type Queue interface {
	types.OpConsumer
	common.Runnable
	Depth() int64
}

type queue struct {
	optDir       string
	optName      string
	optSyncEvery int

	dq diskqueue.DiskQueue

	next types.OpConsumer
}

func NewQueue(opts QueueOptions) (Queue, error) {
	if len(opts.Dir) == 0 {
		return nil, errors.New("queue: Dir is not set")
	}
	if len(opts.Name) == 0 {
		return nil, errors.New("queue: Name is not set")
	}
	if opts.Next == nil {
		return nil, errors.New("queue: Next is not set")
	}
	if opts.SyncEvery <= 0 {
		opts.SyncEvery = 100
	}
	log.Info().Interface("opts", opts).Msg("queue created")
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}
	q := &queue{
		optDir:       opts.Dir,
		optName:      opts.Name,
		optSyncEvery: opts.SyncEvery,
		next:         opts.Next,
	}
	return q, nil
}

func (q *queue) Depth() int64 {
	if q == nil {
		return 0
	}
	dq := q.dq
	if dq == nil {
		return 0
	}
	return dq.Depth()
}

func (q *queue) ConsumeOp(op types.Op) error {
	dq := q.dq
	if dq == nil {
		return errors.New("Queue: not running")
	}
	return dq.Put(types.OpMarshal(op))
}

func (q *queue) Run(ctx context.Context) error {
	log.Info().Str("queue", q.optName).Msg("started")
	defer log.Info().Str("queue", q.optName).Msg("stopped")

	// create and assign diskqueue
	dq := diskqueue.New(q.optName,
		q.optDir,
		256*1024*1024,
		20,
		2*1024*1024,
		int64(q.optSyncEvery),
		time.Second*20,
	)
	q.dq = dq

loop:
	for {
		select {
		case buf := <-dq.ReadChan():
			var op types.Op
			var err error
			if op, err = types.OpUnmarshal(buf); err != nil {
				log.Error().Err(err).Msg("Queue: failed to unmarshal Op")
			}
			if err = q.next.ConsumeOp(op); err != nil {
				log.Error().Err(err).Msg("Queue: OpConsumer failed to ConsumeOp")
			}
		case <-ctx.Done():
			break loop
		}
	}

	// clear diskqueue
	q.dq = nil

	return dq.Close()
}
