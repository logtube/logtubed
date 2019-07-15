package main

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/diskqueue"
	"os"
	"time"
)

var (
	ErrQueueIsNotRunning = errors.New("queue is not running")
)

var (
	hostname string
)

type Queue interface {
	Stats() *Stats
	PutEvent(e Event) error
	Depth() int64
	Run(ctx context.Context, output Output) error
}

type queue struct {
	topicsPri map[string]bool
	topicsKey map[string]bool
	topicsIgn map[string]bool

	dqName      string
	dqDir       string
	dqSyncEvery int

	dqStd diskqueue.DiskQueue
	dqPri diskqueue.DiskQueue

	stats *Stats
}

func init() {
	hostname, _ = os.Hostname()
	if len(hostname) == 0 {
		hostname = "localhost"
	}
}

func NewEventQueue(qOpts QueueOptions, tOpts TopicsOptions) (o Queue, err error) {
	// ensure disk queue directory
	if err = os.MkdirAll(qOpts.Dir, 0755); err != nil {
		return
	}
	// create queue with standard priority and high priority
	eq := &queue{
		topicsPri:   map[string]bool{},
		topicsKey:   map[string]bool{},
		topicsIgn:   map[string]bool{},
		dqName:      qOpts.Name,
		dqDir:       qOpts.Dir,
		dqSyncEvery: qOpts.SyncEvery,
	}
	// update topics
	for _, t := range tOpts.Priors {
		eq.topicsPri[t] = true
	}
	for _, t := range tOpts.Ignored {
		eq.topicsIgn[t] = true
	}
	for _, t := range tOpts.KeywordRequired {
		eq.topicsKey[t] = true
	}
	// create stats
	eq.stats = NewStats(eq.Depth)
	// return
	o = eq
	return
}

func (q *queue) Stats() *Stats {
	return q.stats
}

func (q *queue) PutEvent(e Event) (err error) {
	// check queue running
	dqStd, dqPri := q.dqStd, q.dqPri
	if dqStd == nil || dqPri == nil {
		err = ErrQueueIsNotRunning
		return
	}

	// check ignores
	if q.topicsIgn[e.Topic] {
		return
	}
	// check keyword required
	if q.topicsKey[e.Topic] && len(e.Keyword) == 0 {
		return
	}
	// assign e.Hostname
	e.Hostname = hostname
	// marshal
	var buf []byte
	if buf, err = e.ToOperation().DiskQueueMarshal(); err != nil {
		return
	}
	// update stats
	q.stats.IncrQueueIn()
	// put
	if q.topicsPri[e.Topic] {
		if err = dqPri.Put(buf); err != nil {
			return
		}
	} else {
		if err = dqStd.Put(buf); err != nil {
			return
		}
	}
	return
}

func (q *queue) Run(ctx context.Context, output Output) (err error) {
	// run stats
	go q.stats.Run(ctx)

	// create diskqueue
	dqStd, dqPri := diskqueue.New(
		q.dqName,
		q.dqDir,
		256*1024*1024,
		20,
		2*1024*1024,
		int64(q.dqSyncEvery),
		time.Second*20,
	), diskqueue.New(
		q.dqName+"-pri",
		q.dqDir,
		256*1024*1024,
		20,
		2*1024*1024,
		int64(q.dqSyncEvery),
		time.Second*20,
	)

	// assign diskqueue
	q.dqStd, q.dqPri = dqStd, dqPri

	// loop
LOOP:
	for {
		var o Operation
		var b []byte

		// select
		select {
		case b = <-dqPri.ReadChan():
		case b = <-dqStd.ReadChan():
		case <-ctx.Done():
			break LOOP
		}

		// dequeue
		q.stats.IncrQueueOut()

		// unmarshal
		if o, err = DiskQueueUnmarshal(b); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal Operation")
			continue LOOP
		}

		// output
		if len(o.Index) > 0 && len(o.Body) > 0 {
			if err = output.PutOperation(o); err != nil {
				log.Error().Err(err).Msg("failed to output Operation")
			}
		}
	}

	// close diskqueue
	err = combineError(dqStd.Close(), dqPri.Close())

	// clear
	q.dqStd, q.dqPri = nil, nil
	return
}

func (q *queue) Depth() (out int64) {
	dqStd, dqPri := q.dqStd, q.dqPri
	if dqStd != nil {
		out += dqStd.Depth()
	}
	if dqPri != nil {
		out += dqPri.Depth()
	}
	return
}
