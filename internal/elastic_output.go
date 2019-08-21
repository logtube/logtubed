package internal

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"time"
)

type ElasticOutputOptions struct {
	URLs         []string
	Concurrency  int
	BatchSize    int
	BatchTimeout time.Duration
}

type ElasticOutput interface {
	types.OpConsumer
	common.Runnable
}

// ElasticOutput implements OpConsumer and Runnable
type elasticOutput struct {
	optConcurrency  int
	optBatchSize    int
	optBatchTimeout time.Duration

	och chan types.Op
	bch chan *elastic.BulkService

	c *elastic.Client
}

// NewElasticOutput create a new ElasticOutput
func NewElasticOutput(opts ElasticOutputOptions) (ElasticOutput, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.BatchTimeout <= 0 {
		opts.BatchTimeout = time.Second * 3
	}
	if len(opts.URLs) == 0 {
		opts.URLs = []string{"http://127.0.0.1:9200"}
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 3
	}
	var c *elastic.Client
	var err error
	if c, err = elastic.NewClient(elastic.SetURL(opts.URLs...)); err != nil {
		return nil, err
	}
	log.Info().Str("output", "elastic").Interface("opts", opts).Msg("output created")
	eo := &elasticOutput{
		optConcurrency:  opts.Concurrency,
		optBatchSize:    opts.BatchSize,
		optBatchTimeout: opts.BatchTimeout,
		och:             make(chan types.Op),
		bch:             make(chan *elastic.BulkService),
		c:               c,
	}
	return eo, nil
}

func (e *elasticOutput) ConsumeOp(op types.Op) error {
	e.och <- op
	return nil
}

func (e *elasticOutput) runCommitter(ctx context.Context) {
	log.Info().Str("output", "elastic").Msg("committer started")
	for {
		select {
		case bs := <-e.bch:
			if _, err := bs.Do(context.Background()); err != nil {
				log.Error().Err(err).Msg("ElasticOutput: failed to execute bulk")
			} else {
				log.Debug().Int("count", bs.NumberOfActions()).Msg("ElasticOutput: bulk committed")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (e *elasticOutput) Run(ctx context.Context) error {
	log.Info().Str("output", "elastic").Msg("started")
	defer log.Info().Str("output", "elastic").Msg("stopped")

	// committee context
	cCtx, cCancel := context.WithCancel(context.Background())
	defer cCancel()

	// ignite committees
	for i := 0; i < e.optConcurrency; i++ {
		go e.runCommitter(cCtx)
	}

	// ticker
	t := time.NewTicker(e.optBatchTimeout)
	defer t.Stop()

	// bulk service
	var bs *elastic.BulkService

	submit := func() {
		// execute batch if not empty
		if bs != nil && bs.NumberOfActions() > 0 {
			log.Debug().Str("output", "elastic").Interface("actions", bs.NumberOfActions()).Msg("bulk submitted")
			e.bch <- bs
			bs = nil
		}
	}

	for {
		select {
		case op := <-e.och:
			// create batch if not existed
			if bs == nil {
				bs = elastic.NewBulkService(e.c)
				bs.Retrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(time.Second*5, time.Second*3600)))
			}
			// add batch operation
			bs.Add(elastic.NewBulkIndexRequest().Index(op.Index).Type("_doc").Doc(string(op.Body)))
			// execute batch if batch size exceeded
			if bs.NumberOfActions() >= e.optBatchSize {
				submit()
			}
		case <-t.C:
			submit()
		case <-ctx.Done():
			submit()
			return nil
		}
	}
}
