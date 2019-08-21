package internal

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"time"
)

type elasticCommitter struct {
	name string
	idx  int
	bch  chan *elastic.BulkService
}

func (c *elasticCommitter) Run(ctx context.Context) error {
	log.Info().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Msg("committer started")
	for {
		select {
		case bs := <-c.bch:
			if _, err := bs.Do(ctx); err != nil {
				log.Error().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Err(err).Msg("bulk failed to commit")
			} else {
				log.Debug().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Int("count", bs.NumberOfActions()).Msg("bulk committed")
			}
		case <-ctx.Done():
			log.Info().Int("idx", c.idx).Str("name", c.name).Msg("committer exited")
			return nil
		}
	}
}

type ElasticOutputOptions struct {
	Name         string
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
	optName         string
	optConcurrency  int
	optBatchSize    int
	optBatchTimeout time.Duration

	och chan types.Op

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
	eo := &elasticOutput{
		optName:         opts.Name,
		optConcurrency:  opts.Concurrency,
		optBatchSize:    opts.BatchSize,
		optBatchTimeout: opts.BatchTimeout,
		och:             make(chan types.Op),
		c:               c,
	}
	log.Info().Str("output", "elastic").Str("name", eo.optName).Interface("opts", opts).Msg("output created")
	return eo, nil
}

func (e *elasticOutput) ConsumeOp(op types.Op) error {
	e.och <- op
	return nil
}

func (e *elasticOutput) Run(ctx context.Context) error {
	log.Info().Str("output", "elastic").Str("name", e.optName).Msg("started")
	defer log.Info().Str("output", "elastic").Str("name", e.optName).Msg("stopped")

	// bulk service channel
	bch := make(chan *elastic.BulkService)

	// create committer
	cs := make([]common.Runnable, 0, e.optConcurrency)
	for i := 0; i < e.optConcurrency; i++ {
		cs = append(cs, &elasticCommitter{idx: i + 1, bch: bch, name: e.optName})
	}

	// wait committer done on exit
	cDone := make(chan error)
	defer func() { <-cDone }()

	// run committer
	common.RunAsync(ctx, nil, cDone, cs...)

	// ticker
	t := time.NewTicker(e.optBatchTimeout)
	defer t.Stop()

	// bulk service
	var bs *elastic.BulkService

	submit := func() {
		// execute batch if not empty
		if bs != nil && bs.NumberOfActions() > 0 {
			log.Debug().Str("output", "elastic").Str("name", e.optName).Interface("actions", bs.NumberOfActions()).Msg("bulk submitted")
			bch <- bs
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
