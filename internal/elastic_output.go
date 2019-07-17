package internal

import (
	"context"
	"github.com/logtube/logtubed/internal/runner"
	"github.com/logtube/logtubed/types"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog/log"
	"time"
)

type ElasticOutputOptions struct {
	URLs         []string
	BatchSize    int
	BatchTimeout time.Duration
}

type ElasticOutput interface {
	types.OpConsumer
	runner.Runnable
}

// ElasticOutput implements OpConsumer and Runnable
type elasticOutput struct {
	optBatchSize    int
	optBatchTimeout time.Duration

	ch chan types.Op

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
	var c *elastic.Client
	var err error
	if c, err = elastic.NewClient(elastic.SetURL(opts.URLs...)); err != nil {
		return nil, err
	}
	eo := &elasticOutput{
		optBatchSize:    opts.BatchSize,
		optBatchTimeout: opts.BatchTimeout,
		ch:              make(chan types.Op),
		c:               c,
	}
	return eo, nil
}

func (e *elasticOutput) ConsumeOp(op types.Op) error {
	e.ch <- op
	return nil
}

func (e *elasticOutput) Run(ctx context.Context) error {
	log.Info().Str("output", "elastic").Msg("started")
	defer log.Info().Str("output", "elastic").Msg("stopped")

	t := time.NewTicker(e.optBatchTimeout)
	defer t.Stop()

	var bs *elastic.BulkService

	submit := func() {
		// execute batch if not empty
		if bs != nil && bs.NumberOfActions() > 0 {
			log.Debug().Str("output", "elastic").Interface("actions", bs.NumberOfActions()).Msg("bulk submitted")
			if _, err := bs.Do(context.Background()); err != nil {
				log.Error().Err(err).Msg("ElasticOutput: failed to execute bulk")
			}
			bs = nil
		}
	}

	for {
		select {
		case op := <-e.ch:
			// create batch if not existed
			if bs == nil {
				bs = elastic.NewBulkService(e.c)
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
