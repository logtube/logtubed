package core

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"time"
)

type elasticCommitter struct {
	name   string
	idx    int
	client *elastic.Client
	opCh   chan []types.Op
}

func (c *elasticCommitter) Run(ctx context.Context) error {
	log.Info().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Msg("committer started")
	for {
		select {
		case ops := <-c.opCh:
			var err error
			var res *elastic.BulkResponse
			var retryCount int
		retry:
			// create bulk service
			bs := elastic.NewBulkService(c.client)
			bs.Retrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(time.Second*5, time.Hour*24)))
			for _, op := range ops {
				bs.Add(elastic.NewBulkIndexRequest().Index(op.Index).Type("_doc").Doc(string(op.Body)))
			}
			// execute bulk
			if res, err = bs.Do(ctx); err != nil {
				// connection error, already retried
				log.Error().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Int("total_count", len(ops)).Int("retried", retryCount).Err(err).Msg("bulk failed to commit")
			} else if res.Errors {
				// bulk error
				// calculate failed count
				failed := res.Failed()
				log.Error().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Str("reason", "bulk failed").Int("failed_count", len(failed)).Int("total_count", len(ops)).Int("retried", retryCount).Msg("bulk failed to commit")
				// sample errors
				sampled := failed
				if len(sampled) > 5 {
					sampled = sampled[0:5]
				}
				for _, s := range sampled {
					log.Error().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Interface("sample", s).Msg("bulk failed sampled")
				}
				// 忽略 closed 错误不进行重试
				isClosed := true
				for _, sample := range sampled {
					if sample.Error.Reason != "closed" {
						isClosed = false
						break
					}
				}
				if isClosed {
					continue
				}
				// if more than half of the actions failed, means the system is down
				if len(failed)*2 > len(ops) {
					// increase retryCount
					retryCount++
					// sleep exponential time
					retryTimer := time.NewTimer(2 * time.Second * time.Duration(retryCount) * time.Duration(retryCount))
					select {
					case <-retryTimer.C:
						// retry
						goto retry
					case <-ctx.Done():
						// or exit on context cancelled
						return nil
					}
				}
			} else {
				log.Debug().Int("idx", c.idx).Str("name", c.name).Str("output", "elastic").Int("count", len(ops)).Msg("bulk committed")
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

	// bulk channel
	opCh := make(chan []types.Op)

	// create committer
	cs := make([]common.Runnable, 0, e.optConcurrency)
	for i := 0; i < e.optConcurrency; i++ {
		cs = append(cs, &elasticCommitter{idx: i + 1, opCh: opCh, client: e.c, name: e.optName})
	}

	// wait committer done on exit
	cDone := make(chan error)
	defer func() { <-cDone }()

	// run committer
	common.RunAsync(ctx, nil, cDone, cs...)

	// ticker
	t := time.NewTicker(e.optBatchTimeout)
	defer t.Stop()

	// bulk
	var ops []types.Op

	// submit func
	submit := func() {
		// execute batch if not empty
		if len(ops) > 0 {
			log.Debug().Str("output", "elastic").Str("name", e.optName).Interface("actions", len(ops)).Msg("bulk submitted")
			opCh <- ops
			ops = nil
		}
	}

	for {
		select {
		case op := <-e.och:
			// append op
			ops = append(ops, op)
			// submit bulk
			if len(ops) >= e.optBatchSize {
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
