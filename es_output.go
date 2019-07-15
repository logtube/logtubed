package main

import (
	"context"
	"github.com/juju/ratelimit"
	"github.com/olivere/elastic"
	"github.com/rs/zerolog/log"
	"time"
)

type ESOutput struct {
	Options ESOutputOptions

	client *elastic.Client
	bulk   *elastic.BulkService

	bucket *ratelimit.Bucket

	t time.Time
}

func NewESOutput(options ESOutputOptions) (o *ESOutput, err error) {
	o = &ESOutput{
		Options: options,
		bucket: ratelimit.NewBucket(
			time.Second/time.Duration(options.BatchRate),
			int64(options.BatchBurst),
		),
		t: time.Now(),
	}
	if o.client, err = elastic.NewClient(
		elastic.SetURL(options.URLs...),
	); err != nil {
		return
	}
	return
}

func (e *ESOutput) checkTimeout() bool {
	t := e.t
	n := time.Now()
	e.t = n
	if n.Sub(t) > time.Second*3 {
		return true
	}
	return false
}

func (e *ESOutput) Close() (err error) {
	if e.bulk != nil {
		_, err = e.bulk.Do(context.Background())
		e.bulk = nil
	}
	return
}

func (e *ESOutput) PutOperation(op Operation) (err error) {
	if e.bulk == nil {
		e.bulk = e.client.Bulk()
	}
	// append bulk operation
	br := elastic.NewBulkIndexRequest().Index(op.Index).Type("_doc").Doc(string(op.Body))
	e.bulk.Add(br)
	// submit bulk if needed
	if e.checkTimeout() || e.bulk.NumberOfActions() > e.Options.BatchSize {
		_, err = e.bulk.Do(context.Background())
		if err != nil {
			log.Error().Str("output", "ES").Err(err).Msg("failed to bulk insert")
		} else {
			log.Debug().Str("output", "ES").Int("count", e.bulk.NumberOfActions()).Msg("events out")
		}
		e.bulk = nil
		return
	}
	return
}
