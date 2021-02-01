package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic"
	"log"
	"net/url"
	"strings"
	"time"
)

const (
	BestCompression = "best_compression"
	Green           = "green"
	Open            = "open"
)

type ESIndex struct {
	Index      string
	Open       bool
	Green      bool
	FullMerged bool
}

type ESRecovery struct {
	Index        string `json:"index"`
	Shard        string `json:"shard"`
	BytesPercent string `json:"bytes_percent"`
}

type ES struct {
	Client *elastic.Client
}

func NewES(url string) (es *ES, err error) {
	var client *elastic.Client
	if client, err = elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(time.Second, time.Second*3600))),
	); err != nil {
		return
	}
	es = &ES{Client: client}
	return
}

func (es *ES) PutIndexTemplate(name string, buf []byte) (err error) {
	log.Printf("es: update template %s: %s", name, buf)
	_, err = es.Client.IndexPutTemplate(name).BodyString(string(buf)).Do(context.Background())
	return
}

func (es *ES) GetIndices() (indices []ESIndex, err error) {
	var cis elastic.CatIndicesResponse
	if cis, err = es.Client.CatIndices().Columns(
		"index",
		"status",
		"health",
		"pri",
		"pri.segments.count",
	).Do(context.Background()); err != nil {
		return
	}
	for _, ci := range cis {
		if strings.HasPrefix(ci.Index, ".") {
			continue
		}
		log.Printf("es: found: %s, %s, %s, %d/%d", ci.Index, ci.Status, ci.Health, ci.Pri, ci.PriSegmentsCount)
		indices = append(indices, ESIndex{
			Index:      ci.Index,
			Open:       ci.Status == Open,
			Green:      ci.Health == Green,
			FullMerged: ci.Pri >= ci.PriSegmentsCount,
		})
	}
	return
}

func (es *ES) IsIndexCodecBestCompression(index string) (is bool, err error) {
	var res map[string]*elastic.IndicesGetSettingsResponse
	if res, err = es.Client.IndexGetSettings(index).FlatSettings(true).Do(context.Background()); err != nil {
		return
	}
	settings := res[index]
	if settings == nil {
		err = errors.New("missing settings for " + index)
		return
	}
	log.Printf("es: settings: %s = %+v", index, settings.Settings)
	codec, _ := settings.Settings["index.codec"].(string)
	is = codec == BestCompression
	return
}

func (es *ES) IsIndexRoutingToHDD(index string) (is bool, err error) {
	var res map[string]*elastic.IndicesGetSettingsResponse
	if res, err = es.Client.IndexGetSettings(index).FlatSettings(true).Do(context.Background()); err != nil {
		return
	}
	settings := res[index]
	if settings == nil {
		err = errors.New("missing settings for " + index)
		return
	}
	log.Printf("es: settings: %s = %+v", index, settings.Settings)
	exclude, _ := settings.Settings["index.routing.allocation.exclude.disktype"].(string)
	require, _ := settings.Settings["index.routing.allocation.require.disktype"].(string)
	if len(exclude) == 0 && require == "hdd" {
		is = true
	}
	return
}

func (es *ES) SetIndexCodecBestCompression(index string) (err error) {
	// close
	if err = es.CloseIndex(index); err != nil {
		return
	}
	// update best_compression
	log.Printf("es: set best compression: %s", index)
	if _, err = es.Client.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]string{
		"index.codec": BestCompression,
	}).Do(context.Background()); err != nil {
		return
	}
	// open
	if err = es.OpenIndex(index); err != nil {
		return
	}
	return
}

func (es *ES) SetIndexRoutingToHDD(index string) (err error) {
	log.Printf("es: move index to hdd: %s", index)
	if _, err = es.Client.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.routing.allocation.exclude.disktype": nil,
		"index.routing.allocation.require.disktype": "hdd",
	}).Do(context.Background()); err != nil {
		return
	}
	return
}

func (es *ES) CloseIndex(index string) (err error) {
	log.Printf("es: close: %s", index)
	if err = es.UnmarkIndexReadOnly(index); err != nil {
		return
	}
	if _, err = es.Client.CloseIndex(index).Do(context.Background()); err != nil {
		return
	}
	return
}

func (es *ES) OpenIndex(index string) (err error) {
	log.Printf("es: open: %s", index)
	_, err = es.Client.OpenIndex(index).WaitForActiveShards("all").Do(context.Background())
	return
}

func (es *ES) MarkIndexReadOnly(index string) (err error) {
	log.Printf("es: mark read only: %s", index)
	_, err = es.Client.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]bool{
		"index.blocks.write": true,
	}).Do(context.Background())
	return
}

func (es *ES) UnmarkIndexReadOnly(index string) (err error) {
	log.Printf("es: unmark read only: %s", index)
	_, err = es.Client.IndexPutSettings(index).FlatSettings(true).BodyJson(map[string]interface{}{
		"index.blocks.write":                  nil,
		"index.blocks.read_only_allow_delete": nil,
	}).Do(context.Background())
	return
}

func (es *ES) FullMergeIndex(index string) (err error) {
	if err = es.MarkIndexReadOnly(index); err != nil {
		return
	}
	// force merge with 1 segment
	log.Printf("es: full merge: %s", index)
	if _, err = es.Client.Forcemerge(index).MaxNumSegments(1).Do(context.Background()); err != nil {
		return
	}
	if err = es.UnmarkIndexReadOnly(index); err != nil {
		return
	}
	return
}

func (es *ES) DeleteIndex(index string) error {
	log.Printf("es: delete index: %s", index)
	_, err := es.Client.DeleteIndex(index).Do(context.Background())
	return err
}

func (es *ES) WaitClusterRecovery() (err error) {
	var count int
wait:
	if count, err = es.GetClusterRecoveryCount(); err != nil {
		return
	}
	if count > 0 {
		time.Sleep(time.Second * 10)
		goto wait
	}
	return
}

func (es *ES) GetClusterRecoveryCount() (count int, err error) {
	params := url.Values{}
	params.Set("format", "json")
	params.Set("active_only", "true")
	var res *elastic.Response
	if res, err = es.Client.PerformRequest(context.Background(), elastic.PerformRequestOptions{
		Method: "GET",
		Path:   "/_cat/recovery",
		Params: params,
	}); err != nil {
		return
	}
	if res.StatusCode >= 300 {
		err = fmt.Errorf("es: failed to retrieve /_cat/recovery: %d", res.StatusCode)
		return
	}
	var rs []ESRecovery
	if err = json.Unmarshal(res.Body, &rs); err != nil {
		return
	}
	log.Printf("es: cluster recoveries in progress: %v", rs)
	count = len(rs)
	return
}
