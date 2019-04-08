package main

import (
	"errors"
	"io/ioutil"

	"github.com/go-yaml/yaml"
)

// Options options for xlogd
type Options struct {
	// Dev
	// development mode, will be more verbose
	Dev bool `yaml:"dev"`
	// Bind
	// bind address for redis protocol
	Bind string `yaml:"bind"`
	// DataDir
	DataDir string `yaml:"data_dir"`
	// Multi
	Multi bool `yaml:"multi"`
	// ElasticSearch
	// ElasticSearch options
	ElasticSearch ElasticSearchOptions `yaml:"elasticsearch"`
	// TimeOffset
	// generally timezone information is missing from log files, you may need set a offset to fix it
	// for 'Asia/Shanghai', set TimeOffset to -8
	TimeOffset int `yaml:"time_offset"`
	// EnforceKeyword
	// topic should be keyword enforced
	EnforceKeyword []string `yaml:"enforce_keyword"`
	// Ignore
	// topic should be ignored
	Ignore []string `yaml:"ignore"`
}

// ElasticSearchOptions options for ElasticSearch
type ElasticSearchOptions struct {
	// URLs
	// urls of elasticsearch instances, should be something like http://127.0.0.1:9200
	URLs []string `yaml:"urls"`
	// Batch
	// by default, batch size is 100 and a timeout of 10s
	// that means xlogd will perform a bulk write once cached records reached 100, or been idle for 10 seconds
	Batch BatchOptions `yaml:"batch"`
}

// BatchOptions options for batch processing
type BatchOptions struct {
	// Size
	// batch size
	Size int `yaml:"size"`
	// Rate
	// rate per second reduce elasticsearch write
	Rate int `yaml:"rate"`
	// Burst
	// burst capacity
	Burst int `yaml:"burst"`
}

// LoadOptions load options from yaml file
func LoadOptions(filename string) (opt Options, err error) {
	var buf []byte
	// read and unmarshal
	if buf, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &opt); err != nil {
		return
	}
	// check data_dir
	if len(opt.DataDir) == 0 {
		opt.DataDir = "/data/logtube"
	}
	// check bind
	if len(opt.Bind) == 0 {
		opt.Bind = "0.0.0.0:6379"
	}
	// check elasticsearch urls
	if len(opt.ElasticSearch.URLs) == 0 {
		err = errors.New("no elasticsearch urls")
		return
	}
	// check batch size
	if opt.ElasticSearch.Batch.Size <= 0 {
		opt.ElasticSearch.Batch.Size = 100
	}
	// check batch limit
	if opt.ElasticSearch.Batch.Rate <= 0 {
		opt.ElasticSearch.Batch.Rate = 1000
	}
	// check batch burst
	if opt.ElasticSearch.Batch.Burst <= 0 {
		opt.ElasticSearch.Batch.Burst = 10000
	}
	return
}
