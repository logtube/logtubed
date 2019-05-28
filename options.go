package main

import (
	"io/ioutil"
	"strings"

	"github.com/go-yaml/yaml"
)

type RedisInputOptions struct {
	Enabled    bool   `yaml:"enabled"`     // whether redis input is enabled
	Bind       string `yaml:"bind"`        // bind address
	Multi      bool   `yaml:"multi"`       // report as redis 2.4+, support multiple RPUSH/LPUSH
	TimeOffset int    `yaml:"time_offset"` // for legacy message, default time offset, set -8 for Asia/Shanghai
}

type SPTPInputOptions struct {
	Enabled bool   `yaml:"enabled"` // whether SPTP input is enabled
	Bind    string `yaml:"bind"`    // bind address
}

type TopicsOptions struct {
	KeywordRequired []string `yaml:"keyword_required"`
	Ignored         []string `yaml:"ignored"`
}

type QueueOptions struct {
	Dir       string `yaml:"dir"`
	Name      string `yaml:"name"`
	SyncEvery int    `yaml:"sync_every"`
}

type ESOutputOptions struct {
	Enabled    bool     `yaml:"enabled"`
	URLs       []string `yaml:"urls"`
	BatchSize  int      `yaml:"batch_size"`
	BatchRate  int      `yaml:"batch_rate"`
	BatchBurst int      `yaml:"batch_burst"`
}

type LocalOutputOptions struct {
	Enabled bool   `yaml:"enabled"`
	Dir     string `yaml:"dir"`
}

type PProfOptions struct {
	Bind string `yaml:"bind"`
}

// Options options for logtubed
type Options struct {
	Verbose     bool               `yaml:"verbose"`
	PProf       PProfOptions       `yaml:"pprof"`
	InputRedis  RedisInputOptions  `yaml:"input_redis"`
	InputSPTP   SPTPInputOptions   `yaml:"input_sptp"`
	Topics      TopicsOptions      `yaml:"topics"`
	Queue       QueueOptions       `yaml:"queue"`
	OutputES    ESOutputOptions    `yaml:"output_es"`
	OutputLocal LocalOutputOptions `yaml:"output_local"`
}

func loadOptionsFile(filename string) (opt Options, err error) {
	var buf []byte
	// read and unmarshal
	if buf, err = ioutil.ReadFile(filename); err != nil {
		return
	}
	if err = yaml.Unmarshal(buf, &opt); err != nil {
		return
	}
	return
}

// LoadOptions load options from yaml file
func LoadOptions(filename string) (opt Options, err error) {
	if opt, err = loadOptionsFile(filename); err != nil {
		return
	}
	defaultStr(&opt.InputRedis.Bind, "0.0.0.0:6379")
	defaultStr(&opt.InputSPTP.Bind, "0.0.0.0:9921")
	defaultStr(&opt.Queue.Dir, "/var/lib/logtubed")
	defaultStr(&opt.Queue.Name, "logtubed")
	defaultInt(&opt.Queue.SyncEvery, 100)
	defaultStrSlice(&opt.OutputES.URLs, []string{"http://127.0.0.1:9200"})
	defaultInt(&opt.OutputES.BatchSize, 100)
	defaultInt(&opt.OutputES.BatchRate, 1000)
	defaultInt(&opt.OutputES.BatchBurst, 10000)
	defaultStr(&opt.OutputLocal.Dir, "/var/log")
	defaultStr(&opt.PProf.Bind, "0.0.0.0:6060")
	return
}

func defaultStr(v *string, defaultValue string) {
	*v = strings.TrimSpace(*v)
	if len(*v) == 0 {
		*v = defaultValue
	}
}

func defaultStrSlice(v *[]string, defaultValue []string) {
	if len(*v) == 0 {
		*v = defaultValue
	}
}

func defaultInt(v *int, defaultValue int) {
	if *v <= 0 {
		*v = defaultValue
	}
}
