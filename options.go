package main

import (
	"io/ioutil"
	"os"
	"strings"

	"github.com/go-yaml/yaml"
)

// Options options for logtubed
type Options struct {
	Verbose  bool   `yaml:"verbose"`
	Hostname string `yaml:"hostname"`
	PProf    struct {
		Bind  string `yaml:"bind"`
		Block int    `yaml:"block"`
		Mutex int    `yaml:"mutex"`
	} `yaml:"pprof"`
	InputRedis struct {
		Enabled    bool   `yaml:"enabled"`
		Bind       string `yaml:"bind"`
		Multi      bool   `yaml:"multi"`
		TimeOffset int    `yaml:"time_offset"`
	} `yaml:"input_redis"`
	InputSPTP struct {
		Enabled bool   `yaml:"enabled"`
		Bind    string `yaml:"bind"`
	} `yaml:"input_sptp"`
	Topics struct {
		KeywordRequired []string `yaml:"keyword_required"`
		Ignored         []string `yaml:"ignored"`
		Priors          []string `yaml:"priors"`
	} `yaml:"topics"`
	Queue struct {
		Dir       string `yaml:"dir"`
		Name      string `yaml:"name"`
		SyncEvery int    `yaml:"sync_every"`
	} `yaml:"queue"`
	OutputES struct {
		Enabled      bool     `yaml:"enabled"`
		URLs         []string `yaml:"urls"`
		BatchSize    int      `yaml:"batch_size"`
		BatchTimeout int      `yaml:"batch_timeout"`
	} `yaml:"output_es"`
	OutputLocal struct {
		Enabled bool   `yaml:"enabled"`
		Dir     string `yaml:"dir"`
	} `yaml:"output_local"`
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
	if len(opt.Hostname) == 0 {
		opt.Hostname, _ = os.Hostname()
	}
	if len(opt.Hostname) == 0 {
		opt.Hostname = "localhost"
	}
	defaultStr(&opt.InputRedis.Bind, "0.0.0.0:6379")
	defaultStr(&opt.InputSPTP.Bind, "0.0.0.0:9921")
	defaultStr(&opt.Queue.Dir, "/var/lib/logtubed")
	defaultStr(&opt.Queue.Name, "logtubed")
	defaultInt(&opt.Queue.SyncEvery, 100)
	defaultStrSlice(&opt.OutputES.URLs, []string{"http://127.0.0.1:9200"})
	defaultInt(&opt.OutputES.BatchSize, 100)
	defaultInt(&opt.OutputES.BatchTimeout, 3)
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
