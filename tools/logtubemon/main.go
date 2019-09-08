package main

import (
	"bytes"
	"flag"
	"fmt"
	"go.guoyk.net/common"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type M map[string]interface{}

type Options struct {
	AlertDispatcher string `yaml:"alert_dispatcher"`
	Logtubed        struct {
		Endpoints      []string `yaml:"endpoints"`
		QueueThreshold int64    `yaml:"queue_threshold"`
	} `yaml:"logtubed"`
	ES struct {
		Endpoints     []string           `yaml:"endpoints"`
		DiskThreshold int                `yaml:"disk_threshold"`
		Weights       map[string]float64 `yaml:"weights"`
	} `yaml:"es"`
}

type Measure interface {
	Name() string
	Execute(ret *Results)
}

var (
	optOptions  string
	optES       bool
	optESDisk   bool
	optLogtubed bool

	opts Options

	measures []Measure
)

const (
	EmojiPassing = "✅ "
	EmojiFailed  = "❌ "
)

func exit(err *error) {
	if *err != nil {
		log.Printf((*err).Error())
		os.Exit(1)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.StringVar(&optOptions, "c", "/etc/logtubemon.yml", "config file for logtubemon")
	flag.BoolVar(&optES, "es", false, "enable es check")
	flag.BoolVar(&optESDisk, "es-disk", false, "enable es disk check")
	flag.BoolVar(&optLogtubed, "logtubed", false, "enable logtubed check")
	flag.Parse()

	if err = common.LoadYAMLConfigFile(optOptions, &opts); err != nil {
		return
	}

	if optLogtubed {
		for i, endpoint := range opts.Logtubed.Endpoints {
			measures = append(measures, &LogtubeMeasure{
				name:           fmt.Sprintf("Logtubed %d", i+1),
				endpoint:       endpoint,
				queueThreshold: opts.Logtubed.QueueThreshold,
			})
		}
	}
	if optES {
		for i, endpoint := range opts.ES.Endpoints {
			measures = append(measures, &ESMeasure{
				name:     fmt.Sprintf("ES %d", i+1),
				endpoint: endpoint,
				total:    len(opts.ES.Endpoints),
			})
		}
	}
	if optESDisk {
		common.DefaultHTTPClient = &http.Client{
			Timeout: time.Second * 30,
		}
		if len(opts.ES.Endpoints) > 0 {
			idx := rand.Intn(len(opts.ES.Endpoints))
			measures = append(measures, &ESDiskMeasure{
				name:          fmt.Sprintf("ES 磁盘"),
				endpoint:      opts.ES.Endpoints[idx],
				diskThreshold: opts.ES.DiskThreshold,
				weights:       opts.ES.Weights,
			})
		}
	}

	buf := &bytes.Buffer{}

	for _, m := range measures {
		ret := &Results{}
		name := m.Name()
		m.Execute(ret)
		for _, r := range ret.Results {
			if r.OK {
				fmt.Printf("%s %s: %s\n", EmojiPassing, name, r.Message)
			} else {
				fmt.Printf("%s %s: %s\n", EmojiFailed, name, r.Message)
				_, _ = fmt.Fprintf(buf, "%s %s: %s\n", EmojiFailed, name, r.Message)
			}
		}
	}

	msg := buf.String()

	if len(msg) == 0 {
		return
	}

	_ = common.PostJSON(opts.AlertDispatcher, M{"message": msg, "source": "logtubemon"}, nil)
}
