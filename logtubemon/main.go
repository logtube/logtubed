package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type LogtubeStatsData struct {
	Time  string
	Value uint64
}

type LogtubeStats struct {
	QueueIn    []LogtubeStatsData `json:"queue_in"`
	QueueOut   []LogtubeStatsData `json:"queue_out"`
	QueueDepth []LogtubeStatsData `json:"queue_depth"`
}

type ESHealth struct {
	Status        string `json:"status"`
	NumberOfNodes int    `json:"number_of_nodes"`
}

type Options struct {
	URL                   string   `json:"url"`
	LogtubeStatsEndpoints []string `json:"logtube_stats_endpoints"`
	ESHealthEndpoints     []string `json:"es_health_endpoints"`
}

type State map[string]bool

var (
	optOptions string

	options Options

	message string
)

func getJSON(url string, out interface{}) (err error) {
	var resp *http.Response
	if resp, err = http.Get(url); err != nil {
		return
	}
	defer resp.Body.Close()
	var buf []byte
	if buf, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	err = json.Unmarshal(buf, out)
	return
}

func postJSON(url string, in interface{}) (err error) {
	var buf []byte
	if buf, err = json.Marshal(in); err != nil {
		return
	}
	var resp *http.Response
	if resp, err = http.Post(url, "application/json", bytes.NewReader(buf)); err != nil {
		return
	}
	defer resp.Body.Close()
	return
}

func loadJSON(file string, out interface{}) (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	err = json.Unmarshal(buf, out)
	return
}

func appendMessage(format string, item ...interface{}) {
	if len(message) > 0 {
		message = message + "\n"
	}
	message = message + fmt.Sprintf(format, item...)
	log.Printf(format+"\n", item...)
}

func exit(err *error) {
	if *err != nil {
		log.Println((*err).Error())
		os.Exit(1)
	}
}

func main() {
	var err error
	defer exit(&err)

	flag.StringVar(&optOptions, "c", "/etc/logtubemon.json", "config file for logtubemon")
	flag.Parse()

	// load config and state
	if err = loadJSON(optOptions, &options); err != nil {
		return
	}

	// check logtubed status
	for i, url := range options.LogtubeStatsEndpoints {
		// fetch stats
		var d LogtubeStats
		if err = getJSON(url, &d); err != nil {
			appendMessage("❌ Logtubed %d 无法监控: %s", i+1, err.Error())
			continue
		}
		if len(d.QueueDepth) < 1 {
			continue
		}
		// check queue depth
		depth := d.QueueDepth[len(d.QueueDepth)-1].Value
		if depth > 100000 {
			appendMessage("❌ Logtubed %d 队列过深: %d", i+1, depth)
		}
	}

	// check es health
	for i, url := range options.ESHealthEndpoints {
		// fetch health
		var h ESHealth
		if err = getJSON(url, &h); err != nil {
			appendMessage("❌️ ES %d 无法连接: %s", i+1, err.Error())
			continue
		}
		// check number of nodes
		if h.NumberOfNodes != len(options.ESHealthEndpoints) {
			appendMessage("❌️ ES %d 节点数异常: %d", i+1, h.NumberOfNodes)
		}
	}

	if len(message) > 0 {
		_ = postJSON(options.URL, map[string]interface{}{
			"msgtype": "text",
			"text": map[string]interface{}{
				"content": message,
			},
		})
	}
}
