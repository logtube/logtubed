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

type LogtubeStats struct {
	QueueStdDepth int64 `json:"queue-std-depth"`
	QueuePriDepth int64 `json:"queue-pri-depth"`
}

type ESHealth struct {
	Status        string `json:"status"`
	NumberOfNodes int    `json:"number_of_nodes"`
}

type Options struct {
	URL                   string   `json:"url"`
	QueueThreshold        int64    `json:"queue_threshold"`
	LogtubeStatsEndpoints []string `json:"logtube_stats_endpoints"`
	ESHealthEndpoints     []string `json:"es_health_endpoints"`
}

type State map[string]bool

var (
	optOptions string
	optVerbose bool

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

func appendVerbose(format string, item ...interface{}) {
	if optVerbose {
		fmt.Printf(format+"\n", item...)
	}
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
	flag.BoolVar(&optVerbose, "v", false, "verbose mode")
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
		} else {
			appendVerbose("✅ Logtubed %d 连接成功", i+1)
		}
		// check queue std depth
		if d.QueuePriDepth > options.QueueThreshold {
			appendMessage("❌ Logtubed %d 高级队列过深: %d", i+1, d.QueuePriDepth)
		} else {
			appendVerbose("✅ Logtubed %d 高级队列深度：%d", i+1, d.QueuePriDepth)
		}
		// check queue pri depth
		if d.QueueStdDepth > options.QueueThreshold {
			appendMessage("❌ Logtubed %d 标准队列过深: %d", i+1, d.QueueStdDepth)
		} else {
			appendVerbose("✅ Logtubed %d 标准队列深度：%d", i+1, d.QueueStdDepth)
		}
	}

	// check es health
	for i, url := range options.ESHealthEndpoints {
		// fetch health
		var h ESHealth
		if err = getJSON(url, &h); err != nil {
			appendMessage("❌️ ES %d 无法连接: %s", i+1, err.Error())
			continue
		} else {
			appendVerbose("✅ ES %d 连接成功", i+1)
		}
		// check number of nodes
		if h.NumberOfNodes != len(options.ESHealthEndpoints) || h.Status == "red" {
			appendVerbose("❌️ ES %d 节点异常：%s(%d)", i+1, h.Status, h.NumberOfNodes)
		} else {
			appendVerbose("✅ ES %d 节点信息：%s(%d)", i+1, h.Status, h.NumberOfNodes)
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
