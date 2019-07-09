package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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
	optState   string

	options Options
	state   State

	changed bool
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
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	err = json.Unmarshal(buf, out)
	return
}

func saveJSON(file string, in interface{}) (err error) {
	var buf []byte
	if buf, err = json.Marshal(in); err != nil {
		return
	}
	err = ioutil.WriteFile(file, buf, 0640)
	return
}

func raiseAlert(id string, m string) {
	a := state[id]

	// check and update
	if !a {
		changed = true
		state[id] = true
	}

	// append final message
	if len(message) > 0 {
		message = message + "\n"
	}
	message = message + m
}

func clearAlert(id string, m string) {
	a := state[id]

	// check and update
	if a {
		changed = true
		state[id] = false
	}

	// append final message
	if len(message) > 0 {
		message = message + "\n"
	}
	message = message + m
}

func main() {
	var err error

	flag.StringVar(&optOptions, "c", "/etc/logtubemon.json", "config file for logtubemon")
	flag.StringVar(&optState, "s", "/var/lib/logtubemon.state", "state file for logtubemon")
	flag.Parse()

	// load config and state
	if err = loadJSON(optOptions, &options); err != nil {
		return
	}
	if err = loadJSON(optState, &state); err != nil {
		return
	}

	if state == nil {
		state = State{}
	}
	defer saveJSON(optState, state)

	var all []LogtubeStats

	// check logtubed status
	for i, url := range options.LogtubeStatsEndpoints {
		// fetch stats
		var d LogtubeStats
		if err = getJSON(url, &d); err != nil {
			raiseAlert(fmt.Sprintf("logtube-connect-%d", i), fmt.Sprintf("⛔️ Logtubed %d 连接性: %s", i, err.Error()))
			continue
		} else {
			clearAlert(fmt.Sprintf("logtube-connect-%d", i), fmt.Sprintf("✅ Logtubed %d 连接性", i))
		}
		//save
		all = append(all, d)
		if len(d.QueueDepth) < 1 {
			continue
		}
		// check queue depth
		depth := d.QueueDepth[len(d.QueueDepth)-1].Value
		if depth < 10000 {
			raiseAlert(fmt.Sprintf("logtube-queue-%d", i), fmt.Sprintf("⛔️ Logtubed %d 队列深度: %d", i, depth))
		} else {
			clearAlert(fmt.Sprintf("logtube-queue-%d", i), fmt.Sprintf("✅ Logtubed %d 队列深度: %d", i, depth))
		}
	}

	// check es
	for i, url := range options.ESHealthEndpoints {
		// fetch health
		var h ESHealth
		if err = getJSON(url, &h); err != nil {
			raiseAlert(fmt.Sprintf("es-connect-%d", i), fmt.Sprintf("⛔️ ES %d 连接性: %s", i, err.Error()))
			continue
		} else {
			clearAlert(fmt.Sprintf("es-connect-%d", i), fmt.Sprintf("✅ ES %d 连接性", i))
		}
		// check number of nodes
		if h.NumberOfNodes != len(options.ESHealthEndpoints) {
			raiseAlert(fmt.Sprintf("es-nodes-%d", i), fmt.Sprintf("⛔️ ES %d 报告节点数: %d", i, h.NumberOfNodes))
		} else {
			clearAlert(fmt.Sprintf("es-nodes-%d", i), fmt.Sprintf("✅ ES %d 报告节点数: %d", i, h.NumberOfNodes))
		}
	}

	if changed {
		_ = postJSON(options.URL, map[string]interface{}{
			"msgtype": "text",
			"text": map[string]interface{}{
				"content": message,
			},
		})
	}
}
