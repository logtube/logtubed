package main

import (
	"flag"
	"fmt"
	"go.guoyk.net/common"
	"log"
	"os"
	"strconv"
)

type M map[string]interface{}

type LogtubeStats struct {
	QueueStdDepth int64 `json:"queue-std-depth"`
	QueuePriDepth int64 `json:"queue-pri-depth"`
}

type ESHealth struct {
	Status        string `json:"status"`
	NumberOfNodes int    `json:"number_of_nodes"`
}

type ESAlloc struct {
	Node        string `json:"node"`
	DiskPercent string `json:"disk.percent"`
}

type Options struct {
	URL                   string   `json:"url"`
	ESAllocEndpoint       string   `json:"es_alloc_endpoint"`
	QueueThreshold        int64    `json:"queue_threshold"`
	LogtubeStatsEndpoints []string `json:"logtube_stats_endpoints"`
	ESHealthEndpoints     []string `json:"es_health_endpoints"`
}

var (
	optOptions string
	optVerbose bool

	options Options

	content string
)

func appendAlert(format string, item ...interface{}) {
	if len(content) > 0 {
		content = content + "\n"
	}
	content = content + fmt.Sprintf(format, item...)
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
	if err = common.LoadJSONConfigFile(optOptions, &options); err != nil {
		return
	}

	// check logtubed status
	for i, url := range options.LogtubeStatsEndpoints {
		// fetch stats
		var d LogtubeStats
		if err = common.GetJSON(url, &d); err != nil {
			appendAlert("❌ Logtubed %d 无法监控: %s", i+1, err.Error())
			continue
		} else {
			appendVerbose("✅ Logtubed %d 连接成功", i+1)
		}
		// check queue std depth
		if d.QueuePriDepth > options.QueueThreshold {
			appendAlert("❌ Logtubed %d 高级队列过深: %d", i+1, d.QueuePriDepth)
		} else {
			appendVerbose("✅ Logtubed %d 高级队列深度：%d", i+1, d.QueuePriDepth)
		}
		// check queue pri depth
		if d.QueueStdDepth > options.QueueThreshold {
			appendAlert("❌ Logtubed %d 标准队列过深: %d", i+1, d.QueueStdDepth)
		} else {
			appendVerbose("✅ Logtubed %d 标准队列深度：%d", i+1, d.QueueStdDepth)
		}
	}

	// check es health
	for i, url := range options.ESHealthEndpoints {
		// fetch health
		var h ESHealth
		if err = common.GetJSON(url, &h); err != nil {
			appendAlert("❌️ ES %d 无法连接: %s", i+1, err.Error())
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

	// check es allocation
	if len(options.ESAllocEndpoint) > 0 {
		var as []ESAlloc
		if err = common.GetJSON(options.ESAllocEndpoint, &as); err != nil {
			appendVerbose("❌️ 无法查询 ES 磁盘信息：%s", err.Error())
		} else {
			appendVerbose("✅ ES 磁盘信息已获取，%+v", as)
		}

		for _, a := range as {
			v, _ := strconv.Atoi(a.DiskPercent)
			if v > 85 {
				appendAlert("❌️ ES 磁盘即将用尽：%s = %s%%", a.Node, a.DiskPercent)
			} else {
				appendVerbose("✅️ ES 磁盘状态：%s = %s%%", a.Node, a.DiskPercent)
			}
		}
	}

	if len(content) > 0 {
		_ = common.PostJSON(options.URL, M{"msgtype": "text", "text": M{"content": content}}, nil)
	}
}
