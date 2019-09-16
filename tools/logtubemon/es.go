package main

import (
	"fmt"
	"go.guoyk.net/common"
)

type ESMeasure struct {
	name     string
	endpoint string
	total    int
}

type ESHealth struct {
	Status                 string  `json:"status"`
	NumberOfNodes          int     `json:"number_of_nodes"`
	ActiveShardsPercentage float64 `json:"active_shards_percent_as_number"`
}

func (m *ESMeasure) Name() string {
	return m.name
}

func (m *ESMeasure) Execute(ret *Results) {
	var err error
	var h ESHealth
	if err = common.GetJSON(fmt.Sprintf("http://%s/_cluster/health", m.endpoint), &h); err != nil {
		ret.Failed("无法获取节点信息: %s", err.Error())
		return
	}
	ok := h.NumberOfNodes == m.total && (h.Status != "red" || h.ActiveShardsPercentage > 99)
	ret.Add(ok, "%s, %f%%, 存活节点数 %d", h.Status, h.ActiveShardsPercentage, h.NumberOfNodes)
	return
}
