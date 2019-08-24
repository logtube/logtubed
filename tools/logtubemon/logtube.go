package main

import (
	"fmt"
	"go.guoyk.net/common"
)

type LogtubeMeasure struct {
	name           string
	endpoint       string
	queueThreshold int64
}

type LogtubeStats struct {
	QueueStdDepth int64 `json:"queue-std-depth"`
	QueuePriDepth int64 `json:"queue-pri-depth"`
}

func (m *LogtubeMeasure) Name() string {
	return m.name
}

func (m *LogtubeMeasure) Execute(ret *Results) {
	var err error
	var d LogtubeStats
	if err = common.GetJSON(fmt.Sprintf("http://%s/debug/vars", m.endpoint), &d); err != nil {
		ret.Failed("无法获取调试变量: %s", err.Error())
		return
	}
	okPri := m.queueThreshold <= 0 || d.QueuePriDepth < m.queueThreshold
	ret.Add(okPri, "队列 PRI: %d", d.QueuePriDepth)
	okStd := m.queueThreshold <= 0 || d.QueueStdDepth < m.queueThreshold
	ret.Add(okStd, "队列 STD: %d", d.QueueStdDepth)
	return
}
