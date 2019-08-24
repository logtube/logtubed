package main

import (
	"fmt"
	"go.guoyk.net/common"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	datePattern = "2006-01-02"
)

type ESDiskMeasure struct {
	name          string
	endpoint      string
	diskThreshold int
	weights       map[string]float64
}

type ESAlloc struct {
	IP          string `json:"ip"`
	Node        string `json:"node"`
	DiskPercent string `json:"disk.percent"`
	DiskUsed    string `json:"disk.used"`
	DiskIndices string `json:"disk.indices"`
}

type ESIndex struct {
	Index     string `json:"index"`
	StoreSize string `json:"store.size"`
}

type ESIndexInfo struct {
	Index       string
	IndexBase   string
	Date        time.Time
	StoreSizeKB int64
}

func (m *ESDiskMeasure) Name() string {
	return m.name
}

func (m *ESDiskMeasure) Execute(ret *Results) {
	var err error
	var as []ESAlloc
	if err = common.GetJSON(fmt.Sprintf("http://%s/_cat/allocation?format=json", m.endpoint), &as); err != nil {
		ret.Failed("无法获取磁盘数据: %s", err.Error())
		return
	}
	cleanNeeded := false
	cleanCapable := true
	for _, a := range as {
		perc, _ := strconv.Atoi(a.DiskPercent)
		ok := m.diskThreshold <= 0 || perc < m.diskThreshold
		ret.Add(ok, "%s: 磁盘用量 %d%%", a.Node, perc)

		if !ok {
			cleanNeeded = true
		}

		// check cleanCapable
		var diskIndices common.Capacity
		var diskUsed common.Capacity
		if diskIndices, err = common.ParseCapacity(a.DiskIndices); err != nil {
			ret.Failed("%s: 无法解析 disk.indices: %s", a.Node, err.Error())
			cleanCapable = false
			continue
		}
		if diskUsed, err = common.ParseCapacity(a.DiskUsed); err != nil {
			ret.Failed("%s: 无法解析 disk.used: %s", a.Node, err.Error())
			cleanCapable = false
			continue
		}
		if diskUsed < diskIndices || (diskUsed-diskIndices)*3 > diskIndices {
			ret.Failed("%s: ES 用量 %s / 磁盘用量 %s，清理 ES 无法有效释放空间", a.Node, a.DiskIndices, a.DiskUsed)
			cleanCapable = false
			continue
		}
	}

	if !cleanNeeded || !cleanCapable {
		return
	}

	var indices []ESIndex
	if err = common.GetJSON(fmt.Sprintf("http://%s/_cat/indices?format=json", m.endpoint), &indices); err != nil {
		ret.Failed("无法获取索引数据: %s", err.Error())
		return
	}
	var infos []ESIndexInfo
	if infos, err = m.ConvertIndexInfos(indices); err != nil {
		ret.Failed("无法解析索引用量: %s", err.Error())
		return
	}
	// calculate total kb
	var totalKB int64
	for _, idx := range infos {
		totalKB += idx.StoreSizeKB
	}

	// collect indices to delete
	var countKB int64
	script := ESCleanScript{Endpoint: m.endpoint, TotalGB: totalKB / 1000000}
	for _, info := range infos {
		script.Indices = append(script.Indices, info.Index)
		countKB += info.StoreSizeKB
		if countKB >= totalKB/10 {
			break
		}
	}
	script.TargetGB = countKB / 1000000

	// write delete script
	if err = script.WriteTo("/tmp/clean-es-disk.sh"); err != nil {
		ret.Failed("无法写入清理脚本: %s", err.Error())
		return
	}
	ret.Failed("清理脚本 /tmp/clean-es-disk.sh 已生成，预计释放 %d 个索引，%d Gb 空间", len(script.Indices), countKB/1000000)
	return
}

func (m *ESDiskMeasure) ConvertIndexInfos(indices []ESIndex) (infos []ESIndexInfo, err error) {
	for _, idx := range indices {
		// skip system indices
		if strings.HasPrefix(idx.Index, ".") {
			continue
		}
		// check tailing 2019-08-12
		if len(idx.Index) < 10 {
			continue
		}
		indexBase := idx.Index[:len(idx.Index)-10]
		dateSuffix := idx.Index[len(idx.Index)-10:]
		var date time.Time
		if date, err = time.Parse(datePattern, dateSuffix); err != nil {
			err = nil
			continue
		}
		// decode capacity
		var size common.Capacity
		if size, err = common.ParseCapacity(idx.StoreSize); err != nil {
			return
		}
		infos = append(infos, ESIndexInfo{
			Index:       idx.Index,
			IndexBase:   indexBase,
			Date:        date,
			StoreSizeKB: int64(size / common.Kilobyte),
		})
	}
	now := time.Now()
	sort.Slice(infos, func(i, j int) bool {
		a, b := infos[i], infos[j]
		// calculate the weight
		return m.CalculateWeight(now, a) > m.CalculateWeight(now, b)
	})
	sort.SliceStable(infos, func(i, j int) bool {
		a, b := infos[i], infos[j]
		// older is always prior for index with same base name
		if a.IndexBase == b.IndexBase {
			return a.Date.Before(b.Date)
		}
		return false
	})
	return
}

func (m *ESDiskMeasure) CalculateWeight(now time.Time, i ESIndexInfo) float64 {
	factor := float64(1)
	for k, v := range m.weights {
		if strings.Contains(i.Index, k) {
			factor = v
			break
		}
	}
	weeks := float64(now.Sub(i.Date)/(time.Hour*24)) / 7
	if weeks <= 0 {
		weeks = 1
	} else if weeks > 24 {
		weeks = 24
	}
	x := (weeks - 1) / 2
	return factor * (x*x*x + 1) * float64(i.StoreSizeKB)
}
