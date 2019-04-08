package main

import (
	"fmt"
	"time"
)

// Stats daemon stats record
type Stats struct {
	Timestamp     time.Time
	Hostname      string `json:"hostname"`
	RecordsQueued int64  `json:"records_queued"`
	RecordsTotal  int64  `json:"records_total"`
	Records1M     int64  `json:"records_1m"`
}

func (r Stats) Index() string {
	return fmt.Sprintf("x-xlogd-%04d-%02d-%02d", r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}
