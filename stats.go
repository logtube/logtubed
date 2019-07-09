package main

import (
	"container/ring"
	"encoding/json"
	"fmt"
	"go.guoyk.net/diskqueue"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type Stats struct {
	queue diskqueue.DiskQueue

	dataIn    *ring.Ring // input events count for last 30min, in 10 seconds precision
	dataOut   *ring.Ring // output events count for last 30min, in 10 seconds precision
	dataDepth *ring.Ring // depth of diskqueue for last 30min, in 10 seconds precision
}

func NewStats(queue diskqueue.DiskQueue) (s *Stats) {
	s = &Stats{}
	s.queue = queue
	s.dataIn = ring.New(30 * 6) // 30min in 10sec precision
	s.dataIn = initStatsRing(s.dataIn)
	s.dataOut = ring.New(30 * 6) // 30min in 10sec precision
	s.dataOut = initStatsRing(s.dataOut)
	s.dataDepth = ring.New(30 * 6) // 30min in 10sec precision
	s.dataDepth = initStatsRing(s.dataDepth)
	return
}

func formatStatsTime(t int) string {
	min := t < 0
	if t < 0 {
		t = -t
	}
	out := fmt.Sprintf("%02d:%02d", t/60, t%60)
	if min {
		out = "-" + out
	}
	return out
}

func initStatsRing(r *ring.Ring) *ring.Ring {
	n := r.Len()
	for i := 0; i < n; i++ {
		r.Value = new(uint64)
		r = r.Next()
	}
	return r
}

func sumStatsRing(r *ring.Ring) (out []map[string]interface{}) {
	n := r.Len()
	if n < 1 {
		return
	}
	// collect data, current data is skipped
	for i := 1; i < n; i++ {
		out = append(out, map[string]interface{}{
			"time":  formatStatsTime(-10 * i),
			"value": atomic.LoadUint64(r.Value.(*uint64)),
		})
		r = r.Prev()
	}
	// reverse
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return
}

func (s *Stats) Routine() {
	for {
		// 不直接使用 time.Sleep(10*time.Second)，防止误差放大
		// duration to next 10 second border
		d := time.Second * time.Duration(10-(time.Now().Unix()%10))
		// wait to next 10 seconds border
		time.Sleep(d)
		// update dataDepth
		atomic.StoreUint64(s.dataDepth.Value.(*uint64), uint64(s.queue.Depth()))
		// rotate the ring
		s.Rotate()
	}
}

func (s *Stats) Rotate() {
	s.dataIn = s.dataIn.Next()
	atomic.StoreUint64(s.dataIn.Value.(*uint64), 0)
	s.dataOut = s.dataOut.Next()
	atomic.StoreUint64(s.dataOut.Value.(*uint64), 0)
	s.dataDepth = s.dataDepth.Next()
	atomic.StoreUint64(s.dataDepth.Value.(*uint64), 0)
}

func (s *Stats) IncrQueueIn() {
	atomic.AddUint64(s.dataIn.Value.(*uint64), 1)
}

func (s *Stats) IncrQueueOut() {
	atomic.AddUint64(s.dataOut.Value.(*uint64), 1)
}

func (s *Stats) Handler(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"queue_in":    sumStatsRing(s.dataIn),
		"queue_out":   sumStatsRing(s.dataOut),
		"queue_depth": sumStatsRing(s.dataDepth),
	}
	var buf []byte
	var err error
	if buf, err = json.Marshal(data); err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf)
}
