package main

import (
	"context"
	"testing"
	"time"
)

type testQueue struct {
	data chan Event
}

func newTestQueue() *testQueue {
	return &testQueue{
		data: make(chan Event, 100),
	}
}

func (q *testQueue) Stats() *Stats {
	return nil
}

func (q *testQueue) PutEvent(e Event) error {
	q.data <- e
	return nil
}

func (q *testQueue) Depth() int64 {
	return int64(len(q.data))
}

func (q *testQueue) Run(ctx context.Context, output Output) error {
	for {
		select {
		case e := <-q.data:
			output.PutOperation(e.ToOperation())
		case <-ctx.Done():
		}
	}
	return nil
}

func TestCloseChan(t *testing.T) {
	c := make(chan bool, 1)

	go func() {
		select {
		case b, ok := <-c:
			t.Log(b, ok)
		}
		select {
		case b, ok := <-c:
			t.Log(b, ok)
		}
		select {
		case b, ok := <-c:
			t.Log(b, ok)
		}
	}()

	c <- true
	close(c)
	time.Sleep(time.Second)
}
