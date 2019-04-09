package main

import (
	"github.com/pkg/errors"
	"strconv"
	"testing"
	"time"
)

type testInput struct {
	id int
	d  time.Duration
	c  int
	s  bool
}

func (t *testInput) Close() (err error) {
	t.s = true
	err = errors.New("dummy")
	return
}

func (t *testInput) Run(queue chan Event) (err error) {
	for i := 0; i < t.c; i++ {
		time.Sleep(t.d)
		queue <- Event{Timestamp: time.Now(), Hostname: strconv.Itoa(t.id)}
	}
	return
}

func TestMultiInput(t *testing.T) {
	i1 := &testInput{d: time.Second / 2, c: 3, id: 1}
	i2 := &testInput{d: time.Second / 4, c: 4, id: 2}
	input := MultiInput(i1, i2)
	out := make(chan Event, 100)
	go func() {
		time.Sleep(time.Second * 4)
		input.Close()
		close(out)
	}()
	go func() {
		input.Run(out)
	}()
	c1 := 0
	c2 := 0
	for e := range out {
		if e.Hostname == "1" {
			c1++
		}
		if e.Hostname == "2" {
			c2++
		}
	}
	if c1 != 3 {
		t.Fatal("count from i1 not 3")
	}
	if c2 != 4 {
		t.Fatal("count from i2 not 4")
	}
	if !i1.s {
		t.Fatal("i1 not closed")
	}
	if !i2.s {
		t.Fatal("i2 not closed")
	}
}
