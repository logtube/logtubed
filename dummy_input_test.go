package main

import (
	"context"
	"testing"
	"time"
)

func TestDummyInput(t *testing.T) {
	i := DummyInput{}
	ctx, cancel := context.WithCancel(context.Background())
	var done bool
	dch := make(chan interface{})
	go func() {
		i.Run(ctx, cancel, nil)
		done = true
		close(dch)
	}()
	time.Sleep(time.Second)
	cancel()
	<-dch
	if !done {
		t.Fatal("failed")
	}
}
