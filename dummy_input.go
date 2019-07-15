package main

import "context"

type DummyInput struct{}

func (DummyInput) Run(ctx context.Context, cancelFunc context.CancelFunc, output Queue) error {
	<-ctx.Done()
	return nil
}
