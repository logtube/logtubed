package common

import "context"

var (
	// DummyRunnable dummy runnable implements, does nothing but waits for ctx.Done()
	DummyRunnable Runnable = &dummyRunnable{}
)

// Runnable is similar with Java Runnable
type Runnable interface {
	// Run is the main method, accept a context for cancellation and returns error
	Run(ctx context.Context) error
}

type dummyRunnable struct{}

func (*dummyRunnable) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
