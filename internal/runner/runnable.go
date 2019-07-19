package runner

import "context"

// Runnable is similar with Java Runnable
type Runnable interface {
	// Run is the main method, accept a context for cancellation and returns error
	Run(ctx context.Context) error
}

type dummyRunnable struct {
}

func (*dummyRunnable) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func DummyRunnable() Runnable {
	return &dummyRunnable{}
}
