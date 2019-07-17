package runner

import "context"

// Runnable is similar with Java Runnable
type Runnable interface {
	// Run is the main method, accept a context for cancellation and returns error
	Run(ctx context.Context) error
}
