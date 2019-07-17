package internal

import "testing"

func TestElasticOutput_Run(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2
	close(ch)

	t.Log(<-ch)
	t.Log(<-ch)
	t.Log(<-ch)
}
