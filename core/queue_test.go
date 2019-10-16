package core

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

type testOpConsumer struct {
	data chan types.Op
}

func (qt *testOpConsumer) ConsumeOp(op types.Op) error {
	qt.data <- op
	return nil
}

func TestQueue_Run(t *testing.T) {
	const dir = "/tmp/logtube-queue-test"
	if !assert.NoError(
		t,
		os.RemoveAll(dir),
		"should not failed to remove test dir",
	) {
		return
	}

	oc := &testOpConsumer{data: make(chan types.Op, 5)}

	var q Queue
	var err error
	q, err = NewQueue(QueueOptions{Dir: dir, Name: "lt-test", Next: oc})
	if !assert.NoError(t, err, "should not failed to create Queue") {
		return
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	done := make(chan interface{})

	go func() {
		assert.NoError(t, q.Run(ctx), "should not failed to run Queue")
		close(done)
	}()

	time.Sleep(time.Millisecond * 100)

	op := types.Op{Index: "testindex", Body: []byte("helloworld")}

	assert.NoError(t,
		q.ConsumeOp(op),
		"should not failed to ConsumeOp",
	)

	time.Sleep(time.Millisecond * 100)

	ctxCancel()
	<-done

	assert.Equal(t, <-oc.data, op)
}
