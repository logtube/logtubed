package internal

import (
	"context"
	"github.com/logtube/logtubed/types"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestLocalOutput_Run(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	done := make(chan interface{})

	var o LocalOutput
	var err error

	if !assert.NoError(
		t,
		os.RemoveAll("/tmp/logtubed-local-test"),
		"should not fail on deleting temp dir",
	) {
		return
	}

	o, err = NewLocalOutput(LocalOutputOptions{Dir: "/tmp/logtubed-local-test"})

	if !assert.NoError(t, err, "should not fail on creating LocalOutput") {
		return
	}

	go func() {
		assert.NoError(t, o.Run(ctx), "should not return error")
		close(done)
	}()

	err = o.ConsumeEvent(types.Event{
		Timestamp: time.Unix(1563249593, 0),
		Topic:     "debug",
		Env:       "test",
		Project:   "test",
		Crid:      "aaaa",
		Keyword:   "kkkk",
		Message:   "hello world",
		Extra:     map[string]interface{}{"key1": "val2"},
	})

	if !assert.NoError(t, err, "should not fail on consuming") {
		return
	}

	ctxCancel()
	<-done

	var buf []byte
	buf, err = ioutil.ReadFile("/tmp/logtubed-local-test/debug-test-test-2019-07-16.log")
	if !assert.NoError(t, err, "should not fail on reading output file") {
		return
	}
	assert.Equal(t, "[2019-07-16T11:59:53+08:00] (aaaa) [kkkk] hello world key1 = val2\r\n", string(buf))
}
