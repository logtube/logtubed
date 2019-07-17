package internal

import (
	"github.com/logtube/logtubed/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testEventConsumer struct {
	data chan types.Event
}

func (t *testEventConsumer) ConsumeEvent(e types.Event) error {
	t.data <- e
	return nil
}

func TestDispatcher_ConsumeEvent(t *testing.T) {
	std := &testOpConsumer{data: make(chan types.Op, 10)}
	pri := &testOpConsumer{data: make(chan types.Op, 10)}
	nxt := &testEventConsumer{data: make(chan types.Event, 10)}

	var d types.EventConsumer
	var err error
	d, err = NewDispatcher(DispatcherOptions{
		Ignores:  []string{"ignore"},
		Keywords: []string{"keyword", "prior"},
		Priors:   []string{"prior"},
		Hostname: "test-host",
		Next:     nxt,
		NextStd:  std,
		NextPri:  pri,
	})

	assert.NoError(t, err, "should not failed to create dispatcher")

	err = d.ConsumeEvent(types.Event{
		Topic:   "ignore",
		Message: "hello, world",
	})
	assert.NoError(t, err, "should not failed to consume error")
	assert.Zero(t, len(std.data)+len(pri.data)+len(nxt.data), "should be totally ignored")

	err = d.ConsumeEvent(types.Event{
		Topic:   "keyword",
		Message: "hello, world",
	})
	assert.NoError(t, err, "should not failed to consume error")
	assert.Zero(t, len(std.data)+len(pri.data)+len(nxt.data), "should be totally ignored")

	err = d.ConsumeEvent(types.Event{
		Topic:   "keyword",
		Keyword: "keyword",
		Message: "hello, world",
	})
	assert.NoError(t, err, "should not failed to consume error")
	assert.Equal(t, 1, len(std.data), "should append to std")
	assert.Equal(t, 1, len(nxt.data), "should append to nxt")
	assert.Equal(t, 0, len(pri.data), "should not append to pri")

	err = d.ConsumeEvent(types.Event{
		Topic:   "prior",
		Keyword: "keyword",
		Message: "hello, world",
	})
	assert.NoError(t, err, "should not failed to consume error")
	assert.Equal(t, 1, len(std.data), "should not append to std")
	assert.Equal(t, 2, len(nxt.data), "should append to nxt")
	assert.Equal(t, 1, len(pri.data), "should append to pri")

	e := <-nxt.data
	assert.Equal(t, "test-host", e.Via)
}
