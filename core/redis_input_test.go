package core

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/logtube/logtubed/types"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRedisInput_Run(t *testing.T) {
	var err error
	var ri RedisInput

	eo := &testEventConsumer{data: make(chan types.Event, 5)}

	if ri, err = NewRedisInput(RedisInputOptions{
		Bind:              "127.0.0.1:4589",
		Multi:             true,
		LogtubeTimeOffset: -2,
		Next:              eo,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan interface{})

	go func() {
		var err error
		if err = ri.Run(ctx); err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	time.Sleep(time.Second)

	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:4589",
	})

	info := client.Info().String()
	if !strings.Contains(info, "redis_version:2.4") {
		t.Fatal("not declare as 2.4+", info)
	}

	if err = client.RPush(
		"anykey1",
		// v1 plain message
		`{"beat":{"hostname":"example-1.com"},"source":"/var/log/test/debug/test.2019-01-02.log","message":"[2019/01/02 03:04:05.666] CRID[abcdefg] K[hello] hello, world KW[world]"}`,
		// v1 JSON message
		`{"beat":{"hostname":"example-1.com"},"source":"/var/log/test/_json_/test.2019-01-02.log","message":"[2019/01/02 03:04:05.666] {\"topic\":\"debug-1\",\"custom_key1\":\"custom_val1\"}"}`,
		// v1 JSON message with overrides
		`{"beat":{"hostname":"example-1.com"},"source":"/var/log/test/_json_/test.2019-01-02.log","message":"[2019/01/02 03:04:05.666] {\"topic\":\"debug-1\",\"custom_key1\":\"custom_val1\",\"project\":\"test-1\",\"timestamp\":\"2019-01-02T03:04:05.666Z\",\"crid\":\"abcdefg\"}"}`,
		// v2 plain message
		`{"beat":{"hostname":"example-1.com"},"source":"/var/log/test/debug-2/test.2019-01-02.log","message":"[2019-01-02 03:04:05.666 +0200] CRID[abcdefg] K[hello] hello, world KW[hello,world]"}`,
		// v2 JSON message
		`{"beat":{"hostname":"example-1.com"},"source":"/var/log/test/debug-2/test.2019-01-02.log","message":"[2019-01-02 03:04:05.666 +0200] {\"c\":\"abcdefg\",\"k\":\"hello,hello,world\",\"x\":{\"custom_key2\":\"custom_val2\"},\"m\":\"hello, world\"}"}`,
	).Err(); err != nil {
		t.Fatal(err)
	}

	var e types.Event
	var e1 types.Event

	e = <-eo.data

	e1 = types.Event{
		Timestamp: time.Date(2019, 1, 2, 1, 4, 5, int(666*time.Millisecond), time.UTC),
		Hostname:  "example-1.com",
		Env:       "test",
		Project:   "test",
		Topic:     "debug",
		Crid:      "abcdefg",
		Keyword:   "hello,world",
		Message:   "CRID[abcdefg] K[hello] hello, world KW[world]",
	}

	if !e.Timestamp.Equal(e1.Timestamp) {
		t.Fatal("timestamp not equal")
	}

	e.Timestamp = time.Time{}
	e1.Timestamp = time.Time{}
	e.Extra = nil
	e1.Extra = nil

	if !reflect.DeepEqual(e, e1) {
		t.Fatal("not equal \n", e, "\n", e1)
	}

	e = <-eo.data

	e1 = types.Event{
		Timestamp: time.Date(2019, 1, 2, 1, 4, 5, int(666*time.Millisecond), time.UTC),
		Hostname:  "example-1.com",
		Env:       "test",
		Project:   "test",
		Topic:     "debug-1",
		Extra: map[string]interface{}{
			"custom_key1": "custom_val1",
		},
	}

	if !e.Timestamp.Equal(e1.Timestamp) {
		t.Fatal("timestamp not equal")
	}

	e.Timestamp = time.Time{}
	e1.Timestamp = time.Time{}

	if !reflect.DeepEqual(e, e1) {
		t.Fatal("not equal \n", e, "\n", e1)
	}

	e = <-eo.data

	e1 = types.Event{
		Timestamp: time.Date(2019, 1, 2, 3, 4, 5, int(666*time.Millisecond), time.UTC),
		Hostname:  "example-1.com",
		Env:       "test",
		Project:   "test-1",
		Topic:     "debug-1",
		Crid:      "abcdefg",
		Extra: map[string]interface{}{
			"custom_key1": "custom_val1",
		},
	}

	if !e.Timestamp.Equal(e1.Timestamp) {
		t.Fatal("timestamp not equal")
	}

	e.Timestamp = time.Time{}
	e1.Timestamp = time.Time{}

	if !reflect.DeepEqual(e, e1) {
		t.Fatal("not equal \n", e, "\n", e1)
	}

	e = <-eo.data

	e1 = types.Event{
		Timestamp: time.Date(2019, 1, 2, 1, 4, 5, int(666*time.Millisecond), time.UTC),
		Hostname:  "example-1.com",
		Env:       "test",
		Project:   "test",
		Topic:     "debug-2",
		Crid:      "abcdefg",
		Keyword:   "hello,hello,world",
		Message:   "CRID[abcdefg] K[hello] hello, world KW[hello,world]",
		Extra: map[string]interface{}{
			"custom_key1": "custom_val1",
		},
	}

	if !e.Timestamp.Equal(e1.Timestamp) {
		t.Fatal("timestamp not equal")
	}

	e.Timestamp = time.Time{}
	e1.Timestamp = time.Time{}
	e.Extra = nil
	e1.Extra = nil

	if !reflect.DeepEqual(e, e1) {
		t.Fatal("not equal \n", e, "\n", e1)
	}

	e = <-eo.data

	e1 = types.Event{
		Timestamp: time.Date(2019, 1, 2, 1, 4, 5, int(666*time.Millisecond), time.UTC),
		Hostname:  "example-1.com",
		Env:       "test",
		Project:   "test",
		Topic:     "debug-2",
		Crid:      "abcdefg",
		Keyword:   "hello,hello,world",
		Message:   "hello, world",
		Extra: map[string]interface{}{
			"custom_key2": "custom_val2",
		},
	}

	if !e.Timestamp.Equal(e1.Timestamp) {
		t.Fatal("timestamp not equal")
	}

	e.Timestamp = time.Time{}
	e1.Timestamp = time.Time{}

	if !reflect.DeepEqual(e, e1) {
		t.Fatal("not equal \n", e, "\n", e1)
	}

	cancel()
	<-done
}
