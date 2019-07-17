package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/logtube/logtubed/types"
	"go.guoyk.net/sptp"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestSPTPInput_Run(t *testing.T) {
	var err error
	var si SPTPInput

	eo := &testEventConsumer{data: make(chan types.Event, 100)}

	if si, err = NewSPTPInput(SPTPInputOptions{
		Bind: "127.0.0.1:4555",
		Next: eo,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan interface{})

	go func() {
		var err error
		if err = si.Run(ctx); err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	time.Sleep(time.Second)

	var addr *net.UDPAddr
	if addr, err = net.ResolveUDPAddr("udp", "127.0.0.1:4555"); err != nil {
		t.Fatal(err)
	}

	var conn *net.UDPConn
	if conn, err = net.DialUDP("udp", nil, addr); err != nil {
		t.Fatal(err)
	}

	bytes := make([]byte, 8*1024)
	_, _ = rand.Read(bytes)

	msg := make([]byte, 16*1024)
	hex.Encode(msg, bytes)

	msgStr := string(msg)

	w := sptp.NewWriter(conn)

	ce := types.CompactEvent{
		Timestamp: 1,
		Hostname:  "example-2.com",
		Env:       "test-3",
		Project:   "test-4",
		Topic:     "debug-2",
		Crid:      "abcdefg",
		Keyword:   "duck",
		Message:   msgStr,
		Extra: map[string]interface{}{
			"custom_key3": "custom_val3",
		},
	}

	var buf []byte

	if buf, err = json.Marshal(&ce); err != nil {
		t.Fatal(err)
	}

	if _, err = w.Write(buf); err != nil {
		t.Fatal(err)
	}

	e := <-eo.data

	if !reflect.DeepEqual(e, ce.ToEvent()) {
		t.Fatal("failed")
	}

	cancel()
	<-done
}
