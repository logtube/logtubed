package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"go.guoyk.net/sptp"
	"net"
	"reflect"
	"sync"
	"testing"
)

func TestSPTPInput_Run(t *testing.T) {
	var err error
	var input *SPTPInput

	if input, err = NewSPTPInput(SPTPInputOptions{
		Bind: "127.0.0.1:4555",
	}); err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	queue := make(chan Event, 100)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		var err error

		if err = input.Run(queue); err != nil {
			t.Fatal(err)
		}
	}()

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

	ce := CompactEvent{
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

	e := <-queue

	if !reflect.DeepEqual(e, ce.ToEvent()) {
		t.Fatal("failed")
	}

	_ = input.Close()

	wg.Wait()
}
