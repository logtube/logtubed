package main

import (
	"reflect"
	"testing"
)

func TestOperation_DiskQueueMarshalUnmarshal(t *testing.T) {
	o := Operation{"this-is-a-index", []byte{0x01, 0x02, 0x03}}

	var err error
	var buf []byte
	if buf, err = o.DiskQueueMarshal(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Marshalled: % 02x ", buf)

	var o2 Operation
	if o2, err = DiskQueueUnmarshal(buf); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(&o, &o2) {
		t.Fatal("not equal")
	}
}
