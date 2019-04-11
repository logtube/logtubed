package main

import (
	"reflect"
	"testing"
)

func TestOperation_GobMarshalUnmarshal(t *testing.T) {
	o := Operation{"123", []byte{0x01, 0x02, 0x03}}

	var err error
	var buf []byte
	if buf, err = o.GobMarshal(); err != nil {
		t.Fatal(err)
	}

	var o2 Operation
	if o2, err = UnmarshalOperationGob(buf); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(&o, &o2) {
		t.Fatal("not equal")
	}
}
