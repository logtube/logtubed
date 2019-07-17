package types

import (
	"bytes"
	"testing"
)

func Test_OpMarshalUnmarshal(t *testing.T) {
	o1 := Op{"this-is-a-Index", []byte{0x01, 0x02, 0x03}}

	var err error
	var buf []byte
	buf = OpMarshal(o1)

	t.Logf("Marshalled: % 02x ", buf)

	var o2 Op
	if o2, err = OpUnmarshal(buf); err != nil {
		t.Fatal(err)
	}

	if o1.Index != o2.Index || !bytes.Equal(o1.Body, o2.Body) {
		t.Fatal("not equal")
	}
}
