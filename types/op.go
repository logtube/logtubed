package types

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Op memory layout

// 1. 2-bytes, 0xAC, 0xCF,
// 2. 2-bytes, Index length, uint16 (BE)
// 3. N-bytes, Index
// 4. 4-bytes, Body length, uint32 (BE)
// 5. N-bytes, Body

var (
	ErrInvalidFormat = errors.New("invalid format of Op")
)

var (
	opMagicBytes = []byte{0xAC, 0xCF}
)

type Op struct {
	Index string
	Body  []byte
}

// OpConsumer consumes Ops
type OpConsumer interface {
	// add a Op to the consumer
	ConsumeOp(op Op) error
}

func OpMarshal(o Op) (ret []byte) {
	index := []byte(o.Index)
	total := 2 + 2 + len(index) + 4 + len(o.Body)

	ret = make([]byte, total, total)
	i := 0

	copy(ret, opMagicBytes)
	i += 2

	binary.BigEndian.PutUint16(ret[i:], uint16(len(index)))
	i += 2

	copy(ret[i:], index)
	i += len(index)

	binary.BigEndian.PutUint32(ret[i:], uint32(len(o.Body)))
	i += 4

	copy(ret[i:], o.Body)
	return
}

func OpUnmarshal(b []byte) (ret Op, err error) {
	// check format and magic bytes
	if len(b) < 10 || !bytes.Equal(opMagicBytes, b[0:2]) {
		err = ErrInvalidFormat
		return
	}

	indexLen := int(binary.BigEndian.Uint16(b[2:4]))
	ret.Index = string(b[4 : 4+indexLen])
	bodyLen := int(binary.BigEndian.Uint32(b[4+indexLen:]))
	ret.Body = b[8+indexLen : 8+indexLen+bodyLen]
	return
}
