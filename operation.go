package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
)

// operation memory layout

// 1. 2-bytes, 0xAA, 0xFF,
// 2. 2-bytes, index length, uint16 (BE)
// 3. N-bytes, index
// 4. 4-bytes, body length, uint32 (BE)
// 5. N-bytes, body

var (
	magicBytes = []byte{0xAC, 0xCF}
)

// Operation marshaled record
type Operation struct {
	Index string `json:"index"`
	Body  []byte `json:"body"`
}

func (o Operation) DiskQueueMarshal() (ret []byte, err error) {
	index := []byte(o.Index)
	total := 2 + 2 + len(index) + 4 + len(o.Body)

	ret = make([]byte, total, total)
	i := 0

	copy(ret, magicBytes)
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

func DiskQueueUnmarshal(b []byte) (o Operation, err error) {
	// new format
	if len(b) > 10 && bytes.Equal(magicBytes, b[0:2]) {
		indexLen := int(binary.BigEndian.Uint16(b[2:4]))
		o.Index = string(b[4 : 4+indexLen])
		bodyLen := int(binary.BigEndian.Uint32(b[4+indexLen:]))
		o.Body = b[8+indexLen : 8+indexLen+bodyLen]
		return
	}

	// old gob format
	if err = gob.NewDecoder(bytes.NewReader(b)).Decode(&o); err != nil {
		return
	}
	if len(o.Body) == 0 || len(o.Index) == 0 {
		err = errors.New("empty index or empty body")
		return
	}
	return
}
