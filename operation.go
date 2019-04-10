package main

import (
	"bytes"
	"encoding/gob"
	"errors"
)

// Operation marshaled record
type Operation struct {
	Index string `json:"index"`
	Body  []byte `json:"body"`
}

func (o Operation) GobMarshal() (ret []byte, err error) {
	// encode operation
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(o); err != nil {
		return
	}
	ret = buf.Bytes()
	return
}

func UnmarshalOperationGob(b []byte) (o Operation, err error) {
	if err = gob.NewDecoder(bytes.NewReader(b)).Decode(&o); err != nil {
		return
	}
	if len(o.Body) == 0 || len(o.Index) == 0 {
		err = errors.New("empty index or empty body")
		return
	}
	return
}
