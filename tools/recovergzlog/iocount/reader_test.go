package iocount

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestNewReader(t *testing.T) {
	b := []byte("12435341q53254123")
	r := bytes.NewReader(b)
	cr := NewReader(r)
	_, _ = ioutil.ReadAll(cr)
	require.Equal(t, int64(len(b)), cr.Count())
}
