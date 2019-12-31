package iocount

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSimpleFormatByteSize(t *testing.T) {
	require.Equal(t, "10.25gb", SimpleFormatByteSize(10254776345))
	require.Equal(t, "10.20gb", SimpleFormatByteSize(10200776345))
	require.Equal(t, "10.25mb", SimpleFormatByteSize(10254776))
	require.Equal(t, "10.20mb", SimpleFormatByteSize(10200776))
	require.Equal(t, "10.25kb", SimpleFormatByteSize(10254))
	require.Equal(t, "10.20kb", SimpleFormatByteSize(10200))
	require.Equal(t, "12b", SimpleFormatByteSize(12))
}
