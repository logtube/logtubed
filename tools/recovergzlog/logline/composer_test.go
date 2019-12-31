package logline

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestComposer(t *testing.T) {
	m := NewComposer()
	res := m.Feed("[2020-11/12 10:12:12.555] line 1")
	require.Equal(t, res, "")
	res = m.Feed("line 2")
	require.Equal(t, res, "")
	res = m.Feed("[2020-11/12 10:12:12.555] line 3")
	require.Equal(t, res, "[2020-11/12 10:12:12.555] line 1\nline 2")
	res = m.Feed("[2020-11/12 10:12:12.555] line 4")
	require.Equal(t, res, "[2020-11/12 10:12:12.555] line 3")
	res = m.End()
	require.Equal(t, res, "[2020-11/12 10:12:12.555] line 4")
	res = m.End()
	require.Equal(t, res, "")
}
