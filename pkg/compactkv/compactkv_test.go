package compactkv

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompactKV_Parse(t *testing.T) {
	ckv := NewCompactKV()
	ckv.Add("r", "request", StringType)
	ckv.Add("s", "status", IntegerType)
	ckv.Add("rt", "request_time", IntegerType)
	ckv.Add("ra", "remote_addr", StringType)
	ckv.Add("ft", "float_test", FloatType)

	m := ckv.Parse(`  r = hello|  s = 300|  rt= 34| ra = world | zz=145 | | ft = 0.22`)
	require.Equal(t, "hello", m["request"])
	require.Equal(t, int64(300), m["status"])
	require.Equal(t, int64(34), m["request_time"])
	require.Equal(t, "world", m["remote_addr"])
	require.Equal(t, "145", m["zz"])
	require.Equal(t, float64(0.22), m["float_test"])
}
