package main

import (
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestConf_FindRule(t *testing.T) {
	prefixes := []string{"hello", "hello-world", "world", "world-hello"}
	sort.Sort(sort.Reverse(sort.StringSlice(prefixes)))
	require.Equal(t, []string{"world-hello", "world", "hello-world", "hello"}, prefixes)

	conf := &Conf{
		Rules: map[string]Rule{
			"hello-": {
				Cold: 1,
			},
			"hello-world-": {
				Cold: 2,
			},
			"world-": {
				Cold: 3,
			},
			"world-hello-": {
				Cold: 4,
			},
		},
	}
	r, o := conf.FindRule("hello-world-1")
	require.True(t, o)
	require.Equal(t, int64(2), r.Cold)

	r, o = conf.FindRule("world-hello-1")
	require.True(t, o)
	require.Equal(t, int64(4), r.Cold)
}
