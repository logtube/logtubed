package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLinePattern(t *testing.T) {
	require.True(t, linePattern.MatchString(`[2019-12-12 19:19:19`))
	require.True(t, linePattern.MatchString(`[2019/12/12 19:19:19`))
	require.True(t, linePattern.MatchString(`[2019-12/12 19:19:19`))
}
