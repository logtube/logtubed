package core

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func Test_calculateDirSize(t *testing.T) {
	home, _ := os.UserHomeDir()
	size, err := calculateDirSize(filepath.Join(home, ".config"))
	assert.NoError(t, err)
	assert.NotZero(t, size)
}
