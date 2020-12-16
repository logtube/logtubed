package core

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	BlockFile        = "/tmp/logtubed.block.txt"
	BlockFileContent = "BLOCK"
)

type Blockable interface {
	SetBlocked(blocked bool)
}

type BlockRoutine interface {
	common.Runnable
}

type BlockRoutineOptions struct {
	Dirs       []string
	Watermarks []int
	Blockables []Blockable
}

type blockRoutine struct {
	dirs       []string
	watermarks []int
	blockables []Blockable
}

func NewBlockRoutine(opts BlockRoutineOptions) BlockRoutine {
	return &blockRoutine{
		dirs:       opts.Dirs,
		watermarks: opts.Watermarks,
		blockables: opts.Blockables,
	}
}

func (b *blockRoutine) Run(ctx context.Context) (err error) {
	tk := time.NewTicker(time.Second * 30)
	for {
		var blocked bool
		// check watermarks
		for i, dir := range b.dirs {
			watermark := b.watermarks[i]
			us, err1 := calculateDirSize(dir)
			if err1 != nil {
				log.Error().Err(err1).Str("dir", dir).Msg("failed to calculate dir size")
				continue
			}
			if us > watermark {
				log.Error().Str("dir", dir).Int("usage", us).Msg("watermark exceeded")
				blocked = true
				break
			}
		}
		// check signal file
		if buf, _ := ioutil.ReadFile(BlockFile); strings.TrimSpace(string(buf)) == BlockFileContent {
			blocked = true
		}
		// apply blocked
		for _, b := range b.blockables {
			b.SetBlocked(blocked)
		}
		// wait 30 seconds or ctx.Done()
		select {
		case <-tk.C:
		case <-ctx.Done():
			return
		}
	}
}

func calculateDirSize(dir string) (gb int, err error) {
	var bytes int64
	if err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		bytes += info.Size()
		return nil
	}); err != nil {
		return
	}
	gb = int(bytes / (1000 * 1000 * 1000))
	return
}
