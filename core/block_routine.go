package core

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"time"
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
			du := common.NewDiskUsage(dir)
			us := int(du.Usage() * 100)
			if us >= watermark {
				log.Error().Str("dir", dir).Int("usage", us).Msg("watermark exceeded")
				blocked = true
				break
			}
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
