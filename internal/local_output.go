package internal

import (
	"context"
	"fmt"
	"github.com/logtube/logtubed/internal/errutil"
	"github.com/logtube/logtubed/internal/runner"
	"github.com/logtube/logtubed/types"
	"errors"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"path/filepath"
	"time"
)

type LocalOutputOptions struct {
	Dir string
}

type LocalOutput interface {
	runner.Runnable
	types.EventConsumer
}

// LocalOutput implements EventConsumer and Runnable
type localOutput struct {
	optDir string

	fs map[string]*os.File

	ch chan types.Event
}

func NewLocalOutput(opts LocalOutputOptions) (LocalOutput, error) {
	if len(opts.Dir) == 0 {
		return nil, errors.New("LocalOutput: Dir is not set")
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}
	lo := &localOutput{
		optDir: opts.Dir,
		fs:     make(map[string]*os.File),
		ch:     make(chan types.Event),
	}
	return lo, nil
}

func localOutputSerialize(e types.Event) string {
	var x string
	for k, v := range e.Extra {
		if len(x) > 0 {
			x = x + ", "
		}
		x = x + fmt.Sprintf("%s = %s", k, v)
	}
	if len(x) > 0 {
		x = " " + x
	}
	return fmt.Sprintf(
		"[%s] (%s) [%s] %s%s\r\n",
		e.Timestamp.Format(time.RFC3339),
		e.Crid,
		e.Keyword,
		e.Message,
		x,
	)
}

func (l *localOutput) takeFile(index string) (f *os.File, err error) {
	// close all if opened too much
	if len(l.fs) > 2000 {
		if err = l.closeFiles(); err != nil {
			return
		}
	}

	// find
	if f = l.fs[index]; f != nil {
		return
	}

	// create
	if f, err = os.OpenFile(
		filepath.Join(l.optDir, index+".log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	); err != nil {
		return
	}

	log.Debug().Str("output", "local").Str("file", f.Name()).Msg("file opened")

	// set
	l.fs[index] = f

	return
}

func (l *localOutput) closeFiles() error {
	eg := errutil.UnsafeGroup()
	for _, f := range l.fs {
		log.Debug().Str("output", "local").Str("file", f.Name()).Msg("file closed")
		eg.Add(f.Close())
	}
	l.fs = map[string]*os.File{}
	return eg.Err()
}

func (l *localOutput) ConsumeEvent(e types.Event) error {
	l.ch <- e
	return nil
}

func (l *localOutput) Run(ctx context.Context) error {
	log.Info().Str("output", "local").Msg("started")
	defer log.Info().Str("output", "local").Msg("stopped")
loop:
	for {
		select {
		case e := <-l.ch:
			var f *os.File
			var err error
			if f, err = l.takeFile(e.Index()); err != nil {
				log.Error().Err(err).Msg("LocalOutput: failed to take file")
				continue
			}
			if _, err = io.WriteString(f, localOutputSerialize(e)); err != nil {
				log.Error().Err(err).Msg("LocalOutput: failed to output file")
				continue
			}
		case <-ctx.Done():
			break loop
		}
	}

	_ = l.closeFiles()
	return nil
}
