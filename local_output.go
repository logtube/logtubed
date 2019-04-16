package main

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"strings"
)

type LocalOutput struct {
	Options LocalOutputOptions

	files map[string]*os.File
}

func NewLocalOutput(options LocalOutputOptions) (o *LocalOutput, err error) {
	o = &LocalOutput{
		Options: options,
		files:   map[string]*os.File{},
	}
	err = os.MkdirAll(o.Options.Dir, 0755)
	return
}

func (l *LocalOutput) takeFile(index string) (f *os.File, err error) {
	// close all if opened too much
	if len(l.files) > 2000 {
		l.closeFiles()
	}

	// find
	f = l.files[index]
	if f != nil {
		return
	}

	// create
	if f, err = os.OpenFile(
		filepath.Join(l.Options.Dir, index+".log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	); err != nil {
		return
	}

	// set
	l.files[index] = f

	return
}

func (l *LocalOutput) closeFiles() {
	for _, f := range l.files {
		_ = f.Close()
	}
	l.files = map[string]*os.File{}
}

func (l *LocalOutput) Close() error {
	l.closeFiles()
	return nil
}

func (l *LocalOutput) Put(op Operation) (err error) {
	var f *os.File
	if f, err = l.takeFile(op.Index); err != nil {
		return
	}
	var ret string
	if ret, err = restoreOperation(op.Body); err != nil {
		return
	}
	_, err = f.WriteString(ret)
	if err != nil {
		log.Error().Err(err).Str("output", "local").Str("file", f.Name()).Msg("events out")
	} else {
		log.Debug().Str("output", "local").Str("file", f.Name()).Msg("events out")
	}
	return
}

func restoreOperation(body []byte) (ret string, err error) {
	var e map[string]interface{}
	if err = json.Unmarshal(body, &e); err != nil {
		return
	}
	t, _ := e["timestamp"].(string)
	c, _ := e["crid"].(string)
	k, _ := e["keyword"].(string)
	m, _ := e["message"].(string)
	var x string
	for k, v := range e {
		if strings.HasPrefix(k, "x_") {
			x = x + fmt.Sprintf("%s=%v, ", k[2:], v)
		}
	}
	if len(x) > 0 {
		x = " { " + x + " }"
	}
	ret = fmt.Sprintf("[%s] (%s) [%s] %s %s\r\n", t, c, k, m, x)
	return
}
