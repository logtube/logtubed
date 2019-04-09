package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type LocalOutput struct {
	Options LocalOutputOptions

	files      map[string]*os.File
	filesMutex *sync.RWMutex
}

func NewLocalOutput(options LocalOutputOptions) (o *LocalOutput, err error) {
	o = &LocalOutput{
		Options:    options,
		files:      map[string]*os.File{},
		filesMutex: &sync.RWMutex{},
	}
	err = os.MkdirAll(o.Options.Dir, 0755)
	return
}

func (l *LocalOutput) takeFile(index string) (f *os.File, err error) {
	// close all on exceeded
	if len(l.files) > 2000 {
		l.filesMutex.Lock()
		l.closeFiles()
		l.filesMutex.Unlock()
	}

	// find with R lock
	l.filesMutex.RLock()
	f = l.files[index]
	l.filesMutex.RUnlock()

	if f != nil {
		return
	}

	// re-find or create with W lock
	l.filesMutex.Lock()
	defer l.filesMutex.Unlock()
	f = l.files[index]
	if f != nil {
		return
	}
	if f, err = os.OpenFile(
		filepath.Join(l.Options.Dir, index+".log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	); err != nil {
		return
	}
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
	l.filesMutex.Lock()
	defer l.filesMutex.Unlock()
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
