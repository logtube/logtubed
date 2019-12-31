package iocount

import (
	"io"
	"sync/atomic"
)

type Reader interface {
	io.Reader
	Count() int64
	Reader() io.Reader
}

type reader struct {
	r io.Reader
	c int64
}

func NewReader(r io.Reader) Reader {
	return &reader{r: r}
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	atomic.AddInt64(&r.c, int64(n))
	return
}

func (r *reader) Count() int64 {
	return r.c
}

func (r *reader) Reader() io.Reader {
	return r.r
}
