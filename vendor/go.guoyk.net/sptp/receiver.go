package sptp

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

var (
	// ErrPacketTooSmall packet too small
	ErrPacketTooSmall = errors.New("packet tool small")

	// ErrMissingMagic missing magic number
	ErrMissingMagic = errors.New("missing magic")
)

// ReceiverOptions reader options
type ReceiverOptions struct {
	// ChunkBufferSize size of buffer
	ChunkBufferSize int
	// ChunkTimeout timeout for all chunk to arrive
	ChunkTimeout time.Duration
}

// Receiver receiver
type Receiver interface {
	// Receive receive a payload
	Receive() ([]byte, error)
}

type receiver struct {
	r  io.Reader
	bp *sync.Pool
	cp *ChunkPool
}

func decompress(p []byte) (po []byte, err error) {
	var r *gzip.Reader
	if r, err = gzip.NewReader(bytes.NewReader(p)); err != nil {
		return
	}
	po, err = ioutil.ReadAll(r)
	return
}

func (r *receiver) Receive() (po []byte, err error) {
	// take buffer from buffer pool and remember to return
	buf := r.bp.Get().([]byte)
	defer r.bp.Put(buf)
	// read packet
	var n int
	if n, err = r.r.Read(buf); err != nil {
		return
	}
	// check packet size
	if n < OverheadSimple+1 {
		err = ErrPacketTooSmall
		return
	}
	// check magic
	if buf[0] != Magic {
		err = ErrMissingMagic
		return
	}
	// check chunked
	if buf[1]&ModeChunked == ModeChunked {
		// check packet size
		if n < OverheadChunked+1 {
			err = ErrPacketTooSmall
			return
		}
		// reassemble
		if po, err = r.cp.Consume(
			binary.LittleEndian.Uint64(buf[2:OverheadChunked-2]),
			buf[1],
			int(buf[OverheadChunked-2]),
			int(buf[OverheadChunked-1]),
			buf[OverheadChunked:n],
		); err != nil {
			return
		}
		// decompress
		if po != nil && buf[1]&ModeGzipped == ModeGzipped {
			po, err = decompress(po)
			return
		}
		return
	} else {
		if buf[1]&ModeGzipped == ModeGzipped {
			// decompress
			po, err = decompress(buf[OverheadSimple:n])
			return
		} else {
			// clone payload
			po = make([]byte, n-2, n-2)
			copy(po, buf[2:n])
			return
		}
	}
}

func NewReceiver(r io.Reader) Receiver {
	return NewReceiverWithOptions(r, ReceiverOptions{})
}

func NewReceiverWithOptions(r io.Reader, opts ReceiverOptions) Receiver {
	if opts.ChunkBufferSize <= 0 {
		opts.ChunkBufferSize = ChunkBufferSizeDefault
	}
	if opts.ChunkTimeout == 0 {
		opts.ChunkTimeout = ChunkTimeoutDefault
	}
	return &receiver{
		r: r,
		bp: &sync.Pool{
			New: func() interface{} {
				return make([]byte, opts.ChunkBufferSize, opts.ChunkBufferSize)
			},
		},
		cp: NewChunkPool(opts.ChunkTimeout),
	}
}
