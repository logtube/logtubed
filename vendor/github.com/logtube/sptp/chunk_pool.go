package sptp

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrBadChunk bad chunk
	ErrBadChunk = errors.New("bad chunk")

	// ErrChunkMismatch chunk mismatch with previous received chunk
	ErrChunkMismatch = errors.New("chunk mismatch")
)

type ChunkGroup struct {
	ID        uint64
	Mode      byte
	Data      [][]byte
	CreatedAt time.Time
}

func NewChunkGroup(id uint64, createdAt time.Time, mode byte, c int) *ChunkGroup {
	return &ChunkGroup{
		ID:        id,
		Mode:      mode,
		Data:      make([][]byte, c, c),
		CreatedAt: createdAt,
	}
}

func (cg *ChunkGroup) Add(p []byte, i int) {
	buf := make([]byte, len(p), len(p))
	copy(buf, p)
	cg.Data[i] = buf
}

func (cg *ChunkGroup) IsCompleted() bool {
	for _, c := range cg.Data {
		if c == nil {
			return false
		}
	}
	return true
}

func (cg *ChunkGroup) Bytes() (o []byte) {
	if len(cg.Data) > 0 {
		o = cg.Data[0]
		for i := 1; i < len(cg.Data); i++ {
			o = append(o, cg.Data[i]...)
		}
	}
	return
}

type ChunkPool struct {
	Timeout     time.Duration
	ChunkGroups map[uint64]*ChunkGroup
	L           sync.Locker
}

func NewChunkPool(timeout time.Duration) *ChunkPool {
	return &ChunkPool{
		Timeout:     timeout,
		ChunkGroups: make(map[uint64]*ChunkGroup),
		L:           &sync.Mutex{},
	}
}

func (cp *ChunkPool) Consume(id uint64, m byte, c int, i int, p []byte) (po []byte, err error) {
	if c <= 1 || i >= c || p == nil {
		err = ErrBadChunk
		return
	}
	// lock and unlock
	cp.L.Lock()
	defer cp.L.Unlock()
	// cache now
	now := time.Now()
	// garbage collect
	cp.GC(now)
	// ensure chunks
	cg := cp.ChunkGroups[id]
	if cg == nil {
		cg = NewChunkGroup(id, now, m, c)
		cp.ChunkGroups[id] = cg
	} else {
		// validate mode and chunks count
		if m != cg.Mode {
			err = ErrChunkMismatch
			return
		}
		if c != len(cg.Data) {
			err = ErrChunkMismatch
			return
		}
	}
	// consume chunks
	cg.Add(p, i)
	// check if complete
	if cg.IsCompleted() {
		po = cg.Bytes()
		delete(cp.ChunkGroups, id)
		return
	}
	return
}

func (cp *ChunkPool) GC(t time.Time) {
	td := make([]uint64, 0)
	for id, cg := range cp.ChunkGroups {
		if t.Sub(cg.CreatedAt) > cp.Timeout {
			td = append(td, id)
		}
	}
	for _, id := range td {
		delete(cp.ChunkGroups, id)
	}
}
