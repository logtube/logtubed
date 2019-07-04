package sptp

import "time"

const (
	// Magic magic number for SPTP
	Magic byte = 0xFA

	// OverheadChunked maximum header size, i.e. chunked message
	OverheadChunked = 12

	// OverheadSimple minimum header size, i.e. simple message
	OverheadSimple = 2

	// ChunkedMaxCount maximum chunk count
	ChunkedMaxCount = 255

	// ModeChunked flag for chunked message
	ModeChunked byte = 0x01

	// ModeGzipped flag for gzipped message
	ModeGzipped byte = 0x02

	// ChunkBufferSizeDefault default frame buffer size, i.e. maximum UDP payload size
	ChunkBufferSizeDefault = 8192

	// ChunkPayloadSizeDefault default frame size, i.e. maximum SPTP payload size
	ChunkPayloadSizeDefault = ChunkBufferSizeDefault - OverheadChunked

	// ChunkTimeoutDefault
	ChunkTimeoutDefault = time.Second * 3
)
