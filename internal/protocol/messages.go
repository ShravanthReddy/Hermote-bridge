package protocol

import (
	"encoding/json"
	"errors"
)

// Channel names carried in the "ch" field of every plaintext message.
const (
	ChWS      = "ws"      // one gateway JSON-RPC text frame, verbatim
	ChHTTP    = "http"    // a REST request (phone→bridge) or response (bridge→phone)
	ChCtl     = "ctl"     // liveness and lifecycle
	ChChunk   = "chunk"   // piece of a large message
	ChConfirm = "confirm" // handshake completion (first phone→bridge frame only)
)

// ChunkThreshold is the plaintext size above which a message is split.
const ChunkThreshold = 1 << 20

// WSMessage carries one gateway WebSocket text frame.
type WSMessage struct {
	Ch   string `json:"ch"`
	Data string `json:"d"`
}

// HTTPRequest is a REST call the bridge performs against the loopback gateway
// on the phone's behalf, adding the session-token header.
type HTTPRequest struct {
	Ch     string          `json:"ch"`
	ID     uint64          `json:"id"`
	Method string          `json:"method"`
	Path   string          `json:"path"`            // e.g. "/api/sessions/abc"
	Query  string          `json:"query,omitempty"` // raw, already encoded
	Body   json.RawMessage `json:"body,omitempty"`
}

// HTTPResponse answers an HTTPRequest by id.
type HTTPResponse struct {
	Ch     string          `json:"ch"`
	ID     uint64          `json:"id"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body,omitempty"`
}

// Ctl operations.
const (
	CtlPing    = "ping"
	CtlPong    = "pong"
	CtlClose   = "close"
	CtlGateway = "gateway" // bridge → phone: the gateway child changed state
)

// CtlMessage carries liveness and lifecycle signals.
type CtlMessage struct {
	Ch     string `json:"ch"`
	Op     string `json:"op"`
	Reason string `json:"reason,omitempty"`
	State  string `json:"state,omitempty"` // for CtlGateway: "starting" | "ready" | "down"
}

// Chunk is one piece of a message larger than ChunkThreshold. Pieces are
// sent in order; the receiver reassembles Index 0..Count-1 and then parses the
// joined bytes as a normal message.
type Chunk struct {
	Ch    string `json:"ch"`
	Index int    `json:"i"`
	Count int    `json:"n"`
	Data  Bytes  `json:"d"`
}

// ErrUnknownChannel is returned for a message whose "ch" is not recognised.
var ErrUnknownChannel = errors.New("protocol: unknown channel")

// PeekChannel reads only the "ch" field so callers can pick the right struct.
func PeekChannel(plain []byte) (string, error) {
	var probe struct {
		Ch string `json:"ch"`
	}
	if err := json.Unmarshal(plain, &probe); err != nil {
		return "", err
	}
	switch probe.Ch {
	case ChWS, ChHTTP, ChCtl, ChChunk, ChConfirm:
		return probe.Ch, nil
	}
	return "", ErrUnknownChannel
}

// SplitChunks splits a plaintext message into Chunk messages if it exceeds
// ChunkThreshold; otherwise it returns nil and the caller sends the message as is.
func SplitChunks(plain []byte) []Chunk {
	if len(plain) <= ChunkThreshold {
		return nil
	}
	count := (len(plain) + ChunkThreshold - 1) / ChunkThreshold
	chunks := make([]Chunk, 0, count)
	for i := 0; i < count; i++ {
		lo, hi := i*ChunkThreshold, min((i+1)*ChunkThreshold, len(plain))
		chunks = append(chunks, Chunk{Ch: ChChunk, Index: i, Count: count, Data: plain[lo:hi]})
	}
	return chunks
}

// ChunkAssembler reassembles an in-order chunk sequence.
type ChunkAssembler struct {
	buf   []byte
	next  int
	count int
}

// ErrChunkOrder is returned when chunks arrive out of sequence.
var ErrChunkOrder = errors.New("protocol: chunk out of order")

// Add appends a chunk; it returns the complete message once the last chunk
// arrives, or nil while more are expected.
func (a *ChunkAssembler) Add(c Chunk) ([]byte, error) {
	if c.Count <= 0 || c.Index != a.next || (a.next > 0 && c.Count != a.count) {
		a.Reset()
		return nil, ErrChunkOrder
	}
	if len(a.buf)+len(c.Data) > MaxPlaintext {
		a.Reset()
		return nil, errors.New("protocol: chunked message exceeds MaxPlaintext")
	}
	a.count = c.Count
	a.buf = append(a.buf, c.Data...)
	a.next++
	if a.next == a.count {
		out := a.buf
		a.Reset()
		return out, nil
	}
	return nil, nil
}

// Reset drops any partial message.
func (a *ChunkAssembler) Reset() { a.buf, a.next, a.count = nil, 0, 0 }
