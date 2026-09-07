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
	ChPTY     = "pty"     // a shell on the Mac: open / data / resize / close / exit
	ChBlob    = "blob"    // bounded attachment upload frames
)

// PTY operations (plan 10 / WP6). The phone opens a terminal with an id it
// chooses; bytes flow both ways as "data"; the bridge reports "exit" when the
// shell ends and "close" when it refuses or tears one down.
const (
	PTYOpen   = "open"
	PTYData   = "data"
	PTYResize = "resize"
	PTYClose  = "close"
	PTYExit   = "exit"
)

// PTYMessage is one frame on the pty channel. Data is raw terminal bytes
// (base64url on the wire, like Chunk.Data); Cols/Rows accompany open and
// resize; Code accompanies exit; Reason accompanies close.
type PTYMessage struct {
	Ch     string `json:"ch"`
	Op     string `json:"op"`
	ID     string `json:"id"`
	Cols   int    `json:"cols,omitempty"`
	Rows   int    `json:"rows,omitempty"`
	Cwd    string `json:"cwd,omitempty"`
	Data   Bytes  `json:"d,omitempty"`
	Code   int    `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

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
	CtlPing     = "ping"
	CtlPong     = "pong"
	CtlClose    = "close"
	CtlGateway  = "gateway"  // bridge → phone: the gateway child changed state
	CtlName     = "name"     // phone → bridge: human-readable device name (in Reason)
	CtlAccepted = "accepted" // bridge → phone: admission granted (sent right after the handshake)
)

// CtlMessage carries liveness and lifecycle signals.
type CtlMessage struct {
	Ch     string        `json:"ch"`
	Op     string        `json:"op"`
	Reason string        `json:"reason,omitempty"`
	State  string        `json:"state,omitempty"` // for CtlGateway: "starting" | "ready" | "down"
	Caps   *Capabilities `json:"caps,omitempty"`
	// For CtlPush: how to reach the phone while it is disconnected.
	Push *PushRegistration `json:"push,omitempty"`
}

// Capabilities are advertised only in the encrypted admission-accepted
// control message. They are intentionally absent from the signed handshake.
type Capabilities struct {
	AttachmentBlob *AttachmentBlobCapability `json:"attachment_blob,omitempty"`
}

// AttachmentBlobCapability is version 1 of the bounded upload channel.
type AttachmentBlobCapability struct {
	Version      int   `json:"version"`
	MaxFileBytes int64 `json:"max_file_bytes"`
	ChunkBytes   int   `json:"chunk_bytes"`
}

// BlobError is a typed bridge-local or gateway JSON-RPC error. Gateway errors
// preserve their original code so clients can retain runtime recovery logic.
type BlobError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Kind    string `json:"kind"`
}

// BlobMessage is the version 1 attachment upload wire shape. Fields not used
// by an operation are omitted.
type BlobMessage struct {
	Ch         string          `json:"ch"`
	Op         string          `json:"op"`
	ID         string          `json:"id"`
	SessionID  string          `json:"session_id,omitempty"`
	Name       string          `json:"name,omitempty"`
	Size       *int64          `json:"size,omitempty"`
	SHA256     string          `json:"sha256,omitempty"`
	Offset     *int64          `json:"offset,omitempty"`
	ChunkBytes *int            `json:"chunk_bytes,omitempty"`
	Data       Bytes           `json:"d,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	Error      *BlobError      `json:"error,omitempty"`
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
	case ChWS, ChHTTP, ChCtl, ChChunk, ChConfirm, ChPTY, ChBlob:
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
