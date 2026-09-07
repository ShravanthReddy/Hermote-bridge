// Package bridge is the encrypted WebSocket endpoint phones connect to. Each
// phone connection gets its own gateway WebSocket, so per-session event
// rebinding behaves exactly as if the phone talked to `hermes serve` directly.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

const (
	handshakeTimeout            = 15 * time.Second
	gatewayWait                 = 60 * time.Second
	pingInterval                = 20 * time.Second
	idleTimeout                 = 100 * time.Second
	maxFrame                    = protocol.MaxPlaintext + 64
	maxAdmissionPublishAttempts = 3
)

// admissionTestHooks are installed before a test starts serving. Production
// leaves them nil. Hooks always run without Server.mu or a state lock held.
type admissionTestHooks struct {
	afterDecision    func()
	beforePublish    func(*Server)
	addTrustedResult func(error) error
	revokeResult     func(state.Device, error) (state.Device, error)
}

// Server accepts phone connections and tunnels them to the gateway child.
type Server struct {
	Identity   *protocol.Identity
	Store      *state.Store
	Gateway    *gateway.Supervisor
	Logger     *slog.Logger
	Pairings   *pairings
	httpClient *http.Client
	deps       bridgeDependencies
	httpSlots  *workPool
	fsSlots    *workPool
	blobs      *blobManager
	// OnPush receives a phone's push registration (device token, wanted
	// kinds); nil when push is not wired.
	OnPush func(deviceID string, reg protocol.PushRegistration)

	mu                   sync.Mutex
	conns                map[*conn]struct{}
	revocationGeneration uint64
	testHooks            *admissionTestHooks
}

// New wires a Server.
func New(id *protocol.Identity, st *state.Store, gw *gateway.Supervisor, logger *slog.Logger) *Server {
	return newServer(id, st, gw, logger, productionBridgeDependencies())
}

func newServer(
	id *protocol.Identity, st *state.Store, gw *gateway.Supervisor, logger *slog.Logger, deps bridgeDependencies,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	srv := &Server{
		Identity:   id,
		Store:      st,
		Gateway:    gw,
		Logger:     logger,
		Pairings:   newPairings(),
		httpClient: &http.Client{Timeout: 60 * time.Second},
		deps:       deps,
		httpSlots:  newWorkPool(deps.httpProcessWide),
		fsSlots:    newWorkPool(deps.fsProcessWide),
		conns:      map[*conn]struct{}{},
	}
	spoolRoot := deps.blobSpoolRoot
	if spoolRoot == "" && st != nil {
		spoolRoot = st.Path("attachment-blob-spool-v1")
	}
	srv.blobs = newBlobManager(spoolRoot, deps)
	return srv
}

// Handler serves /v1/bridge (WebSocket) and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		st := s.Gateway.State()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": st == gateway.StateReady, "gateway": st, "session": s.Identity.SessionID(),
			"connections": s.ConnectionCount(),
		})
	})
	mux.HandleFunc("GET /v1/bridge", s.serveWS)
	return mux
}

// ConnectionCount is the number of live phone connections.
func (s *Server) ConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *Server) blobUsageSnapshot() blobUsageSnapshot {
	if s.blobs == nil {
		return blobUsageSnapshot{}
	}
	return s.blobs.snapshot()
}

// OnlineDevices lists the device ids with a live connection; they see events
// as they happen and need no push.
func (s *Server) OnlineDevices() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for c := range s.conns {
		if c.deviceID != "" {
			ids = append(ids, c.deviceID)
		}
	}
	return ids
}

// DisconnectDevice closes currently published connections for deviceID. State
// revocation must use Revoke so handshakes in flight are also coordinated.
func (s *Server) DisconnectDevice(deviceID string) {
	s.mu.Lock()
	var matching []*conn
	for c := range s.conns {
		if c.deviceID == deviceID {
			matching = append(matching, c)
		}
	}
	s.mu.Unlock()
	for _, c := range matching {
		c.close(websocket.StatusPolicyViolation, "device revoked")
	}
}

// Revoke removes a trusted device, advances the admission generation, and
// closes every connection that published that identity before the generation
// changed. Store and network operations never run under Server.mu.
func (s *Server) Revoke(idOrPrefix string) (state.Device, error) {
	dev, err := s.Store.Revoke(idOrPrefix)
	if hooks := s.testHooks; hooks != nil && hooks.revokeResult != nil {
		dev, err = hooks.revokeResult(dev, err)
	}
	committedDiagnostic := errors.Is(err, state.ErrMutationCommitted)
	if err != nil && !committedDiagnostic {
		return state.Device{}, err
	}
	s.mu.Lock()
	s.revocationGeneration++
	var matching []*conn
	for c := range s.conns {
		if c.deviceID == dev.ID {
			matching = append(matching, c)
		}
	}
	s.mu.Unlock()
	for _, c := range matching {
		c.close(websocket.StatusPolicyViolation, "device revoked")
	}
	if committedDiagnostic {
		s.Logger.Warn("device revocation committed with post-commit diagnostic",
			"device", dev.ID,
			"closed_connections", len(matching),
			"err", err,
		)
	}
	return dev, err
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The tailnet / relay is the boundary; browsers are not a client.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.Logger.Warn("websocket accept failed", "err", err)
		return
	}
	ws.SetReadLimit(maxFrame)
	s.serveLink(r.Context(), ws, r.RemoteAddr)
}

// phoneLink is one phone's message stream: a WebSocket in direct mode, or a
// channel demultiplexed from the relay connection. *websocket.Conn satisfies
// it as-is.
type phoneLink interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(code websocket.StatusCode, reason string) error
}

// serveLink runs the handshake and tunnel for one phone until it disconnects.
func (s *Server) serveLink(ctx context.Context, link phoneLink, remote string) {
	c := s.newConnection(link, remote)
	s.track(c, true)
	defer s.track(c, false)
	c.run(ctx)
}

func (s *Server) newConnection(link phoneLink, remote string) *conn {
	c := &conn{
		srv: s, ws: link, remote: remote,
		httpSlots: newWorkPool(s.deps.httpPerConnection),
		fsSlots:   newWorkPool(s.deps.fsPerConnection),
	}
	c.blobs = newBlobConnection(c)
	return c
}

func (s *Server) track(c *conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// ── One phone connection ────────────────────────────────────────────────────

type conn struct {
	srv       *Server
	ws        phoneLink
	remote    string
	suite     *protocol.Suite
	deviceID  string
	sendMu    sync.Mutex
	closed    sync.Once
	httpSlots *workPool
	fsSlots   *workPool
	blobs     *blobConnection
	// Shells this phone opened over the tunnel (plan 10 / WP6); nil until the first.
	terminals *terminalManager
}

func (c *conn) close(code websocket.StatusCode, reason string) {
	c.closed.Do(func() { _ = c.ws.Close(code, reason) })
}

func (c *conn) run(ctx context.Context) {
	log := c.srv.Logger.With("remote", c.remote)
	if err := c.handshake(ctx); err != nil {
		log.Info("handshake rejected", "err", err)
		c.close(websocket.StatusPolicyViolation, "handshake failed")
		return
	}
	log = log.With("device", short(c.deviceID))
	log.Info("phone connected")
	defer log.Info("phone disconnected")
	// Tell the phone admission succeeded before the (possibly slow) gateway
	// dial, so it can distinguish "paired" from "refused".
	capabilities := &protocol.Capabilities{}
	if c.srv.blobs != nil && c.srv.blobs.available() {
		capabilities.AttachmentBlob = &protocol.AttachmentBlobCapability{
			Version:      blobProtocolVersion,
			MaxFileBytes: c.srv.deps.blobMaxFileBytes,
			ChunkBytes:   c.srv.deps.blobChunkBytes,
		}
	}
	if err := c.sendJSON(ctx, protocol.CtlMessage{
		Ch:   protocol.ChCtl,
		Op:   protocol.CtlAccepted,
		Caps: capabilities,
	}); err != nil {
		return
	}

	if err := c.tunnel(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Debug("tunnel ended", "err", err)
	}
	c.close(websocket.StatusNormalClosure, "")
}

// handshake runs Hello → Accept → Confirm and decides admission.
func (c *conn) handshake(ctx context.Context) error {
	hctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	typ, raw, err := c.ws.Read(hctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageText {
		return errors.New("hello must be a text frame")
	}
	var hello protocol.Hello
	if err := json.Unmarshal(raw, &hello); err != nil {
		return fmt.Errorf("bad hello: %w", err)
	}
	accept, pending, err := protocol.BridgeAccept(c.srv.Identity, hello, nil)
	if err != nil {
		return err
	}
	acceptJSON, _ := json.Marshal(accept)
	if err := c.ws.Write(hctx, websocket.MessageText, acceptJSON); err != nil {
		return err
	}
	typ, frame, err := c.ws.Read(hctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageBinary {
		return errors.New("confirm must be a binary frame")
	}
	// Capture before the trust lookup. Revoke advances this value only after its
	// state removal commits, so a later mismatch forces a fresh disk-backed
	// trust decision before publication.
	generation := c.srv.admissionGeneration()
	suite, used, err := pending.Finish(frame, c.srv.Store.IsTrusted, c.srv.Pairings.Outstanding())
	if err != nil {
		return err
	}
	if hooks := c.srv.testHooks; hooks != nil && hooks.afterDecision != nil {
		hooks.afterDecision()
	}
	newlyPaired := used != nil
	if used != nil {
		if !c.srv.Pairings.Consume(used) {
			return protocol.ErrUntrustedPhone
		}
		trustErr := c.srv.Store.AddTrusted(pending.PhoneID(), "")
		if hooks := c.srv.testHooks; hooks != nil && hooks.addTrustedResult != nil {
			trustErr = hooks.addTrustedResult(trustErr)
		}
		if trustErr != nil {
			if !errors.Is(trustErr, state.ErrMutationCommitted) {
				return fmt.Errorf("record device: %w", trustErr)
			}
			c.srv.Logger.Warn("paired device trust committed with post-commit diagnostic",
				"device", protocol.DeviceID(pending.PhoneID()),
				"err", trustErr,
			)
		}
	}
	if err := c.srv.publishAdmission(c, pending.PhoneID(), generation); err != nil {
		if newlyPaired {
			c.srv.Logger.Warn("pairing admission failed; persisted trust may remain",
				"device", protocol.DeviceID(pending.PhoneID()),
				"err", err,
				"persisted_trust_may_remain", true,
			)
		}
		return err
	}
	c.suite = suite
	if newlyPaired {
		c.srv.Logger.Info("new phone paired", "device", short(protocol.DeviceID(pending.PhoneID())))
	}
	return nil
}

func (s *Server) admissionGeneration() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revocationGeneration
}

// publishAdmission sets c.deviceID under the same mutex used by revocation and
// connection enumeration. A changed generation is revalidated from disk; the
// one-time pairing code is never consulted or consumed again.
func (s *Server) publishAdmission(c *conn, phoneID []byte, generation uint64) error {
	deviceID := protocol.DeviceID(phoneID)
	for attempt := 0; attempt < maxAdmissionPublishAttempts; attempt++ {
		if hooks := s.testHooks; hooks != nil && hooks.beforePublish != nil {
			hooks.beforePublish(s)
		}
		s.mu.Lock()
		if s.revocationGeneration == generation {
			c.deviceID = deviceID
			s.mu.Unlock()
			return nil
		}
		generation = s.revocationGeneration
		s.mu.Unlock()
		if !s.Store.IsTrusted(phoneID) {
			return protocol.ErrUntrustedPhone
		}
	}
	return errors.New("admission rejected: revocation state kept changing")
}

// send encrypts and writes one plaintext message, chunking when large.
func (c *conn) send(ctx context.Context, plain []byte) error {
	if len(plain) > protocol.MaxPlaintext {
		return plaintextLimitError{size: len(plain)}
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.sendLocked(ctx, plain)
}

func (c *conn) sendLocked(ctx context.Context, plain []byte) error {
	if chunks := protocol.SplitChunks(plain); chunks != nil {
		for _, ch := range chunks {
			raw, _ := json.Marshal(ch)
			if err := c.sealAndWrite(ctx, raw); err != nil {
				return err
			}
		}
		return nil
	}
	return c.sealAndWrite(ctx, plain)
}

func (c *conn) sealAndWrite(ctx context.Context, plain []byte) error {
	frame, err := c.suite.Seal(plain)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return c.ws.Write(wctx, websocket.MessageBinary, frame)
}

func (c *conn) sendJSON(ctx context.Context, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.send(ctx, raw)
}

// recv reads, decrypts and (if chunked) reassembles the next plaintext message.
func (c *conn) recv(ctx context.Context, asm *protocol.ChunkAssembler) ([]byte, error) {
	for {
		timeout := idleTimeout
		if c.blobs != nil {
			timeout = c.blobs.readTimeout(timeout)
		}
		rctx, cancel := context.WithTimeout(ctx, timeout)
		typ, frame, err := c.ws.Read(rctx)
		cancel()
		if err != nil {
			return nil, err
		}
		if typ != websocket.MessageBinary {
			return nil, errors.New("expected binary envelope")
		}
		plain, err := c.suite.Open(frame)
		if err != nil {
			return nil, err
		}
		ch, err := protocol.PeekChannel(plain)
		if err != nil {
			return nil, err
		}
		if ch != protocol.ChChunk {
			return plain, nil
		}
		var chunk protocol.Chunk
		if err := json.Unmarshal(plain, &chunk); err != nil {
			return nil, err
		}
		if whole, err := asm.Add(chunk); err != nil {
			return nil, err
		} else if whole != nil {
			return whole, nil
		}
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
