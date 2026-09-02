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

	"github.com/shravanthreddy/hermes-ios/remote/internal/gateway"
	"github.com/shravanthreddy/hermes-ios/remote/internal/protocol"
	"github.com/shravanthreddy/hermes-ios/remote/internal/state"
)

const (
	handshakeTimeout = 15 * time.Second
	gatewayWait      = 60 * time.Second
	pingInterval     = 20 * time.Second
	idleTimeout      = 100 * time.Second
	maxFrame         = protocol.MaxPlaintext + 64
)

// Server accepts phone connections and tunnels them to the gateway child.
type Server struct {
	Identity   *protocol.Identity
	Store      *state.Store
	Gateway    *gateway.Supervisor
	Logger     *slog.Logger
	Pairings   *pairings
	httpClient *http.Client

	mu    sync.Mutex
	conns map[*conn]struct{}
}

// New wires a Server.
func New(id *protocol.Identity, st *state.Store, gw *gateway.Supervisor, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		Identity:   id,
		Store:      st,
		Gateway:    gw,
		Logger:     logger,
		Pairings:   newPairings(),
		httpClient: &http.Client{Timeout: 60 * time.Second},
		conns:      map[*conn]struct{}{},
	}
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

// DisconnectDevice closes every connection from a revoked phone.
func (s *Server) DisconnectDevice(deviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		if c.deviceID == deviceID {
			c.close(websocket.StatusPolicyViolation, "device revoked")
		}
	}
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
	c := &conn{srv: s, ws: ws, remote: r.RemoteAddr}
	s.track(c, true)
	defer s.track(c, false)
	c.run(r.Context())
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
	srv      *Server
	ws       *websocket.Conn
	remote   string
	suite    *protocol.Suite
	deviceID string
	sendMu   sync.Mutex
	closed   sync.Once
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
	if err := c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlAccepted}); err != nil {
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
	suite, used, err := pending.Finish(frame, c.srv.Store.IsTrusted, c.srv.Pairings.Outstanding())
	if err != nil {
		return err
	}
	if used != nil {
		c.srv.Pairings.Consume(used)
		if err := c.srv.Store.Trust(pending.PhoneID(), ""); err != nil {
			return fmt.Errorf("record device: %w", err)
		}
		c.srv.Logger.Info("new phone paired", "device", short(protocol.DeviceID(pending.PhoneID())))
	}
	c.suite = suite
	c.deviceID = protocol.DeviceID(pending.PhoneID())
	return nil
}

// send encrypts and writes one plaintext message, chunking when large.
func (c *conn) send(ctx context.Context, plain []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
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
		rctx, cancel := context.WithTimeout(ctx, idleTimeout)
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
