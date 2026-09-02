// Package relay is the stateless meeting point for bridges that cannot be
// reached directly: a bridge dials in and proves its session id, phones dial
// in with that id, and the relay forwards opaque frames between them. It holds
// no keys, keeps nothing on disk, and cannot read or inject traffic.
package relay

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// Limits are the abuse controls (docs/REMOTE-ACCESS.md §6).
type Limits struct {
	MaxPhonesPerSession int
	IdleTimeout         time.Duration
	// BytesPerSecond caps each channel's sustained throughput (token bucket);
	// Burst is the bucket size.
	BytesPerSecond int
	Burst          int
	// ConnectionsPerIPPerMinute bounds new connections from one address.
	ConnectionsPerIPPerMinute int
	MaxFrame                  int64
}

// DefaultLimits match the design document.
var DefaultLimits = Limits{
	MaxPhonesPerSession:       protocol.RelayMaxPhones,
	IdleTimeout:               100 * time.Second,
	BytesPerSecond:            2 << 20,
	Burst:                     8 << 20,
	ConnectionsPerIPPerMinute: 60,
	MaxFrame:                  protocol.MaxPlaintext + 1024,
}

// Server is the relay.
type Server struct {
	Limits Limits
	Logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	ipRate   map[string]*ipWindow
	started  time.Time
}

// New builds a relay with the given limits (zero fields fall back to defaults).
func New(limits Limits, logger *slog.Logger) *Server {
	d := DefaultLimits
	if limits.MaxPhonesPerSession > 0 {
		d.MaxPhonesPerSession = limits.MaxPhonesPerSession
	}
	if limits.IdleTimeout > 0 {
		d.IdleTimeout = limits.IdleTimeout
	}
	if limits.BytesPerSecond > 0 {
		d.BytesPerSecond = limits.BytesPerSecond
	}
	if limits.Burst > 0 {
		d.Burst = limits.Burst
	}
	if limits.ConnectionsPerIPPerMinute > 0 {
		d.ConnectionsPerIPPerMinute = limits.ConnectionsPerIPPerMinute
	}
	if limits.MaxFrame > 0 {
		d.MaxFrame = limits.MaxFrame
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{Limits: d, Logger: logger, sessions: map[string]*session{}, ipRate: map[string]*ipWindow{}, started: time.Now()}
}

// Handler serves /v1/bridge, /v1/phone and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		n := len(s.sessions)
		phones := 0
		for _, sess := range s.sessions {
			phones += sess.phoneCount()
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "bridges": n, "phones": phones, "uptime_s": int(time.Since(s.started).Seconds())})
	})
	mux.HandleFunc("GET /v1/bridge", s.serveBridge)
	mux.HandleFunc("GET /v1/phone", s.servePhone)
	return mux
}

// ── per-IP connection rate ──────────────────────────────────────────────────

type ipWindow struct {
	start time.Time
	count int
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) allowIP(ip string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	w := s.ipRate[ip]
	if w == nil || now.Sub(w.start) > time.Minute {
		w = &ipWindow{start: now}
		s.ipRate[ip] = w
	}
	w.count++
	if len(s.ipRate) > 10000 { // keep the map bounded
		for k, v := range s.ipRate {
			if now.Sub(v.start) > time.Minute {
				delete(s.ipRate, k)
			}
		}
	}
	return w.count <= s.Limits.ConnectionsPerIPPerMinute
}

// ── sessions ────────────────────────────────────────────────────────────────

type session struct {
	id     string
	bridge *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	phones  map[uint16]*phone
	nextCh  uint16
	writeMu sync.Mutex
}

type phone struct {
	ws     *websocket.Conn
	ch     uint16
	bucket *bucket
	cancel context.CancelFunc
}

func (sess *session) phoneCount() int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.phones)
}

// writeBridge serialises writes to the bridge socket.
func (sess *session) writeBridge(ctx context.Context, frame []byte) error {
	sess.writeMu.Lock()
	defer sess.writeMu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return sess.bridge.Write(wctx, websocket.MessageBinary, frame)
}

func (sess *session) control(ctx context.Context, c protocol.RelayControl) error {
	raw, _ := json.Marshal(c)
	return sess.writeBridge(ctx, protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
}

// ── bridge side ─────────────────────────────────────────────────────────────

func (s *Server) serveBridge(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !s.allowIP(ip) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ws.SetReadLimit(s.Limits.MaxFrame)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	log := s.Logger.With("ip", ip)

	// Challenge / attach.
	nonce := make([]byte, protocol.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		ws.Close(websocket.StatusInternalError, "")
		return
	}
	chal, _ := json.Marshal(protocol.RelayChallenge{T: "challenge", Nonce: nonce})
	hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
	if err := ws.Write(hctx, websocket.MessageText, chal); err != nil {
		hcancel()
		return
	}
	typ, raw, err := ws.Read(hctx)
	hcancel()
	if err != nil || typ != websocket.MessageText {
		ws.Close(protocol.RelayCloseBadAttach, "attach expected")
		return
	}
	var attach protocol.RelayAttach
	if err := json.Unmarshal(raw, &attach); err != nil || protocol.VerifyRelayAttach(attach, nonce) != nil {
		log.Info("bridge attach rejected")
		ws.Close(protocol.RelayCloseBadAttach, "bad attach")
		return
	}
	sess := &session{id: attach.S, bridge: ws, ctx: ctx, cancel: cancel, phones: map[uint16]*phone{}}

	s.mu.Lock()
	if old := s.sessions[attach.S]; old != nil {
		// A bridge restart supersedes the previous socket.
		old.bridge.Close(protocol.RelayCloseReplaced, "replaced by a newer bridge connection")
		old.cancel()
	}
	s.sessions[attach.S] = sess
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.sessions[attach.S] == sess {
			delete(s.sessions, attach.S)
		}
		s.mu.Unlock()
		sess.closeAllPhones()
	}()
	ack, _ := json.Marshal(map[string]string{"t": "attached"})
	if err := ws.Write(ctx, websocket.MessageText, ack); err != nil {
		return
	}
	log = log.With("session", short(attach.S))
	log.Info("bridge attached")
	defer log.Info("bridge detached")

	// bridge → phones
	for {
		rctx, rcancel := context.WithTimeout(ctx, s.Limits.IdleTimeout)
		typ, frame, err := ws.Read(rctx)
		rcancel()
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		ch, kind, payload, err := protocol.ParseRelayFrame(frame)
		if err != nil {
			continue
		}
		if ch == protocol.RelayControlChannel {
			var c protocol.RelayControl
			if json.Unmarshal(payload, &c) == nil && c.T == "close" {
				sess.closePhone(c.C, websocket.StatusNormalClosure, c.Reason)
			}
			continue
		}
		sess.mu.Lock()
		p := sess.phones[ch]
		sess.mu.Unlock()
		if p == nil {
			continue // phone already gone
		}
		if !p.bucket.take(len(payload)) {
			sess.closePhone(ch, websocket.StatusPolicyViolation, "rate limit")
			continue
		}
		mt := websocket.MessageBinary
		if kind == protocol.RelayKindText {
			mt = websocket.MessageText
		}
		wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
		err = p.ws.Write(wctx, mt, payload)
		wcancel()
		if err != nil {
			sess.closePhone(ch, websocket.StatusGoingAway, "")
		}
	}
}

func (sess *session) closePhone(ch uint16, code websocket.StatusCode, reason string) {
	sess.mu.Lock()
	p := sess.phones[ch]
	delete(sess.phones, ch)
	sess.mu.Unlock()
	if p != nil {
		p.ws.Close(code, reason)
		p.cancel()
	}
}

func (sess *session) closeAllPhones() {
	sess.mu.Lock()
	phones := sess.phones
	sess.phones = map[uint16]*phone{}
	sess.mu.Unlock()
	for _, p := range phones {
		p.ws.Close(protocol.RelayCloseNoBridge, "bridge went away")
		p.cancel()
	}
}

// ── phone side ──────────────────────────────────────────────────────────────

func (s *Server) servePhone(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	if !s.allowIP(ip) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	id := r.URL.Query().Get("s")
	if len(id) != protocol.SessionIDLength {
		http.Error(w, "missing session id", http.StatusBadRequest)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ws.SetReadLimit(s.Limits.MaxFrame)

	s.mu.Lock()
	sess := s.sessions[id]
	s.mu.Unlock()
	if sess == nil {
		ws.Close(protocol.RelayCloseNoBridge, "no bridge is connected for this session")
		return
	}
	pctx, pcancel := context.WithCancel(sess.ctx)
	defer pcancel()
	p := &phone{ws: ws, bucket: newBucket(s.Limits.BytesPerSecond, s.Limits.Burst), cancel: pcancel}

	sess.mu.Lock()
	if len(sess.phones) >= s.Limits.MaxPhonesPerSession {
		sess.mu.Unlock()
		ws.Close(protocol.RelayCloseFull, "too many phones for this bridge")
		return
	}
	sess.nextCh++
	if sess.nextCh == 0 {
		sess.nextCh = 1
	}
	for sess.phones[sess.nextCh] != nil {
		sess.nextCh++
		if sess.nextCh == 0 {
			sess.nextCh = 1
		}
	}
	p.ch = sess.nextCh
	sess.phones[p.ch] = p
	sess.mu.Unlock()

	log := s.Logger.With("ip", ip, "session", short(id), "channel", p.ch)
	log.Info("phone connected")
	defer log.Info("phone disconnected")
	defer func() {
		sess.mu.Lock()
		if sess.phones[p.ch] == p {
			delete(sess.phones, p.ch)
		}
		sess.mu.Unlock()
		_ = sess.control(context.Background(), protocol.RelayControl{T: "close", C: p.ch})
	}()

	if err := sess.control(pctx, protocol.RelayControl{T: "open", C: p.ch}); err != nil {
		return
	}

	// phone → bridge
	for {
		rctx, rcancel := context.WithTimeout(pctx, s.Limits.IdleTimeout)
		typ, data, err := ws.Read(rctx)
		rcancel()
		if err != nil {
			return
		}
		if !p.bucket.take(len(data)) {
			ws.Close(websocket.StatusPolicyViolation, "rate limit")
			return
		}
		kind := protocol.RelayKindBinary
		if typ == websocket.MessageText {
			kind = protocol.RelayKindText
		}
		if err := sess.writeBridge(pctx, protocol.RelayFrame(p.ch, kind, data)); err != nil {
			ws.Close(protocol.RelayCloseNoBridge, "bridge went away")
			return
		}
	}
}

// ── token bucket ────────────────────────────────────────────────────────────

type bucket struct {
	mu     sync.Mutex
	tokens float64
	rate   float64
	burst  float64
	last   time.Time
}

func newBucket(rate, burst int) *bucket {
	return &bucket{tokens: float64(burst), rate: float64(rate), burst: float64(burst), last: time.Now()}
}

func (b *bucket) take(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if float64(n) > b.tokens {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// Drain closes every bridge connection (and with it every phone) so bridges
// reconnect — to this instance after a restart, or to whatever replaces it.
// Used on shutdown and by tests that simulate a relay outage.
func (s *Server) Drain() {
	s.mu.Lock()
	sessions := s.sessions
	s.sessions = map[string]*session{}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.bridge.Close(websocket.StatusGoingAway, "relay draining")
		sess.cancel()
		sess.closeAllPhones()
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

var _ = errors.New // keep the import stable for future error values
