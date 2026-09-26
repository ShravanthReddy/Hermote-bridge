// Package relay is the stateless meeting point for bridges that cannot be
// reached directly: a bridge dials in and proves its session id, phones dial
// in with that id, and the relay forwards opaque frames between them. It holds
// no keys, keeps nothing on disk, and cannot read or inject traffic.
package relay

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

// Limits are the abuse controls (docs/REMOTE-ACCESS.md §6).
type Limits struct {
	// MaxPhonesPerSession caps admitted phones per session: a phone counts
	// once the relay has forwarded it a second data frame from the bridge,
	// i.e. once the bridge's §4 handshake with it has succeeded.
	MaxPhonesPerSession int
	// MaxPendingPerSession caps unadmitted phones per session; once it is
	// reached the oldest unadmitted phone is displaced to make room.
	MaxPendingPerSession int
	// MaxPendingPerIP caps unadmitted phones per session from one client IP.
	MaxPendingPerIP int
	// AdmissionTimeout is how long a phone may stay unadmitted before the
	// relay closes it. Must stay above the 30 s per-frame phone write
	// timeout: one slow phone can delay another phone's frames, because the
	// bridge→phones loop writes inline.
	AdmissionTimeout time.Duration
	IdleTimeout      time.Duration
	// BytesPerSecond caps each channel's sustained throughput (token bucket);
	// Burst is the bucket size.
	BytesPerSecond int
	Burst          int
	// ConnectionsPerIPPerMinute bounds new connections from one address.
	ConnectionsPerIPPerMinute int
	MaxFrame                  int64
	// TrustedProxies are the CIDR ranges allowed to speak for a client via
	// X-Forwarded-For — the local reverse proxy (loopback on the hosted VM,
	// the compose network on self-hosted installs). Empty falls back to
	// DefaultTrustedProxies.
	TrustedProxies []netip.Prefix
}

// DefaultLimits match the design document.
var DefaultLimits = Limits{
	MaxPhonesPerSession:       protocol.RelayMaxPhones,
	MaxPendingPerSession:      8,
	MaxPendingPerIP:           4,
	AdmissionTimeout:          45 * time.Second,
	IdleTimeout:               100 * time.Second,
	BytesPerSecond:            2 << 20,
	Burst:                     8 << 20,
	ConnectionsPerIPPerMinute: 60,
	MaxFrame:                  protocol.MaxPlaintext + 1024,
}

// DefaultTrustedProxies is the loopback range the hosted VM's proxy uses.
var DefaultTrustedProxies = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("::1/128"),
}

// ParseTrustedProxies parses a comma-separated list of CIDRs, rejecting any
// entry that is not a valid prefix so a typo fails fast at startup.
func ParseTrustedProxies(list string) ([]netip.Prefix, error) {
	if strings.TrimSpace(list) == "" {
		return nil, nil
	}
	var prefixes []netip.Prefix
	for _, part := range strings.Split(list, ",") {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", part, err)
		}
		prefixes = append(prefixes, prefix)
	}
	return prefixes, nil
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
	if limits.MaxPendingPerSession > 0 {
		d.MaxPendingPerSession = limits.MaxPendingPerSession
	}
	if limits.MaxPendingPerIP > 0 {
		d.MaxPendingPerIP = limits.MaxPendingPerIP
	}
	if limits.AdmissionTimeout > 0 {
		d.AdmissionTimeout = limits.AdmissionTimeout
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
	d.TrustedProxies = DefaultTrustedProxies
	if len(limits.TrustedProxies) > 0 {
		d.TrustedProxies = limits.TrustedProxies
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
		pending := 0
		for _, sess := range s.sessions {
			ph, pe := sess.counts()
			phones += ph
			pending += pe
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "bridges": n, "phones": phones, "pending": pending, "uptime_s": int(time.Since(s.started).Seconds())})
	})
	mux.HandleFunc("GET /v1/bridge", s.serveBridge)
	mux.HandleFunc("GET /v1/phone", s.servePhone)
	return mux
}

// ipWindow tracks the per-IP connection rate.
type ipWindow struct {
	start time.Time
	count int
}

func clientIP(r *http.Request, trusted []netip.Prefix) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// X-Forwarded-For counts only when the direct peer sits inside a trusted
	// proxy prefix — from anywhere else the header is client-controlled and
	// would let a caller pick the address the relay rate-limits and groups
	// pending connections by. Under this Caddyfile the proxy replaces the
	// header rather than appending, so the last valid entry is the client it
	// saw; earlier entries may be forged.
	if peer, aerr := netip.ParseAddr(host); aerr == nil {
		peer = peer.WithZone("").Unmap()
		for _, prefix := range trusted {
			if !prefix.Contains(peer) {
				continue
			}
			parts := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
			for i := len(parts) - 1; i >= 0; i-- {
				entry := strings.TrimSpace(parts[i])
				if entry == "" {
					continue
				}
				if addr, perr := netip.ParseAddr(entry); perr == nil {
					return addr.String()
				}
				break
			}
			break
		}
	}
	return host
}

// limitKey groups client addresses for the connection and pending limits:
// IPv4 (including IPv4-mapped IPv6) keys by the address itself; IPv6 keys by
// its /64 prefix, since a single allocation can rotate through many
// addresses in that range. Anything that fails to parse is returned
// unchanged.
func limitKey(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	if addr.Is4() || addr.Is4In6() {
		return addr.Unmap().String()
	}
	return netip.PrefixFrom(addr, 64).Masked().String()
}

func (s *Server) allowIP(ip string) bool {
	key := limitKey(ip)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	w := s.ipRate[key]
	if w == nil || now.Sub(w.start) > time.Minute {
		w = &ipWindow{start: now}
		s.ipRate[key] = w
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

// session connects one bridge to its paired phone tunnels.
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

	key    string // limitKey(ip); groups this phone for the pending-connection limits
	opened time.Time
	token  string

	// bridgeFrames and admitted are guarded by session.mu.
	bridgeFrames int
	admitted     bool
	vouched      bool
}

// shutdown closes p off the caller's goroutine. Close waits up to 5 s for the
// peer's close handshake, and a peer that never reads would otherwise stall
// every phone in the session (the bridge→phones loop writes inline).
func (p *phone) shutdown(code websocket.StatusCode, reason string) {
	go func() {
		p.ws.Close(code, reason)
		p.cancel()
	}()
}

// counts returns the total and pending phone counts in one lock acquisition.
func (sess *session) counts() (phones, pending int) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, p := range sess.phones {
		phones++
		if !p.admitted {
			pending++
		}
	}
	return phones, pending
}

// displacementVictim picks the pending phone to evict, if any, to make room
// for a new connection keyed by key. It never returns an admitted phone.
// Callers should displace this victim only after confirming there isn't
// already room without doing so. Caller holds sess.mu.
func (sess *session) displacementVictim(key string, maxPerKey, maxTotal int) *phone {
	perKey := map[string]int{}
	eligibleByKey := map[string]int{}
	oldestByKey := map[string]*phone{}
	total := 0
	for _, p := range sess.phones {
		if p.admitted {
			continue
		}
		total++
		perKey[p.key]++
		if !p.vouched {
			eligibleByKey[p.key]++
			if old, ok := oldestByKey[p.key]; !ok || p.opened.Before(old.opened) {
				oldestByKey[p.key] = p
			}
		}
	}

	if perKey[key] >= maxPerKey {
		return oldestByKey[key]
	}
	if total < maxTotal {
		return nil
	}
	// Evict from the busiest key; ties go to whichever key's oldest pending
	// phone is oldest.
	var busiestKey string
	var busiestCount int
	var busiestOldest time.Time
	for k, c := range eligibleByKey {
		old := oldestByKey[k]
		if busiestKey == "" || c > busiestCount || (c == busiestCount && old.opened.Before(busiestOldest)) {
			busiestKey, busiestCount, busiestOldest = k, c, old.opened
		}
	}
	if busiestKey == "" {
		return nil
	}
	return oldestByKey[busiestKey]
}

// admittedCount returns the number of admitted phones. Caller must hold
// sess.mu.
func (sess *session) admittedCount() int {
	n := 0
	for _, p := range sess.phones {
		if p.admitted {
			n++
		}
	}
	return n
}

// countBridgeFrame records a data frame from the bridge on channel ch and
// decides admission: a phone is admitted the moment it has received its
// second bridge frame, provided the session has room for another admitted
// phone. p is nil if the channel has no phone (already gone). overCap is
// true when this frame would admit a phone past maxAdmitted; the caller
// must then close the phone and drop the frame.
func (sess *session) countBridgeFrame(ch uint16, maxAdmitted int) (p *phone, overCap bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p = sess.phones[ch]
	if p == nil {
		return nil, false
	}
	p.bridgeFrames++
	if p.bridgeFrames == 2 && !p.admitted {
		if sess.admittedCount() < maxAdmitted {
			p.admitted = true
		} else {
			overCap = true
		}
	}
	return p, overCap
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

func (s *Server) serveBridge(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r, s.Limits.TrustedProxies)
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
			if json.Unmarshal(payload, &c) == nil {
				switch c.T {
				case "close":
					sess.closePhoneFromBridge(c.C, c.Token, c.Reason)
				case "vouch":
					s.vouch(sess, c)
				}
			}
			continue
		}
		p, overCap := sess.countBridgeFrame(ch, s.Limits.MaxPhonesPerSession)
		if p == nil {
			continue // phone already gone
		}
		if overCap {
			sess.closePhone(ch, protocol.RelayCloseFull, "too many phones for this bridge")
			continue
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

func (s *Server) vouch(sess *session, control protocol.RelayControl) {
	tokenBytes, err := base64.RawURLEncoding.DecodeString(control.Token)
	if err != nil || len(tokenBytes) != 16 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[sess.id] != sess {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.phones[control.C]
	if p == nil || p.admitted || p.bridgeFrames != 0 || p.vouched || p.token != control.Token {
		return
	}
	p.vouched = true
}

func (sess *session) closePhone(ch uint16, code websocket.StatusCode, reason string) {
	sess.mu.Lock()
	p := sess.phones[ch]
	delete(sess.phones, ch)
	sess.mu.Unlock()
	if p != nil {
		p.shutdown(code, reason)
	}
}

// closePhoneFromBridge applies a close only to the socket incarnation named by
// its token. A missing token preserves compatibility with older bridges.
func (sess *session) closePhoneFromBridge(ch uint16, token, reason string) {
	sess.mu.Lock()
	p := sess.phones[ch]
	if p == nil || (token != "" && p.token != token) {
		sess.mu.Unlock()
		return
	}
	delete(sess.phones, ch)
	sess.mu.Unlock()
	p.shutdown(websocket.StatusNormalClosure, reason)
}

// closeIfPending closes p only if it is still the phone occupying its
// channel and has not since been admitted, so a stale reference (e.g. from a
// delayed timer) never closes a different phone that reused the same channel
// number, and never races an admission that just landed.
func (sess *session) closeIfPending(p *phone, code websocket.StatusCode, reason string) {
	sess.mu.Lock()
	if sess.phones[p.ch] != p || p.admitted {
		sess.mu.Unlock()
		return
	}
	delete(sess.phones, p.ch)
	sess.mu.Unlock()
	p.shutdown(code, reason)
}

func (sess *session) closeAllPhones() {
	sess.mu.Lock()
	phones := sess.phones
	sess.phones = map[uint16]*phone{}
	sess.mu.Unlock()
	for _, p := range phones {
		p.shutdown(protocol.RelayCloseNoBridge, "bridge went away")
	}
}

func (s *Server) servePhone(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r, s.Limits.TrustedProxies)
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
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		ws.Close(websocket.StatusInternalError, "channel token generation failed")
		return
	}
	p := &phone{ws: ws, bucket: newBucket(s.Limits.BytesPerSecond, s.Limits.Burst), cancel: pcancel, key: limitKey(ip), opened: time.Now(), token: base64.RawURLEncoding.EncodeToString(tokenBytes)}

	sess.mu.Lock()
	if sess.admittedCount() >= s.Limits.MaxPhonesPerSession {
		sess.mu.Unlock()
		ws.Close(protocol.RelayCloseFull, "too many phones for this bridge")
		return
	}
	victim := sess.displacementVictim(p.key, s.Limits.MaxPendingPerIP, s.Limits.MaxPendingPerSession)
	pending, sameKey := 0, 0
	for _, current := range sess.phones {
		if current.admitted {
			continue
		}
		pending++
		if current.key == p.key {
			sameKey++
		}
	}
	if (sameKey >= s.Limits.MaxPendingPerIP || pending >= s.Limits.MaxPendingPerSession) && victim == nil {
		sess.mu.Unlock()
		ws.Close(protocol.RelayCloseFull, "pending phone capacity is protected")
		return
	}
	if victim != nil {
		delete(sess.phones, victim.ch)
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
	if victim != nil {
		victim.shutdown(protocol.RelayCloseFull, "displaced by a newer connection")
	}

	log := s.Logger.With("ip", ip, "session", short(id), "channel", p.ch)
	log.Info("phone connected")
	defer log.Info("phone disconnected")
	defer func() {
		sess.mu.Lock()
		if sess.phones[p.ch] == p {
			delete(sess.phones, p.ch)
		}
		sess.mu.Unlock()
		_ = sess.control(context.Background(), protocol.RelayControl{T: "close", C: p.ch, Token: p.token})
	}()

	timer := time.AfterFunc(s.Limits.AdmissionTimeout, func() {
		sess.closeIfPending(p, protocol.RelayCloseAdmissionTimeout, "not admitted by the bridge in time")
	})
	defer timer.Stop()

	if err := sess.control(pctx, protocol.RelayControl{T: "open", C: p.ch, Token: p.token}); err != nil {
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

// bucket limits traffic with a token bucket.
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
