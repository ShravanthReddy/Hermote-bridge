package relay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

// newTestRelay starts a relay with the given limits behind an httptest server.
func newTestRelay(t *testing.T, limits Limits) (*Server, *httptest.Server) {
	t.Helper()
	rly := New(limits, slog.New(slog.NewTextHandler(discard{}, nil)))
	rs := httptest.NewServer(rly.Handler())
	t.Cleanup(rs.Close)
	return rly, rs
}

// discard is an io.Writer that drops everything, keeping test logs quiet.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// fakeBridge drives the bridge side of /v1/bridge directly: it attaches with
// a fresh identity, tracks open/close control frames for phone channels, and
// can push data frames to any channel to simulate the bridge's handshake.
type fakeBridge struct {
	t  *testing.T
	ws *websocket.Conn
	id *protocol.Identity

	openSig  chan uint16
	closeSig chan uint16

	openMu     sync.Mutex
	sendMu     sync.Mutex
	openTokens map[uint16]string
}

func newFakeBridge(t *testing.T, ctx context.Context, relayHTTPURL string) *fakeBridge {
	t.Helper()
	id, err := protocol.NewIdentity(nil)
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
	return newFakeBridgeWithIdentity(t, ctx, relayHTTPURL, id)
}

func newFakeBridgeWithIdentity(t *testing.T, ctx context.Context, relayHTTPURL string, id *protocol.Identity) *fakeBridge {
	t.Helper()
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dctx, wsURL(relayHTTPURL)+"/v1/bridge", nil)
	if err != nil {
		t.Fatalf("dial bridge: %v", err)
	}

	typ, raw, err := ws.Read(dctx)
	if err != nil || typ != websocket.MessageText {
		t.Fatalf("read challenge: %v", err)
	}
	var chal protocol.RelayChallenge
	if err := json.Unmarshal(raw, &chal); err != nil || chal.T != "challenge" {
		t.Fatalf("bad challenge: %s (%v)", raw, err)
	}

	attach, _ := json.Marshal(protocol.RelayAttach{
		T:   "attach",
		S:   id.SessionID(),
		K:   id.Public(),
		Sig: id.SignRelayAttach(chal.Nonce),
	})
	if err := ws.Write(dctx, websocket.MessageText, attach); err != nil {
		t.Fatalf("write attach: %v", err)
	}

	typ, raw, err = ws.Read(dctx)
	if err != nil || typ != websocket.MessageText {
		t.Fatalf("read attached: %v", err)
	}
	var ack struct {
		T string `json:"t"`
	}
	if err := json.Unmarshal(raw, &ack); err != nil || ack.T != "attached" {
		t.Fatalf("bad attach ack: %s (%v)", raw, err)
	}

	fb := &fakeBridge{
		t:          t,
		ws:         ws,
		id:         id,
		openSig:    make(chan uint16, 64),
		closeSig:   make(chan uint16, 64),
		openTokens: make(map[uint16]string),
	}
	go fb.readLoop()
	t.Cleanup(func() { ws.Close(websocket.StatusNormalClosure, "") })
	return fb
}

func (fb *fakeBridge) readLoop() {
	for {
		typ, frame, err := fb.ws.Read(context.Background())
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue
		}
		ch, _, payload, err := protocol.ParseRelayFrame(frame)
		if err != nil || ch != protocol.RelayControlChannel {
			continue
		}
		var c protocol.RelayControl
		if json.Unmarshal(payload, &c) != nil {
			continue
		}
		switch c.T {
		case "open":
			fb.openMu.Lock()
			fb.openTokens[c.C] = c.Token
			fb.openMu.Unlock()
			select {
			case fb.openSig <- c.C:
			default:
			}
		case "close":
			select {
			case fb.closeSig <- c.C:
			default:
			}
		}
	}
}

// nextOpen blocks for the next "open" control the relay sends this bridge.
func (fb *fakeBridge) nextOpen(timeout time.Duration) uint16 {
	fb.t.Helper()
	select {
	case ch := <-fb.openSig:
		return ch
	case <-time.After(timeout):
		fb.t.Fatalf("timed out waiting for an open control")
		return 0
	}
}

func (fb *fakeBridge) openToken(ch uint16) string {
	fb.openMu.Lock()
	defer fb.openMu.Unlock()
	return fb.openTokens[ch]
}

// nextClose blocks for the next "close" control the relay sends this bridge.
func (fb *fakeBridge) nextClose(timeout time.Duration) uint16 {
	fb.t.Helper()
	select {
	case ch := <-fb.closeSig:
		return ch
	case <-time.After(timeout):
		fb.t.Fatalf("timed out waiting for a close control")
		return 0
	}
}

// sendFrame pushes one data frame from the bridge to a phone channel.
func (fb *fakeBridge) sendFrame(ch uint16, payload []byte) error {
	fb.sendMu.Lock()
	defer fb.sendMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fb.ws.Write(ctx, websocket.MessageBinary, protocol.RelayFrame(ch, protocol.RelayKindBinary, payload))
}

// sendClose pushes a control "close" for a channel, exactly as the real
// bridge does when a per-channel handshake ends.
func (fb *fakeBridge) sendClose(ch uint16, reason string) error {
	fb.sendMu.Lock()
	defer fb.sendMu.Unlock()
	raw, _ := json.Marshal(protocol.RelayControl{T: "close", C: ch, Reason: reason})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fb.ws.Write(ctx, websocket.MessageBinary, protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
}

func (fb *fakeBridge) sendVouch(ch uint16, token string) error {
	fb.sendMu.Lock()
	defer fb.sendMu.Unlock()
	raw, _ := json.Marshal(protocol.RelayControl{T: "vouch", C: ch, Token: token})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fb.ws.Write(ctx, websocket.MessageBinary, protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
}

func vouchAndBarrier(t *testing.T, ctx context.Context, fb *fakeBridge, ch uint16, phone *websocket.Conn) {
	t.Helper()
	if err := fb.sendVouch(ch, fb.openToken(ch)); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(ch, []byte("vouch-barrier")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, phone, 2*time.Second)); got != "vouch-barrier" {
		t.Fatalf("vouch barrier frame = %q", got)
	}
}

func mustDialPhone(t *testing.T, ctx context.Context, relayHTTPURL, sessionID, ip string) *websocket.Conn {
	t.Helper()
	url := wsURL(relayHTTPURL) + "/v1/phone?s=" + sessionID
	var opts *websocket.DialOptions
	if ip != "" {
		opts = &websocket.DialOptions{HTTPHeader: http.Header{"X-Forwarded-For": {ip}}}
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dctx, url, opts)
	if err != nil {
		t.Fatalf("dial phone: %v", err)
	}
	t.Cleanup(func() { ws.Close(websocket.StatusNormalClosure, "") })
	return ws
}

// waitForClose reads (discarding data frames) until the socket closes, then
// checks the close code.
func waitForClose(t *testing.T, ctx context.Context, ws *websocket.Conn, wantCode websocket.StatusCode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for close %d", wantCode)
			return
		}
		rctx, cancel := context.WithTimeout(ctx, remaining)
		_, _, err := ws.Read(rctx)
		cancel()
		if err == nil {
			continue // a data frame; keep reading for the close
		}
		code := websocket.CloseStatus(err)
		if code == -1 {
			t.Fatalf("read ended without a clean close: %v", err)
			return
		}
		if websocket.StatusCode(code) != wantCode {
			t.Fatalf("expected close %d, got %d (%v)", wantCode, code, err)
		}
		return
	}
}

// assertStillOpen proves ws is still open by having the fake bridge push a
// data frame over ch and confirming the phone reads it. A Read bounded by a
// context that times out would itself abort and close the underlying
// connection (there is no other way for the library to unblock a pending
// read), so a timed-out Read can never be used to prove liveness — it can
// only ever prove non-liveness, and would close the very socket it's meant
// to be checking. Only send one such proof frame per still-pending phone:
// a second frame would hit the phone's admission decision.
func assertStillOpen(t *testing.T, ctx context.Context, fb *fakeBridge, ch uint16, ws *websocket.Conn) {
	t.Helper()
	payload := []byte("still-open")
	if err := fb.sendFrame(ch, payload); err != nil {
		t.Fatalf("send frame to channel %d: %v", ch, err)
	}
	if got := readData(t, ctx, ws, 2*time.Second); string(got) != string(payload) {
		t.Fatalf("channel %d: got %q, want %q", ch, got, payload)
	}
}

// readData reads exactly one data frame's payload.
func readData(t *testing.T, ctx context.Context, ws *websocket.Conn, timeout time.Duration) []byte {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, data, err := ws.Read(rctx)
	if err != nil {
		t.Fatalf("expected a data frame, got: %v", err)
	}
	return data
}

func TestUnadmittedPhonesDoNotConsumeAdmittedSlots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPhonesPerSession: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	p1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch1 := fb.nextOpen(2 * time.Second)
	p2 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	ch2 := fb.nextOpen(2 * time.Second)
	p3 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	ch3 := fb.nextOpen(2 * time.Second)
	p4 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.4")
	ch4 := fb.nextOpen(2 * time.Second)

	if err := fb.sendFrame(ch4, []byte("frame1")); err != nil {
		t.Fatalf("send frame 1: %v", err)
	}
	if err := fb.sendFrame(ch4, []byte("frame2")); err != nil {
		t.Fatalf("send frame 2: %v", err)
	}
	if got := string(readData(t, ctx, p4, 2*time.Second)); got != "frame1" {
		t.Fatalf("frame 1 = %q", got)
	}
	if got := string(readData(t, ctx, p4, 2*time.Second)); got != "frame2" {
		t.Fatalf("frame 2 = %q", got)
	}

	// The three unadmitted phones are unaffected by the 4th being admitted.
	assertStillOpen(t, ctx, fb, ch1, p1)
	assertStillOpen(t, ctx, fb, ch2, p2)
	assertStillOpen(t, ctx, fb, ch3, p3)
}

func TestAdmittedCapClosesTheExtraPhone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPhonesPerSession: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	// A and B both connect while the session still has room in its pending
	// queue (neither is admitted yet), so both get an open control.
	pA := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chA := fb.nextOpen(2 * time.Second)
	pB := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	chB := fb.nextOpen(2 * time.Second)

	if err := fb.sendFrame(chA, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chA, []byte("a2")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, pA, 2*time.Second)); got != "a1" {
		t.Fatalf("A frame 1 = %q", got)
	}
	if got := string(readData(t, ctx, pA, 2*time.Second)); got != "a2" {
		t.Fatalf("A frame 2 = %q", got)
	}
	// A's second frame admitted it, filling the session's one slot.

	if err := fb.sendFrame(chB, []byte("b1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chB, []byte("b2")); err != nil {
		t.Fatal(err)
	}
	waitForClose(t, ctx, pB, protocol.RelayCloseFull, 2*time.Second)

	// A is unaffected: it still receives a third frame.
	if err := fb.sendFrame(chA, []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, pA, 2*time.Second)); got != "a3" {
		t.Fatalf("A frame 3 = %q", got)
	}
}

// TestFullSessionRejectsNewPhoneAtConnect covers the fast path: a session
// that already has its admitted-phone cap filled refuses a new connection
// immediately, without ever telling the bridge about it.
func TestFullSessionRejectsNewPhoneAtConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPhonesPerSession: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	pA := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chA := fb.nextOpen(2 * time.Second)
	if err := fb.sendFrame(chA, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chA, []byte("a2")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, pA, 2*time.Second)
	readData(t, ctx, pA, 2*time.Second)
	// A is now admitted, filling the session's one slot.

	pC := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	waitForClose(t, ctx, pC, protocol.RelayCloseFull, 2*time.Second)

	select {
	case ch := <-fb.openSig:
		t.Fatalf("bridge should never have seen an open for the rejected phone, got channel %d", ch)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestPerAddressLimitDisplacesOldestFromSameAddress covers the per-key
// pending cap: a third pending connection from an address already at the
// cap displaces that same address's oldest pending phone, leaving every
// other address alone.
func TestPerAddressLimitDisplacesOldestFromSameAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 2, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	x1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.9")
	chX1 := fb.nextOpen(2 * time.Second)
	x2 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.9")
	chX2 := fb.nextOpen(2 * time.Second)

	x3 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.9")
	chX3 := fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, x1, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != chX1 {
		t.Fatalf("bridge got close for channel %d, want %d", closed, chX1)
	}

	assertStillOpen(t, ctx, fb, chX2, x2)
	assertStillOpen(t, ctx, fb, chX3, x3)

	y1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.10")
	chY1 := fb.nextOpen(2 * time.Second) // the bridge sees an open for it
	assertStillOpen(t, ctx, fb, chY1, y1)
}

// TestSessionLimitDisplacesFromBusiestAddress covers the whole-session
// pending cap: once it's reached, the busiest address loses its oldest
// pending phone, not whichever address happens to connect next.
func TestSessionLimitDisplacesFromBusiestAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerSession: 3, MaxPendingPerIP: 10, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	x1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chX1 := fb.nextOpen(2 * time.Second)
	x2 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chX2 := fb.nextOpen(2 * time.Second)
	y1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	chY1 := fb.nextOpen(2 * time.Second)

	// The session is now at its pending cap (3): X has 2, Y has 1. Z's
	// connection displaces X's oldest — the busiest address — not Y's.
	z1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	chZ1 := fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, x1, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != chX1 {
		t.Fatalf("bridge got close for channel %d, want %d", closed, chX1)
	}

	// Exactly one close control: no other phone was touched.
	select {
	case ch := <-fb.closeSig:
		t.Fatalf("unexpected second close control for channel %d", ch)
	case <-time.After(300 * time.Millisecond):
	}

	assertStillOpen(t, ctx, fb, chX2, x2)
	assertStillOpen(t, ctx, fb, chY1, y1)
	assertStillOpen(t, ctx, fb, chZ1, z1)
}

// TestDisplacementNeverPicksAnAdmittedPhone confirms an admitted phone is
// never a displacement victim, even when it's the only other phone around.
func TestDisplacementNeverPicksAnAdmittedPhone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerSession: 1, MaxPhonesPerSession: 4, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	a := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chA := fb.nextOpen(2 * time.Second)
	if err := fb.sendFrame(chA, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chA, []byte("a2")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, a, 2*time.Second)
	readData(t, ctx, a, 2*time.Second)
	// A is now admitted.

	p1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	chP1 := fb.nextOpen(2 * time.Second)

	p2 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	chP2 := fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, p1, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != chP1 {
		t.Fatalf("bridge got close for channel %d, want %d", closed, chP1)
	}

	// A is unaffected — prove it with one more frame.
	if err := fb.sendFrame(chA, []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, a, 2*time.Second)); got != "a3" {
		t.Fatalf("A frame 3 = %q", got)
	}
	assertStillOpen(t, ctx, fb, chP2, p2)
}

func requireOpenToken(t *testing.T, fb *fakeBridge, ch uint16) string {
	t.Helper()
	token := fb.openToken(ch)
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 16 {
		t.Fatalf("open token %q does not encode 16 bytes (err %v)", token, err)
	}
	return token
}

func TestVouchedVictimSelectionAtAddressCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	protected := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	protectedCh := fb.nextOpen(2 * time.Second)
	requireOpenToken(t, fb, protectedCh)
	vouchAndBarrier(t, ctx, fb, protectedCh, protected)

	newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	waitForClose(t, ctx, newcomer, protocol.RelayCloseFull, 2*time.Second)
	select {
	case ch := <-fb.closeSig:
		t.Fatalf("vouched channel %d was closed to make room", ch)
	case <-time.After(100 * time.Millisecond):
	}
	if err := fb.sendFrame(protectedCh, []byte("admit")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, protected, 2*time.Second)); got != "admit" {
		t.Fatalf("protected phone got %q", got)
	}
}

func TestVouchedVictimSelectionAtSessionCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerSession: 2, MaxPendingPerIP: 10, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	protected := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	protectedCh := fb.nextOpen(2 * time.Second)
	requireOpenToken(t, fb, protectedCh)
	vouchAndBarrier(t, ctx, fb, protectedCh, protected)
	victim := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	victimCh := fb.nextOpen(2 * time.Second)

	newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	newcomerCh := fb.nextOpen(2 * time.Second)
	waitForClose(t, ctx, victim, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != victimCh {
		t.Fatalf("closed channel %d, want eligible channel %d", closed, victimCh)
	}
	assertStillOpen(t, ctx, fb, newcomerCh, newcomer)
	if err := fb.sendFrame(protectedCh, []byte("protected")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, protected, 2*time.Second)); got != "protected" {
		t.Fatalf("protected phone got %q", got)
	}
}

func TestVouchMatchesLiveAttachmentAndToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	first := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	firstCh := fb.nextOpen(2 * time.Second)
	firstToken := requireOpenToken(t, fb, firstCh)
	if err := fb.sendVouch(firstCh, "wrong-token"); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(firstCh, []byte("wrong-token-barrier")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, first, 2*time.Second)

	second := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	secondCh := fb.nextOpen(2 * time.Second)
	waitForClose(t, ctx, first, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != firstCh {
		t.Fatalf("wrong-token vouch closed channel %d, want %d", closed, firstCh)
	}
	secondToken := requireOpenToken(t, fb, secondCh)
	if secondToken == firstToken {
		t.Fatal("channel incarnation reused its open token")
	}
	vouchAndBarrier(t, ctx, fb, secondCh, second)
	third := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	waitForClose(t, ctx, third, protocol.RelayCloseFull, 2*time.Second)
	select {
	case ch := <-fb.closeSig:
		t.Fatalf("valid-token vouch closed channel %d", ch)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestVouchIgnoresStaleCloseAndReusedChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rly, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	oldPhone := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch := fb.nextOpen(2 * time.Second)
	oldToken := requireOpenToken(t, fb, ch)
	if err := fb.sendClose(ch, "replace channel"); err != nil {
		t.Fatal(err)
	}
	waitForClose(t, ctx, oldPhone, websocket.StatusNormalClosure, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != ch {
		t.Fatalf("close control channel %d, want %d", closed, ch)
	}

	sess := rly.sessions[fb.id.SessionID()]
	sess.mu.Lock()
	sess.nextCh = ch - 1
	sess.mu.Unlock()
	current := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	currentCh := fb.nextOpen(2 * time.Second)
	if currentCh != ch {
		t.Fatalf("reused channel = %d, want %d", currentCh, ch)
	}
	currentToken := requireOpenToken(t, fb, currentCh)
	if currentToken == oldToken {
		t.Fatal("reused channel retained its previous token")
	}
	if err := fb.sendVouch(currentCh, oldToken); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(currentCh, []byte("stale-token-barrier")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, current, 2*time.Second)

	newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	_ = fb.nextOpen(2 * time.Second)
	waitForClose(t, ctx, current, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != currentCh {
		t.Fatalf("closed channel %d, want unvouched channel %d", closed, currentCh)
	}
	_ = newcomer
}

func TestVouchAfterFirstBridgeFrameIsIgnored(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	phone := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch := fb.nextOpen(2 * time.Second)
	token := requireOpenToken(t, fb, ch)
	if err := fb.sendFrame(ch, []byte("first")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, phone, 2*time.Second)
	if err := fb.sendVouch(ch, token); err != nil {
		t.Fatal(err)
	}
	// The first data frame is already observed by this point; allow the ordered
	// bridge reader to consume the following control before adding a competitor.
	time.Sleep(50 * time.Millisecond)
	newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	_ = fb.nextOpen(2 * time.Second)
	waitForClose(t, ctx, phone, protocol.RelayCloseFull, 2*time.Second)
	_ = newcomer
}

func TestVouchPlusAcceptRemainsPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)
	phone := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch := fb.nextOpen(2 * time.Second)
	requireOpenToken(t, fb, ch)
	vouchAndBarrier(t, ctx, fb, ch, phone)

	resp, err := http.Get(rs.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Phones  int `json:"phones"`
		Pending int `json:"pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Phones != 1 || body.Pending != 1 {
		t.Fatalf("vouch and one Accept frame changed admission: %+v", body)
	}
}

func TestFullVouchedPendingPoolRefusesNewcomerWithoutClosingPhones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rly, rs := newTestRelay(t, Limits{MaxPendingPerSession: 2, MaxPendingPerIP: 2, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)
	first := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	firstCh := fb.nextOpen(2 * time.Second)
	second := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	secondCh := fb.nextOpen(2 * time.Second)
	requireOpenToken(t, fb, firstCh)
	requireOpenToken(t, fb, secondCh)
	vouchAndBarrier(t, ctx, fb, firstCh, first)
	vouchAndBarrier(t, ctx, fb, secondCh, second)

	newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.3")
	waitForClose(t, ctx, newcomer, protocol.RelayCloseFull, 2*time.Second)
	select {
	case ch := <-fb.closeSig:
		t.Fatalf("full vouched pool closed existing channel %d", ch)
	case <-time.After(100 * time.Millisecond):
	}
	sess := rly.sessions[fb.id.SessionID()]
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.phones) != 2 || sess.phones[firstCh] == nil || sess.phones[secondCh] == nil {
		t.Fatalf("full-pool refusal changed occupants: %+v", sess.phones)
	}
}

func TestVouchCloseReplacementRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rly, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)
	phone := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch := fb.nextOpen(2 * time.Second)
	token := requireOpenToken(t, fb, ch)
	sess := rly.sessions[fb.id.SessionID()]
	sess.mu.Lock()
	pending := sess.phones[ch]
	sess.mu.Unlock()

	var wg sync.WaitGroup
	var closeTimer sync.WaitGroup
	closeTimer.Add(1)
	timer := time.AfterFunc(0, func() {
		defer closeTimer.Done()
		sess.closeIfPending(pending, protocol.RelayCloseAdmissionTimeout, "racing timer")
	})
	wg.Add(3)
	go func() { defer wg.Done(); _ = fb.sendVouch(ch, token) }()
	go func() { defer wg.Done(); sess.closeIfPending(pending, protocol.RelayCloseFull, "racing close") }()
	go func() { defer wg.Done(); _ = fb.sendFrame(ch, []byte("racing frame")) }()
	wg.Wait()
	if !timer.Stop() {
		closeTimer.Wait()
	}

	sess.mu.Lock()
	current := sess.phones[ch]
	sess.mu.Unlock()
	if current != nil && current != pending {
		t.Fatal("racing close/vouch changed the channel occupant")
	}
	_ = phone
}

func TestBridgeReplacementCannotCarryVouchToReusedChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	firstBridge := newFakeBridge(t, ctx, rs.URL)
	oldPhone := mustDialPhone(t, ctx, rs.URL, firstBridge.id.SessionID(), "10.0.0.1")
	oldChannel := firstBridge.nextOpen(2 * time.Second)
	oldToken := requireOpenToken(t, firstBridge, oldChannel)

	startVouch := make(chan struct{})
	vouchDone := make(chan error, 1)
	go func() {
		<-startVouch
		vouchDone <- firstBridge.sendVouch(oldChannel, oldToken)
	}()
	close(startVouch)
	secondBridge := newFakeBridgeWithIdentity(t, ctx, rs.URL, firstBridge.id)
	<-vouchDone
	if _, _, err := oldPhone.Read(ctx); err == nil {
		t.Fatal("phone remained attached after its bridge was replaced")
	}

	currentPhone := mustDialPhone(t, ctx, rs.URL, secondBridge.id.SessionID(), "10.0.0.1")
	currentChannel := secondBridge.nextOpen(2 * time.Second)
	if currentChannel != oldChannel {
		t.Fatalf("replacement channel = %d, want reused number %d", currentChannel, oldChannel)
	}
	currentToken := requireOpenToken(t, secondBridge, currentChannel)
	if currentToken == oldToken {
		t.Fatal("replacement reused the prior channel token")
	}
	if err := secondBridge.sendVouch(currentChannel, oldToken); err != nil {
		t.Fatal(err)
	}
	if err := secondBridge.sendFrame(currentChannel, []byte("replacement-barrier")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, currentPhone, 2*time.Second)); got != "replacement-barrier" {
		t.Fatalf("replacement bridge barrier = %q", got)
	}

	newcomer := mustDialPhone(t, ctx, rs.URL, secondBridge.id.SessionID(), "10.0.0.1")
	newChannel := secondBridge.nextOpen(2 * time.Second)
	waitForClose(t, ctx, currentPhone, protocol.RelayCloseFull, 2*time.Second)
	if closed := secondBridge.nextClose(2 * time.Second); closed != currentChannel {
		t.Fatalf("stale vouch protected channel %d; closed channel %d instead", currentChannel, closed)
	}
	_ = newcomer
	_ = newChannel
}

func TestSameGroupSprayCanEvictBeforeVouch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)
	first := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	firstCh := fb.nextOpen(2 * time.Second)
	second := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	_ = fb.nextOpen(2 * time.Second)
	waitForClose(t, ctx, first, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != firstCh {
		t.Fatalf("pre-vouch spray closed channel %d, want %d", closed, firstCh)
	}
	_ = second
}

func TestVouchedSocketSurvivesSameGroupSpray(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)
	protected := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	ch := fb.nextOpen(2 * time.Second)
	requireOpenToken(t, fb, ch)
	vouchAndBarrier(t, ctx, fb, ch, protected)
	for i := 0; i < 3; i++ {
		newcomer := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
		waitForClose(t, ctx, newcomer, protocol.RelayCloseFull, 2*time.Second)
	}
	select {
	case closed := <-fb.closeSig:
		t.Fatalf("spray closed vouched channel %d", closed)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestIPv6AddressesShareOneLimit(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"ipv4", "192.0.2.1", "192.0.2.1"},
		{"ipv4-mapped ipv6", "::ffff:192.0.2.1", "192.0.2.1"},
		{"ipv6", "2001:db8::1", "2001:db8::/64"},
		{"junk", "not-an-ip", "not-an-ip"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := limitKey(c.in); got != c.want {
				t.Fatalf("limitKey(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{MaxPendingPerIP: 1, AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	a1 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "2001:db8::1")
	chA1 := fb.nextOpen(2 * time.Second)
	a2 := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "2001:db8::2")
	chA2 := fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, a1, protocol.RelayCloseFull, 2*time.Second)
	if closed := fb.nextClose(2 * time.Second); closed != chA1 {
		t.Fatalf("bridge got close for channel %d, want %d", closed, chA1)
	}
	assertStillOpen(t, ctx, fb, chA2, a2)
}

func TestAdmissionTimeoutUsesRetryableCode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{AdmissionTimeout: 300 * time.Millisecond})
	fb := newFakeBridge(t, ctx, rs.URL)

	stuck := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, stuck, protocol.RelayCloseAdmissionTimeout, 2*time.Second)
}

func TestUnadmittedPhoneTimesOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{AdmissionTimeout: 300 * time.Millisecond})
	fb := newFakeBridge(t, ctx, rs.URL)

	admitted := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chAdmitted := fb.nextOpen(2 * time.Second)
	if err := fb.sendFrame(chAdmitted, []byte("f1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chAdmitted, []byte("f2")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, admitted, 2*time.Second)
	readData(t, ctx, admitted, 2*time.Second)

	stuck := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	fb.nextOpen(2 * time.Second)

	waitForClose(t, ctx, stuck, protocol.RelayCloseAdmissionTimeout, 2*time.Second)

	// The admitted phone is unaffected — prove it with a fresh frame.
	if err := fb.sendFrame(chAdmitted, []byte("f3")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, admitted, 2*time.Second)); got != "f3" {
		t.Fatalf("admitted frame 3 = %q", got)
	}
}

func TestAdmissionTimerDoesNotCloseAReusedChannel(t *testing.T) {
	sess := &session{phones: map[uint16]*phone{}}
	p1 := &phone{ch: 1}
	sess.phones[1] = p1
	delete(sess.phones, 1)
	p2 := &phone{ch: 1}
	sess.phones[1] = p2

	// If closeIfPending did not check identity, this would close p2's ws
	// (nil here) and remove it from the map.
	sess.closeIfPending(p1, protocol.RelayCloseAdmissionTimeout, "stale timer")

	if sess.phones[1] != p2 {
		t.Fatalf("closeIfPending on a stale phone must not touch the channel's current occupant")
	}
}

// TestCloseIfPendingSkipsAdmittedPhone covers the other half of A4's race
// fix: a phone that has since been admitted must survive even though its
// map entry is still the one the caller has a reference to.
func TestCloseIfPendingSkipsAdmittedPhone(t *testing.T) {
	sess := &session{phones: map[uint16]*phone{}}
	p := &phone{ch: 1, admitted: true}
	sess.phones[1] = p

	sess.closeIfPending(p, protocol.RelayCloseAdmissionTimeout, "stale timer")

	if sess.phones[1] != p {
		t.Fatalf("closeIfPending must not remove an admitted phone")
	}
}

// TestBridgeCloseDoesNotStallOtherPhones confirms A3's fix: closing phones
// that never read (so their close handshake can't complete quickly) must not
// delay frames to an unrelated, already-admitted phone on the same session.
func TestBridgeCloseDoesNotStallOtherPhones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{})
	fb := newFakeBridge(t, ctx, rs.URL)

	a := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	chA := fb.nextOpen(2 * time.Second)
	if err := fb.sendFrame(chA, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chA, []byte("a2")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, a, 2*time.Second)
	readData(t, ctx, a, 2*time.Second)
	// A is now admitted.

	// Three phones that never read anything.
	var stuckChans []uint16
	for _, ip := range []string{"10.0.0.10", "10.0.0.11", "10.0.0.12"} {
		mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), ip)
		stuckChans = append(stuckChans, fb.nextOpen(2*time.Second))
	}

	start := time.Now()
	for _, ch := range stuckChans {
		if err := fb.sendClose(ch, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := fb.sendFrame(chA, []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if got := string(readData(t, ctx, a, time.Second)); got != "a3" {
		t.Fatalf("A frame 3 = %q", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("A's frame took %s, want under 1s", elapsed)
	}
}

func TestHealthzReportsPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, rs := newTestRelay(t, Limits{AdmissionTimeout: 5 * time.Second})
	fb := newFakeBridge(t, ctx, rs.URL)

	admitted := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.1")
	_ = admitted
	chAdmitted := fb.nextOpen(2 * time.Second)
	if err := fb.sendFrame(chAdmitted, []byte("f1")); err != nil {
		t.Fatal(err)
	}
	if err := fb.sendFrame(chAdmitted, []byte("f2")); err != nil {
		t.Fatal(err)
	}
	readData(t, ctx, admitted, 2*time.Second)
	readData(t, ctx, admitted, 2*time.Second)

	pending := mustDialPhone(t, ctx, rs.URL, fb.id.SessionID(), "10.0.0.2")
	_ = pending
	fb.nextOpen(2 * time.Second)

	resp, err := http.Get(rs.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Phones  int `json:"phones"`
		Pending int `json:"pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /healthz: %v", err)
	}
	if body.Phones != 2 || body.Pending != 1 {
		t.Fatalf("healthz = %+v, want phones=2 pending=1", body)
	}
}
