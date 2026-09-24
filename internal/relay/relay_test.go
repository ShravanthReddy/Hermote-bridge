package relay

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
}

func newFakeBridge(t *testing.T, ctx context.Context, relayHTTPURL string) *fakeBridge {
	t.Helper()
	id, err := protocol.NewIdentity(nil)
	if err != nil {
		t.Fatalf("new identity: %v", err)
	}
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
		t:        t,
		ws:       ws,
		id:       id,
		openSig:  make(chan uint16, 64),
		closeSig: make(chan uint16, 64),
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fb.ws.Write(ctx, websocket.MessageBinary, protocol.RelayFrame(ch, protocol.RelayKindBinary, payload))
}

// sendClose pushes a control "close" for a channel, exactly as the real
// bridge does when a per-channel handshake ends.
func (fb *fakeBridge) sendClose(ch uint16, reason string) error {
	raw, _ := json.Marshal(protocol.RelayControl{T: "close", C: ch, Reason: reason})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fb.ws.Write(ctx, websocket.MessageBinary, protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
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
