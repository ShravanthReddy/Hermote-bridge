package bridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/relay"
)

// The phone side only differs from the direct test by the URL it dials: the
// relay's /v1/phone?s=<session id>. Everything inside the tunnel is identical.
func TestRelayTransportEndToEnd(t *testing.T) {
	srv, _ := newTestServer(t) // bridge + fake gateway (direct listener unused here)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rly := relay.New(relay.Limits{MaxPhonesPerSession: 2}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	rs := httptest.NewServer(rly.Handler())
	t.Cleanup(rs.Close)
	relayURL := "ws" + strings.TrimPrefix(rs.URL, "http")

	dialer := &RelayDialer{Server: srv, RelayURL: relayURL, Logger: srv.Logger}
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	go dialer.Run(dctx)
	for i := 0; i < 100; i++ {
		if ok, _ := dialer.Attached(); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok, err := dialer.Attached(); !ok {
		t.Fatalf("bridge never attached to the relay: %s", err)
	}

	// A phone for an unknown session is refused with the documented close code.
	if ws, _, err := websocket.Dial(ctx, relayURL+"/v1/phone?s=AAAAAAAAAAAAAAAAAAAAAA", nil); err == nil {
		_, _, rerr := ws.Read(ctx)
		if websocket.CloseStatus(rerr) != protocol.RelayCloseNoBridge {
			t.Fatalf("expected close %d for unknown session, got %v", protocol.RelayCloseNoBridge, rerr)
		}
	}

	phoneURL := relayURL + "/v1/phone?s=" + srv.Identity.SessionID()
	phone, _ := protocol.NewIdentity(nil)
	code, _, _ := srv.Pairings.Issue(time.Minute)
	p, err := connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		t.Fatalf("no gateway.ready via relay: %v", err)
	}
	_ = p.send(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: `{"jsonrpc":"2.0","id":3,"method":"session.list","params":{}}`})
	if _, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool {
		return strings.Contains(string(b), `\"id\":3`) || strings.Contains(string(b), `\"id\": 3`)
	}); err != nil {
		t.Fatalf("no RPC reply via relay: %v", err)
	}
	_ = p.send(ctx, protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: 1, Method: "GET", Path: "/api/sessions", Query: "limit=1"})
	plain, err := p.waitFor(ctx, protocol.ChHTTP, func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	var resp protocol.HTTPResponse
	_ = json.Unmarshal(plain, &resp)
	if resp.Status != 200 {
		t.Fatalf("REST via relay: %+v", resp)
	}

	// A second phone works; a third is refused by the per-session cap (2 in this test).
	p2, err := connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		t.Fatal(err)
	}
	if ws, _, err := websocket.Dial(ctx, phoneURL, nil); err == nil {
		_, _, rerr := ws.Read(ctx)
		if websocket.CloseStatus(rerr) != protocol.RelayCloseFull {
			t.Fatalf("expected close %d when full, got %v", protocol.RelayCloseFull, rerr)
		}
	}

	// Closing a phone tears down its channel on the bridge; the other survives.
	p.ws.Close(websocket.StatusNormalClosure, "")
	time.Sleep(200 * time.Millisecond)
	if n := srv.ConnectionCount(); n != 1 {
		t.Fatalf("expected 1 live bridge connection after a phone left, got %d", n)
	}
	_ = p2.send(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlPing})
	if _, err := p2.waitFor(ctx, protocol.ChCtl, func(b []byte) bool { return strings.Contains(string(b), `"pong"`) }); err != nil {
		t.Fatal(err)
	}

	// Relay restart (Drain closes the bridge socket, as a real outage would):
	// the bridge notices, tears down its phone channels, and re-attaches.
	rly.Drain()
	for i := 0; i < 100; i++ {
		if ok, _ := dialer.Attached(); !ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok, _ := dialer.Attached(); ok {
		t.Fatal("bridge did not notice the relay drop")
	}
	for i := 0; i < 200; i++ {
		if ok, _ := dialer.Attached(); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok, err := dialer.Attached(); !ok {
		t.Fatalf("bridge did not re-attach after relay drop: %s", err)
	}
	for i := 0; i < 40 && srv.ConnectionCount() != 0; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	if srv.ConnectionCount() != 0 {
		t.Fatalf("stale bridge connections after relay drop: %d", srv.ConnectionCount())
	}
	p3, err := connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p3.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		t.Fatalf("reconnect after relay drop failed: %v", err)
	}
}

// TestRelayAdmitsARealBridgeHandshake exercises admission end to end against
// the real bridge (not the relay package's fake one): a real phone handshake
// must admit the phone well before the (deliberately short) admission
// timeout would otherwise close it, and the tunnel must keep working after.
func TestRelayAdmitsARealBridgeHandshake(t *testing.T) {
	srv, _ := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rly := relay.New(relay.Limits{AdmissionTimeout: time.Second}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	rs := httptest.NewServer(rly.Handler())
	t.Cleanup(rs.Close)
	relayURL := "ws" + strings.TrimPrefix(rs.URL, "http")

	dialer := &RelayDialer{Server: srv, RelayURL: relayURL, Logger: srv.Logger}
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	go dialer.Run(dctx)
	for i := 0; i < 100; i++ {
		if ok, _ := dialer.Attached(); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ok, err := dialer.Attached(); !ok {
		t.Fatalf("bridge never attached to the relay: %s", err)
	}

	phoneURL := relayURL + "/v1/phone?s=" + srv.Identity.SessionID()
	phone, _ := protocol.NewIdentity(nil)
	code, _, _ := srv.Pairings.Issue(time.Minute)
	p, err := connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		t.Fatalf("no gateway.ready via relay: %v", err)
	}

	// Outlive the 1 s admission timeout; a real, admitted phone must survive it.
	time.Sleep(1500 * time.Millisecond)

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
	if body.Phones != 1 || body.Pending != 0 {
		t.Fatalf(`healthz = %+v, want {"phones":1,"pending":0}`, body)
	}

	_ = p.send(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: `{"jsonrpc":"2.0","id":9,"method":"session.list","params":{}}`})
	if _, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool {
		return strings.Contains(string(b), `\"id\":9`) || strings.Contains(string(b), `\"id\": 9`)
	}); err != nil {
		t.Fatalf("no RPC reply via relay after outliving the admission timeout: %v", err)
	}
}

func TestRelayKeepaliveFrameIsAnIgnorableControlMessage(t *testing.T) {
	ch, kind, payload, err := protocol.ParseRelayFrame(relayKeepaliveFrame())
	if err != nil {
		t.Fatal(err)
	}
	if ch != protocol.RelayControlChannel || kind != protocol.RelayKindText {
		t.Fatalf("keepalive must ride the control channel as text, got ch=%d kind=%d", ch, kind)
	}
	var c protocol.RelayControl
	if err := json.Unmarshal(payload, &c); err != nil || c.T != "ping" {
		t.Fatalf("keepalive payload = %q (%v)", payload, err)
	}
	if relayKeepalive*3 >= idleTimeout {
		t.Fatalf("keepalive %v must fire at least three times per relay idle timeout %v", relayKeepalive, idleTimeout)
	}
}

func TestRelayChannelCloseRequiresCurrentOwnershipAndCarriesToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type acceptedSocket struct{ ws *websocket.Conn }
	accepted := make(chan acceptedSocket, 1)
	harness := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err == nil {
			accepted <- acceptedSocket{ws: ws}
		}
	}))
	t.Cleanup(harness.Close)
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(harness.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(websocket.StatusNormalClosure, "") })
	server := receiveTestValue(t, ctx, accepted, "relay socket").ws
	t.Cleanup(func() { _ = server.Close(websocket.StatusNormalClosure, "") })

	mux := newRelayMux(client)
	old := mux.open(9, "old-token")
	current := &relayChannel{mux: mux, ch: 9, token: "current-token", inbox: make(chan relayMsg, 4), done: make(chan struct{})}
	mux.mu.Lock()
	mux.channels[9] = current
	mux.mu.Unlock()
	if err := old.Close(websocket.StatusNormalClosure, "stale"); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(websocket.StatusNormalClosure, "current"); err != nil {
		t.Fatal(err)
	}

	_, frame, err := server.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	channel, kind, payload, err := protocol.ParseRelayFrame(frame)
	if err != nil || channel != protocol.RelayControlChannel || kind != protocol.RelayKindText {
		t.Fatalf("close frame = channel %d kind %d (%v)", channel, kind, err)
	}
	var control protocol.RelayControl
	if err := json.Unmarshal(payload, &control); err != nil || control.T != "close" || control.C != 9 || control.Token != "current-token" {
		t.Fatalf("close control = %+v (%v)", control, err)
	}
	legacy := mux.open(10, "")
	if err := legacy.Close(websocket.StatusNormalClosure, "legacy relay"); err != nil {
		t.Fatal(err)
	}
	_, frame, err = server.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	channel, kind, payload, err = protocol.ParseRelayFrame(frame)
	if err != nil || channel != protocol.RelayControlChannel || kind != protocol.RelayKindText {
		t.Fatalf("legacy close frame = channel %d kind %d (%v)", channel, kind, err)
	}
	control = protocol.RelayControl{}
	if err := json.Unmarshal(payload, &control); err != nil || control.T != "close" || control.C != 10 || control.Token != "" {
		t.Fatalf("legacy close control = %+v (%v)", control, err)
	}
}

func TestVouchPrecedesAccept(t *testing.T) {
	srv, _ := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type acceptedSocket struct {
		ws *websocket.Conn
	}
	accepted := make(chan acceptedSocket, 1)
	harness := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err == nil {
			accepted <- acceptedSocket{ws: ws}
		}
	}))
	t.Cleanup(harness.Close)
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(harness.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.Close(websocket.StatusNormalClosure, "") })
	serverSocket := receiveTestValue(t, ctx, accepted, "legacy relay socket").ws
	t.Cleanup(func() { _ = serverSocket.Close(websocket.StatusNormalClosure, "") })

	const channel uint16 = 17
	const token = "AAECAwQFBgcICQoLDA0ODw"
	mux := newRelayMux(ws)
	phoneLink := &orderedVouchTestLink{mux: mux, channel: channel, token: token, inbox: make(chan relayMsg, 4)}
	phone, _ := protocol.NewIdentity(nil)
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	hello, phoneState, err := protocol.PhoneHello(phone, srv.Identity.SessionID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := phone.AddHelloAdmissionProof(&hello, time.Now().Unix(), code); err != nil {
		t.Fatal(err)
	}
	helloJSON, _ := json.Marshal(hello)
	phoneLink.inbox <- relayMsg{typ: websocket.MessageText, data: helloJSON}
	conn := srv.newConnection(phoneLink, "relay:17")
	handshake := make(chan error, 1)
	go func() { handshake <- conn.handshake(ctx) }()

	readRelayFrame := func() (uint16, byte, []byte) {
		t.Helper()
		_, frame, err := serverSocket.Read(ctx)
		if err != nil {
			t.Fatalf("read relay frame: %v", err)
		}
		channel, kind, payload, err := protocol.ParseRelayFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		return channel, kind, payload
	}
	controlChannel, controlKind, controlPayload := readRelayFrame()
	var control protocol.RelayControl
	if err := json.Unmarshal(controlPayload, &control); err != nil || controlChannel != 0 || controlKind != protocol.RelayKindText || control.T != "vouch" || control.Token != token {
		t.Fatalf("first bridge frame was not the matching vouch: channel=%d kind=%d control=%+v err=%v", controlChannel, controlKind, control, err)
	}
	acceptChannel, acceptKind, acceptRaw := readRelayFrame()
	if acceptChannel != channel || acceptKind != protocol.RelayKindText {
		t.Fatalf("second bridge frame was not Accept on channel %d: channel=%d kind=%d", channel, acceptChannel, acceptKind)
	}
	var accept protocol.Accept
	if err := json.Unmarshal(acceptRaw, &accept); err != nil {
		t.Fatal(err)
	}
	confirm, _, err := phoneState.Finish(accept, srv.Identity.Public(), code)
	if err != nil {
		t.Fatal(err)
	}
	phoneLink.inbox <- relayMsg{typ: websocket.MessageBinary, data: confirm}
	if err := receiveTestValue(t, ctx, handshake, "bridge handshake"); err != nil {
		t.Fatal(err)
	}
}

type orderedVouchTestLink struct {
	mux     *relayMux
	channel uint16
	token   string
	inbox   chan relayMsg
}

func (l *orderedVouchTestLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case msg := <-l.inbox:
		return msg.typ, msg.data, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (l *orderedVouchTestLink) Write(ctx context.Context, typ websocket.MessageType, payload []byte) error {
	kind := protocol.RelayKindBinary
	if typ == websocket.MessageText {
		kind = protocol.RelayKindText
	}
	return l.mux.write(ctx, protocol.RelayFrame(l.channel, kind, payload))
}

func (l *orderedVouchTestLink) Close(code websocket.StatusCode, reason string) error {
	return l.mux.ws.Close(code, reason)
}

func (l *orderedVouchTestLink) hasVouchToken() bool { return l.token != "" }

func (l *orderedVouchTestLink) vouch(ctx context.Context) error {
	raw, err := json.Marshal(protocol.RelayControl{T: "vouch", C: l.channel, Token: l.token})
	if err != nil {
		return err
	}
	return l.mux.write(ctx, protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
}

type rawFrameLegacyRelayHarness struct{}

func (rawFrameLegacyRelayHarness) stripOpenToken(frame []byte) []byte {
	channel, kind, payload, err := protocol.ParseRelayFrame(frame)
	if err != nil || channel != protocol.RelayControlChannel || kind != protocol.RelayKindText {
		return frame
	}
	var control protocol.RelayControl
	if json.Unmarshal(payload, &control) != nil || control.T != "open" {
		return frame
	}
	control.Token = ""
	forwarded, _ := json.Marshal(control)
	return protocol.RelayFrame(channel, kind, forwarded)
}

func (rawFrameLegacyRelayHarness) ignoresVouch(frame []byte) bool {
	channel, kind, payload, err := protocol.ParseRelayFrame(frame)
	if err != nil || channel != protocol.RelayControlChannel || kind != protocol.RelayKindText {
		return false
	}
	var control struct {
		T string `json:"t"`
	}
	return json.Unmarshal(payload, &control) == nil && control.T == "vouch"
}

func TestRelayOldPeerCompatibility(t *testing.T) {
	srv, hs := newTestServer(t)
	harness := rawFrameLegacyRelayHarness{}
	open, _ := json.Marshal(protocol.RelayControl{T: "open", C: 9, Token: "AAECAwQFBgcICQoLDA0ODw"})
	openFrame := protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, open)
	legacyFrame := harness.stripOpenToken(openFrame)
	_, _, legacyOpen, _ := protocol.ParseRelayFrame(legacyFrame)
	var got protocol.RelayControl
	if err := json.Unmarshal(legacyOpen, &got); err != nil || got.Token != "" {
		t.Fatalf("legacy relay did not remove the open token: %+v %v", got, err)
	}
	vouch, _ := json.Marshal(protocol.RelayControl{T: "vouch", C: 9, Token: "AAECAwQFBgcICQoLDA0ODw"})
	if !harness.ignoresVouch(protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, vouch)) {
		t.Fatal("legacy relay did not ignore the additive vouch control")
	}
	var oldBridge struct {
		T string `json:"t"`
		C uint16 `json:"c"`
	}
	if err := json.Unmarshal(open, &oldBridge); err != nil || oldBridge.T != "open" || oldBridge.C != 9 {
		t.Fatalf("legacy bridge rejected open with extra token: %+v %v", oldBridge, err)
	}
	newPhone, _ := protocol.NewIdentity(nil)
	newHello := helloWithAdmissionProof(t, newPhone, "AAAAAAAAAAAAAAAAAAAAAA", time.Now().Unix(), nil)
	newHelloJSON, _ := json.Marshal(newHello)
	var oldHello struct {
		Version int    `json:"v"`
		Session string `json:"s"`
	}
	if err := json.Unmarshal(newHelloJSON, &oldHello); err != nil || oldHello.Version != protocol.Version {
		t.Fatalf("legacy bridge rejected additive Hello fields: %+v %v", oldHello, err)
	}
	phone, _ := protocol.NewIdentity(nil)
	legacyHello, _, err := protocol.PhoneHello(phone, srv.Identity.SessionID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(legacyHello)
	if err != nil || strings.Contains(string(raw), `"ats"`) || strings.Contains(string(raw), `"asig"`) || strings.Contains(string(raw), `"ap"`) {
		t.Fatalf("legacy phone Hello changed: %s (%v)", raw, err)
	}
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil || !admissionAccepted(client, ctx) {
		t.Fatalf("legacy phone could not complete the old Confirm handshake: %v", err)
	}
}
