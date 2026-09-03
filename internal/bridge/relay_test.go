package bridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/relay"
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
