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

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

// fakeGateway imitates the loopback `hermes serve`: a token-checked WebSocket
// that sends gateway.ready and echoes RPCs, plus a couple of REST routes.
func fakeGateway(t *testing.T, token string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	auth := func(r *http.Request) bool {
		return r.Header.Get("X-Hermes-Session-Token") == token || r.URL.Query().Get("token") == token
	}
	mux.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "")
		ws.SetReadLimit(maxFrame) // the real gateway accepts 16 MB frames
		ctx := r.Context()
		_ = ws.Write(ctx, websocket.MessageText, []byte(`{"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{"replay_epoch":"e1"}}}`))
		for {
			_, data, err := ws.Read(ctx)
			if err != nil {
				return
			}
			var req struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			_ = json.Unmarshal(data, &req)
			resp, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{"echo": req.Method}})
			_ = ws.Write(ctx, websocket.MessageText, resp)
		}
	})
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"auth_required":false,"version":"test"}`))
	})
	mux.HandleFunc("/api/sessions", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"sessions":[],"limit":"` + r.URL.Query().Get("limit") + `"}`))
	})
	mux.HandleFunc("/api/secret", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"leak":true}`))
	})
	return httptest.NewServer(mux)
}

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("HERMES_HOME", t.TempDir())
	st, err := state.Open()
	if err != nil {
		t.Fatal(err)
	}
	id, err := st.Identity()
	if err != nil {
		t.Fatal(err)
	}
	sup, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: os.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gw := fakeGateway(t, sup.Token())
	t.Cleanup(gw.Close)
	port := gw.Listener.Addr().(interface{ String() string }).String()
	port = port[strings.LastIndex(port, ":")+1:]
	gateway.ForceReadyForTest(sup, port)
	srv := New(id, st, sup, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, hs
}

// phoneClient is a minimal Go stand-in for the iOS BridgeLink.
type phoneClient struct {
	ws    *websocket.Conn
	suite *protocol.Suite
	asm   protocol.ChunkAssembler
}

func connectPhone(t *testing.T, ctx context.Context, hs *httptest.Server, bridgeID *protocol.Identity, phone *protocol.Identity, code []byte) (*phoneClient, error) {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http")+"/v1/bridge", nil)
	if err != nil {
		t.Fatal(err)
	}
	ws.SetReadLimit(maxFrame)
	hello, ps, err := protocol.PhoneHello(phone, bridgeID.SessionID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(hello)
	if err := ws.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
	_, acceptRaw, err := ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	var accept protocol.Accept
	if err := json.Unmarshal(acceptRaw, &accept); err != nil {
		t.Fatal(err)
	}
	confirm, suite, err := ps.Finish(accept, bridgeID.Public(), code)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, confirm); err != nil {
		t.Fatal(err)
	}
	return &phoneClient{ws: ws, suite: suite}, nil
}

func (p *phoneClient) send(ctx context.Context, v any) error {
	raw, _ := json.Marshal(v)
	frame, err := p.suite.Seal(raw)
	if err != nil {
		return err
	}
	return p.ws.Write(ctx, websocket.MessageBinary, frame)
}

func (p *phoneClient) recv(ctx context.Context) (string, []byte, error) {
	for {
		_, frame, err := p.ws.Read(ctx)
		if err != nil {
			return "", nil, err
		}
		plain, err := p.suite.Open(frame)
		if err != nil {
			return "", nil, err
		}
		ch, err := protocol.PeekChannel(plain)
		if err != nil {
			return "", nil, err
		}
		if ch == protocol.ChChunk {
			var c protocol.Chunk
			_ = json.Unmarshal(plain, &c)
			whole, err := p.asm.Add(c)
			if err != nil {
				return "", nil, err
			}
			if whole == nil {
				continue
			}
			ch, _ = protocol.PeekChannel(whole)
			return ch, whole, nil
		}
		return ch, plain, nil
	}
}

// waitFor reads until a message on channel ch satisfies pred.
func (p *phoneClient) waitFor(ctx context.Context, ch string, pred func([]byte) bool) ([]byte, error) {
	for {
		got, plain, err := p.recv(ctx)
		if err != nil {
			return nil, err
		}
		if got == ch && pred(plain) {
			return plain, nil
		}
	}
}

func TestPairThenTunnel(t *testing.T) {
	srv, hs := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	phone, _ := protocol.NewIdentity(nil)

	// Unknown phone without a code is refused before any gateway socket opens.
	if p, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil); err == nil {
		if _, _, err := p.recv(ctx); err == nil {
			t.Fatal("unpaired phone got a tunnel")
		}
	}

	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	p, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	// gateway.ready is forwarded verbatim on the ws channel.
	plain, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") })
	if err != nil {
		t.Fatalf("no gateway.ready: %v", err)
	}
	var m protocol.WSMessage
	_ = json.Unmarshal(plain, &m)
	if !strings.Contains(m.Data, `"replay_epoch"`) {
		t.Fatalf("unexpected ready frame %q", m.Data)
	}
	// RPC round trip.
	if err := p.send(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: `{"jsonrpc":"2.0","id":7,"method":"session.list","params":{}}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool {
		return strings.Contains(string(b), `\"id\":7`) || strings.Contains(string(b), `\"id\": 7`)
	}); err != nil {
		t.Fatalf("no RPC reply: %v", err)
	}
	// HTTP proxy: allowed route works and is token-authenticated; unknown route is refused.
	_ = p.send(ctx, protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: 1, Method: "GET", Path: "/api/sessions", Query: "limit=3"})
	plain, err = p.waitFor(ctx, protocol.ChHTTP, func([]byte) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	var resp protocol.HTTPResponse
	_ = json.Unmarshal(plain, &resp)
	if resp.ID != 1 || resp.Status != 200 || !strings.Contains(string(resp.Body), `"limit":"3"`) {
		t.Fatalf("bad http response: %+v %s", resp, resp.Body)
	}
	_ = p.send(ctx, protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: 2, Method: "GET", Path: "/api/secret"})
	plain, _ = p.waitFor(ctx, protocol.ChHTTP, func([]byte) bool { return true })
	_ = json.Unmarshal(plain, &resp)
	if resp.ID != 2 || resp.Status != http.StatusForbidden {
		t.Fatalf("disallowed path not refused: %+v", resp)
	}
	// Ping/pong and device naming.
	_ = p.send(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlPing})
	if _, err := p.waitFor(ctx, protocol.ChCtl, func(b []byte) bool { return strings.Contains(string(b), `"pong"`) }); err != nil {
		t.Fatal(err)
	}
	_ = p.send(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlName, Reason: "Test iPhone"})
	time.Sleep(100 * time.Millisecond)
	devices, _ := srv.Store.Devices()
	if len(devices) != 1 || devices[0].Name != "Test iPhone" || devices[0].ID != protocol.DeviceID(phone.Public()) {
		t.Fatalf("device not recorded: %+v", devices)
	}
	if srv.Pairings.Pending() != 0 {
		t.Fatal("pairing code not consumed")
	}
	p.ws.Close(websocket.StatusNormalClosure, "")

	// The now-trusted phone reconnects with no code.
	p2, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		t.Fatalf("trusted reconnect failed: %v", err)
	}
	// Revocation disconnects it.
	if _, err := srv.Store.Revoke(protocol.DeviceID(phone.Public())[:6]); err != nil {
		t.Fatal(err)
	}
	srv.DisconnectDevice(protocol.DeviceID(phone.Public()))
	if _, _, err := p2.recv(ctx); err == nil {
		t.Fatal("revoked device still connected")
	}
	if _, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil); err == nil {
		t.Log("handshake accepted at transport level; tunnel must not open")
	}
}

func TestLargeFramesAreChunked(t *testing.T) {
	srv, hs := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	phone, _ := protocol.NewIdentity(nil)
	code, _, _ := srv.Pairings.Issue(time.Minute)
	p, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", protocol.ChunkThreshold+1000)
	// The fake gateway echoes the method name back; send a huge method so the
	// reply exceeds the chunk threshold on the way back.
	_ = p.send(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: `{"jsonrpc":"2.0","id":9,"method":"` + big + `","params":{}}`})
	plain, err := p.waitFor(ctx, protocol.ChWS, func(b []byte) bool { return len(b) > protocol.ChunkThreshold })
	if err != nil {
		t.Fatal(err)
	}
	var m protocol.WSMessage
	_ = json.Unmarshal(plain, &m)
	if !strings.Contains(m.Data, big) {
		t.Fatal("chunked reply corrupted")
	}
}
