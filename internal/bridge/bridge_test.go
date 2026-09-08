package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
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
	return newTestServerWithHooksAndLogger(t, nil, nil)
}

func newTestServerWithHooks(t *testing.T, hooks *admissionTestHooks) (*Server, *httptest.Server) {
	return newTestServerWithHooksAndLogger(t, hooks, nil)
}

func newTestServerWithHooksAndLogger(t *testing.T, hooks *admissionTestHooks, logger *slog.Logger) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("HERMES_HOME", t.TempDir())
	st, err := state.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close Store: %v", err)
		}
	})
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
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	srv := New(id, st, sup, logger)
	srv.testHooks = hooks
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, hs
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func receiveTestValue[T any](t *testing.T, ctx context.Context, ch <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", label, ctx.Err())
		var zero T
		return zero
	}
}

// phoneClient is a minimal Go stand-in for the iOS BridgeLink.
type phoneClient struct {
	ws    *websocket.Conn
	suite *protocol.Suite
	asm   protocol.ChunkAssembler
}

func connectPhone(t *testing.T, ctx context.Context, hs *httptest.Server, bridgeID *protocol.Identity, phone *protocol.Identity, code []byte) (*phoneClient, error) {
	t.Helper()
	return connectPhoneURL(t, ctx, "ws"+strings.TrimPrefix(hs.URL, "http")+"/v1/bridge", bridgeID, phone, code)
}

// connectPhoneURL dials any bridge endpoint — direct (/v1/bridge) or a relay's
// /v1/phone?s=… — and runs the phone side of the handshake.
func connectPhoneURL(t *testing.T, ctx context.Context, url string, bridgeID *protocol.Identity, phone *protocol.Identity, code []byte) (*phoneClient, error) {
	t.Helper()
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = ws.Close(websocket.StatusInternalError, "phone setup failed")
		}
	}()
	ws.SetReadLimit(maxFrame)
	hello, ps, err := protocol.PhoneHello(phone, bridgeID.SessionID(), nil)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(hello)
	if err != nil {
		return nil, err
	}
	if err := ws.Write(ctx, websocket.MessageText, raw); err != nil {
		return nil, err
	}
	_, acceptRaw, err := ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	var accept protocol.Accept
	if err := json.Unmarshal(acceptRaw, &accept); err != nil {
		return nil, err
	}
	confirm, suite, err := ps.Finish(accept, bridgeID.Public(), code)
	if err != nil {
		return nil, err
	}
	if err := ws.Write(ctx, websocket.MessageBinary, confirm); err != nil {
		return nil, err
	}
	failed = false
	return &phoneClient{ws: ws, suite: suite}, nil
}

func admissionAccepted(p *phoneClient, ctx context.Context) bool {
	ch, plain, err := p.recv(ctx)
	if err != nil || ch != protocol.ChCtl {
		return false
	}
	var ctl protocol.CtlMessage
	return json.Unmarshal(plain, &ctl) == nil && ctl.Op == protocol.CtlAccepted
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
	dropped := make(chan error, 1)
	go func() {
		_, _, err := p2.recv(ctx)
		dropped <- err
	}()
	if _, err := srv.Revoke(protocol.DeviceID(phone.Public())[:6]); err != nil {
		t.Fatal(err)
	}
	if err := receiveTestValue(t, ctx, dropped, "revoked connection close"); err == nil {
		t.Fatal("revoked device still connected")
	}
	if _, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil); err == nil {
		t.Log("handshake accepted at transport level; tunnel must not open")
	}
}

func TestOnePairingCodeAdmitsExactlyOneConcurrentPhone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	hooks := &admissionTestHooks{afterDecision: func() {
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}
	srv, hs := newTestServerWithHooks(t, hooks)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	phones := make([]*protocol.Identity, 2)
	for i := range phones {
		phones[i], _ = protocol.NewIdentity(nil)
	}
	type result struct {
		index  int
		client *phoneClient
		err    error
	}
	connected := make(chan result, 2)
	for i := range phones {
		go func(i int) {
			client, err := connectPhone(t, ctx, hs, srv.Identity, phones[i], code)
			connected <- result{i, client, err}
		}(i)
	}
	clients := make([]*phoneClient, 2)
	for range clients {
		result := receiveTestValue(t, ctx, connected, "concurrent phone setup")
		if result.err != nil {
			t.Fatalf("phone %d setup: %v", result.index, result.err)
		}
		clients[result.index] = result.client
	}
	receiveTestValue(t, ctx, arrived, "first pairing contender")
	receiveTestValue(t, ctx, arrived, "second pairing contender")
	releaseOnce.Do(func() { close(release) })

	accepted := make(chan result, 2)
	for i, client := range clients {
		go func(i int, client *phoneClient) {
			if admissionAccepted(client, ctx) {
				accepted <- result{index: i, client: client}
				return
			}
			accepted <- result{index: -1, client: client}
		}(i, client)
	}
	winner, loser := -1, -1
	for range clients {
		result := receiveTestValue(t, ctx, accepted, "concurrent admission result")
		if result.index >= 0 {
			if winner >= 0 {
				t.Fatal("both phones were admitted with one code")
			}
			winner = result.index
		} else if result.client == clients[0] {
			loser = 0
		} else {
			loser = 1
		}
	}
	if winner < 0 || loser < 0 || winner == loser {
		t.Fatalf("winner=%d loser=%d", winner, loser)
	}
	devices, err := srv.Store.Devices()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 || devices[0].ID != protocol.DeviceID(phones[winner].Public()) {
		t.Fatalf("trusted devices = %+v, winner %d", devices, winner)
	}
	if srv.Pairings.Pending() != 0 {
		t.Fatal("single-use pairing code remained outstanding")
	}

	retry, err := connectPhone(t, ctx, hs, srv.Identity, phones[loser], nil)
	if err == nil {
		if admissionAccepted(retry, ctx) {
			t.Fatal("losing phone reconnected without a new code")
		}
		_ = retry.ws.Close(websocket.StatusNormalClosure, "")
	}
	for _, client := range clients {
		_ = client.ws.Close(websocket.StatusNormalClosure, "")
	}
}

func TestCorruptTrustStateConsumesCodeAndRefusesAdmission(t *testing.T) {
	srv, hs := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := os.WriteFile(srv.Store.Path("devices.json"), []byte("{corrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	phone, _ := protocol.NewIdentity(nil)
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	client, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	if admissionAccepted(client, ctx) {
		t.Fatal("corrupt trust state admitted a new phone")
	}
	if srv.Pairings.Pending() != 0 {
		t.Fatal("failed persistence restored the consumed code")
	}
	if err := os.WriteFile(srv.Store.Path("devices.json"), []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	retry, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err == nil && admissionAccepted(retry, ctx) {
		t.Fatal("consumed code succeeded after trust state was repaired")
	}
	if retry != nil {
		_ = retry.ws.Close(websocket.StatusNormalClosure, "")
	}
}

func TestCommittedPairingDiagnosticAcceptsAndWarns(t *testing.T) {
	diagnostic := errors.New("injected committed pairing diagnostic")
	hooks := &admissionTestHooks{addTrustedResult: func(err error) error {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: %w", state.ErrMutationCommitted, diagnostic)
	}}
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, hs := newTestServerWithHooksAndLogger(t, hooks, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	phone, _ := protocol.NewIdentity(nil)
	deviceID := protocol.DeviceID(phone.Public())
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil || !admissionAccepted(first, ctx) {
		t.Fatalf("committed pairing was not accepted: %v", err)
	}
	if srv.Pairings.Pending() != 0 {
		t.Fatal("committed pairing restored its one-time code")
	}
	devices, err := srv.Store.Devices()
	if err != nil || len(devices) != 1 || devices[0].ID != deviceID {
		t.Fatalf("committed pairing record = %+v, %v", devices, err)
	}
	reconnect, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil)
	if err != nil || !admissionAccepted(reconnect, ctx) {
		t.Fatalf("proof-less reconnect after committed pairing: %v", err)
	}
	logText := logs.String()
	for _, want := range []string{"paired device trust committed with post-commit diagnostic", deviceID, diagnostic.Error()} {
		if !strings.Contains(logText, want) {
			t.Fatalf("pairing warning missing %q: %s", want, logText)
		}
	}
	_ = first.ws.Close(websocket.StatusNormalClosure, "")
	_ = reconnect.ws.Close(websocket.StatusNormalClosure, "")
}

func TestCommittedRevocationDiagnosticReconcilesBeforeReturn(t *testing.T) {
	diagnostic := errors.New("injected committed revocation diagnostic")
	hooks := &admissionTestHooks{revokeResult: func(dev state.Device, err error) (state.Device, error) {
		if err != nil {
			return dev, err
		}
		return dev, fmt.Errorf("%w: %w", state.ErrMutationCommitted, diagnostic)
	}}
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, hs := newTestServerWithHooksAndLogger(t, hooks, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	phone, _ := protocol.NewIdentity(nil)
	deviceID := protocol.DeviceID(phone.Public())
	code, _, _ := srv.Pairings.Issue(time.Minute)
	client, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil || !admissionAccepted(client, ctx) {
		t.Fatalf("initial pairing failed: %v", err)
	}
	removed, err := srv.Revoke(deviceID)
	if removed.ID != deviceID || !errors.Is(err, state.ErrMutationCommitted) || !errors.Is(err, diagnostic) {
		t.Fatalf("committed revoke result = %+v, %v", removed, err)
	}
	if closeErr := readUntilPhoneClose(client, ctx); closeErr == nil {
		t.Fatal("live connection was not closed before committed Revoke returned")
	}
	devices, readErr := srv.Store.Devices()
	if readErr != nil || len(devices) != 0 {
		t.Fatalf("devices after committed revoke = %+v, %v", devices, readErr)
	}
	retry, dialErr := connectPhone(t, ctx, hs, srv.Identity, phone, nil)
	if dialErr == nil {
		if admissionAccepted(retry, ctx) {
			t.Fatal("revoked phone reconnected without proof")
		}
		_ = retry.ws.Close(websocket.StatusNormalClosure, "")
	}
	if retryRemoved, retryErr := srv.Revoke(deviceID); retryRemoved != (state.Device{}) || retryErr == nil || errors.Is(retryErr, state.ErrMutationCommitted) {
		t.Fatalf("revoke retry result = %+v, %v", retryRemoved, retryErr)
	}
	logText := logs.String()
	for _, want := range []string{"device revocation committed with post-commit diagnostic", deviceID, "closed_connections=1", diagnostic.Error()} {
		if !strings.Contains(logText, want) {
			t.Fatalf("revocation warning missing %q: %s", want, logText)
		}
	}
}

func TestAdmissionRevalidatesSuccessfullyAfterUnrelatedRevocation(t *testing.T) {
	var revokeOnce sync.Once
	revokeResult := make(chan error, 1)
	unrelated := bytes.Repeat([]byte{0xE1}, 32)
	hooks := &admissionTestHooks{beforePublish: func(srv *Server) {
		revokeOnce.Do(func() {
			_, err := srv.Revoke(protocol.DeviceID(unrelated))
			revokeResult <- err
		})
	}}
	srv, hs := newTestServerWithHooks(t, hooks)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Store.AddTrusted(unrelated, "unrelated"); err != nil {
		t.Fatal(err)
	}
	phone, _ := protocol.NewIdentity(nil)
	deviceID := protocol.DeviceID(phone.Public())
	code, _, _ := srv.Pairings.Issue(time.Minute)
	client, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil || !admissionAccepted(client, ctx) {
		t.Fatalf("admission did not survive unrelated generation change: %v", err)
	}
	if err := receiveTestValue(t, ctx, revokeResult, "unrelated revoke hook"); err != nil {
		t.Fatal(err)
	}
	online := srv.OnlineDevices()
	if len(online) != 1 || online[0] != deviceID {
		t.Fatalf("published devices = %v, want %s", online, deviceID)
	}
	devices, err := srv.Store.Devices()
	if err != nil || len(devices) != 1 || devices[0].ID != deviceID {
		t.Fatalf("trusted devices after revalidation = %+v, %v", devices, err)
	}
	_ = client.ws.Close(websocket.StatusNormalClosure, "")
}

func TestPairingPublicationChurnRejectsAndWarnsPersistedTrust(t *testing.T) {
	hooks := &admissionTestHooks{beforePublish: func(srv *Server) {
		srv.mu.Lock()
		srv.revocationGeneration++
		srv.mu.Unlock()
	}}
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, hs := newTestServerWithHooksAndLogger(t, hooks, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	phone, _ := protocol.NewIdentity(nil)
	deviceID := protocol.DeviceID(phone.Public())
	code, _, _ := srv.Pairings.Issue(time.Minute)
	client, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	if admissionAccepted(client, ctx) {
		t.Fatal("persistent generation churn admitted phone")
	}
	if srv.Pairings.Pending() != 0 {
		t.Fatal("rejected churn pairing restored its code")
	}
	devices, err := srv.Store.Devices()
	if err != nil || len(devices) != 1 || devices[0].ID != deviceID {
		t.Fatalf("persisted trust after churn rejection = %+v, %v", devices, err)
	}
	logText := logs.String()
	for _, want := range []string{"pairing admission failed; persisted trust may remain", deviceID, "admission rejected: revocation state kept changing", "persisted_trust_may_remain=true"} {
		if !strings.Contains(logText, want) {
			t.Fatalf("churn warning missing %q: %s", want, logText)
		}
	}
	_ = client.ws.Close(websocket.StatusNormalClosure, "")
}

func readUntilPhoneClose(client *phoneClient, ctx context.Context) error {
	for {
		if _, _, err := client.recv(ctx); err != nil {
			return err
		}
	}
}

func TestRevokeRejectsHandshakePausedBeforePublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	paused := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var decisions atomic.Int32
	var pauseOnce sync.Once
	hooks := &admissionTestHooks{afterDecision: func() {
		if decisions.Add(1) == 2 {
			pauseOnce.Do(func() { close(paused) })
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}}
	srv, hs := newTestServerWithHooks(t, hooks)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	phone, _ := protocol.NewIdentity(nil)
	code, _, _ := srv.Pairings.Issue(time.Minute)
	first, err := connectPhone(t, ctx, hs, srv.Identity, phone, code)
	if err != nil || !admissionAccepted(first, ctx) {
		t.Fatalf("initial pairing failed: %v", err)
	}
	_ = first.ws.Close(websocket.StatusNormalClosure, "")
	deadline := time.Now().Add(2 * time.Second)
	for srv.ConnectionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.ConnectionCount() != 0 {
		t.Fatal("initial connection did not leave before revocation interleaving")
	}

	second, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	receiveTestValue(t, ctx, paused, "known-phone admission pause")
	const observerCount = 24
	observeStart := make(chan struct{})
	observed := make(chan struct{}, observerCount)
	for range observerCount {
		go func() {
			<-observeStart
			_ = srv.OnlineDevices()
			srv.DisconnectDevice("unrelated-device")
			observed <- struct{}{}
		}()
	}
	close(observeStart)
	if _, err := srv.Revoke(protocol.DeviceID(phone.Public())); err != nil {
		t.Fatal(err)
	}
	for range observerCount {
		receiveTestValue(t, ctx, observed, "finite concurrent observer")
	}
	releaseOnce.Do(func() { close(release) })
	if admissionAccepted(second, ctx) {
		t.Fatal("handshake survived a completed concurrent revoke")
	}
	devices, err := srv.Store.Devices()
	if err != nil || len(devices) != 0 {
		t.Fatalf("devices after revoke = %+v, err %v", devices, err)
	}

	freshCode, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := connectPhone(t, ctx, hs, srv.Identity, phone, freshCode)
	if err != nil || !admissionAccepted(fresh, ctx) {
		t.Fatalf("deliberate fresh-code re-pair failed: %v", err)
	}
	_ = fresh.ws.Close(websocket.StatusNormalClosure, "")
}

func TestAdmissionPublicationRejectsPersistentRevocationChurn(t *testing.T) {
	store, err := state.OpenAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close Store: %v", err)
		}
	})
	phone := make([]byte, 32)
	for i := range phone {
		phone[i] = 0xD4
	}
	if err := store.AddTrusted(phone, "churn"); err != nil {
		t.Fatal(err)
	}
	srv := &Server{Store: store, conns: map[*conn]struct{}{}}
	srv.testHooks = &admissionTestHooks{beforePublish: func(srv *Server) {
		srv.mu.Lock()
		srv.revocationGeneration++
		srv.mu.Unlock()
	}}
	c := &conn{srv: srv}
	if err := srv.publishAdmission(c, phone, srv.admissionGeneration()); err == nil {
		t.Fatal("persistent generation churn did not bound admission publication")
	}
	if c.deviceID != "" {
		t.Fatalf("device published during persistent churn: %s", c.deviceID)
	}
}

type blockingCloseLink struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingCloseLink) Read(context.Context) (websocket.MessageType, []byte, error) {
	return 0, nil, errors.New("unused")
}

func (l *blockingCloseLink) Write(context.Context, websocket.MessageType, []byte) error {
	return errors.New("unused")
}

func (l *blockingCloseLink) Close(websocket.StatusCode, string) error {
	l.once.Do(func() { close(l.started) })
	<-l.release
	return nil
}

func TestDisconnectDeviceDoesNotHoldServerLockWhileClosing(t *testing.T) {
	link := &blockingCloseLink{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(link.release) }) })
	srv := &Server{conns: map[*conn]struct{}{}}
	c := &conn{srv: srv, ws: link, deviceID: "phone"}
	srv.conns[c] = struct{}{}
	disconnected := make(chan struct{}, 1)
	go func() {
		srv.DisconnectDevice("phone")
		close(disconnected)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receiveTestValue(t, ctx, link.started, "blocking close")
	counted := make(chan int, 1)
	go func() { counted <- srv.ConnectionCount() }()
	select {
	case count := <-counted:
		if count != 1 {
			t.Fatalf("connection count = %d", count)
		}
	case <-time.After(time.Second):
		releaseOnce.Do(func() { close(link.release) })
		t.Fatal("ConnectionCount blocked behind websocket Close")
	}
	releaseOnce.Do(func() { close(link.release) })
	receiveTestValue(t, ctx, disconnected, "disconnect completion")
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
