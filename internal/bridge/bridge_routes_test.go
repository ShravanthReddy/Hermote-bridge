package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
	"github.com/ShravanthReddy/Hermote-bridge/internal/update"
)

var updateGolden = flag.Bool("update", false, "rewrite the bridge route goldens consumed by the Swift tests")

const testIncarnation = "5f0c6a9e2b7d4c1a8e3f9b6d2a7c4e10"

// statusProvider answers from fixed facts; health, when set, supplies the
// gateway health live at request time.
type statusProvider struct {
	facts  StatusFacts
	health func() gateway.Health
}

func (p statusProvider) BridgeCapability() protocol.BridgeCapability {
	return NewBridgeCapability(p.facts.Version, p.facts.Incarnation)
}

func (p statusProvider) BridgeStatus(context.Context) BridgeStatus {
	facts := p.facts
	if p.health != nil {
		facts.Gateway = p.health()
	}
	return NewBridgeStatus(facts)
}

func fixed(facts StatusFacts) func(*Server) StatusProvider {
	return func(*Server) StatusProvider { return statusProvider{facts: facts} }
}

func goldenFacts() StatusFacts {
	start := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	return StatusFacts{
		Version: "0.15.0", Incarnation: testIncarnation, StartedAt: start,
		GOOS: "darwin", GOARCH: "arm64",
		Transport: protocol.TransportRelay, RelayURL: "wss://relay.hermote.app", RelayAttached: true,
		Install: update.Install{
			Method: update.MethodManual, Path: "/Users/x/.local/bin/hermote-bridge",
			UpdateCommand: update.ManualUpdateCommand("/Users/x/.local/bin"),
		},
		ServiceLabel: "ai.hermes.remote",
		Gateway: gateway.Health{
			State: gateway.StateReady, Since: start.Add(5 * time.Second), LastProbeOK: start.Add(15 * time.Minute),
			Restarts: 1, LastRestartReason: gateway.RestartUnresponsive,
		},
		Paired: 2, Connected: 1,
	}
}

// statusTestServer is newTestServer with a StatusProvider installed before
// serving and a gateway that counts every request under /bridge/. provide
// may be nil for a bridge without one.
func statusTestServer(t *testing.T, provide func(*Server) StatusProvider) (*Server, *httptest.Server, *atomic.Int32) {
	t.Helper()
	t.Setenv("HERMES_HOME", t.TempDir())
	st, err := state.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	id, err := st.Identity()
	if err != nil {
		t.Fatal(err)
	}
	sup, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: os.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	inner := fakeGateway(t, sup.Token())
	t.Cleanup(inner.Close)
	var bridgeHits atomic.Int32
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/bridge") {
			bridgeHits.Add(1)
			_, _ = w.Write([]byte(`{"proxied":true}`))
			return
		}
		inner.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(gw.Close)
	gateway.ForceReadyForTest(sup, gw.URL[strings.LastIndex(gw.URL, ":")+1:])
	srv := New(id, st, sup, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if provide != nil {
		srv.Status = provide(srv)
	}
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, hs, &bridgeHits
}

// trustedPhone connects an already paired phone and returns it with the raw
// `ctl accepted` plaintext.
func trustedPhone(t *testing.T, ctx context.Context, srv *Server, hs *httptest.Server) (*phoneClient, []byte) {
	t.Helper()
	phone, _ := protocol.NewIdentity(nil)
	if err := srv.Store.AddTrusted(phone.Public(), "Status test phone"); err != nil {
		t.Fatal(err)
	}
	p, err := connectPhone(t, ctx, hs, srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.ws.Close(websocket.StatusNormalClosure, "") })
	accepted, err := p.waitFor(ctx, protocol.ChCtl, func(b []byte) bool { return strings.Contains(string(b), `"accepted"`) })
	if err != nil {
		t.Fatalf("no ctl accepted: %v", err)
	}
	return p, accepted
}

func bridgeCall(t *testing.T, ctx context.Context, p *phoneClient, id uint64, method, path, query string) protocol.HTTPResponse {
	t.Helper()
	if err := p.send(ctx, protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: method, Path: path, Query: query}); err != nil {
		t.Fatal(err)
	}
	plain, err := p.waitFor(ctx, protocol.ChHTTP, func(b []byte) bool {
		var resp protocol.HTTPResponse
		return json.Unmarshal(b, &resp) == nil && resp.ID == id
	})
	if err != nil {
		t.Fatalf("no reply to %s %s: %v", method, path, err)
	}
	var resp protocol.HTTPResponse
	if err := json.Unmarshal(plain, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestBridgeStatusIsServedLocallyAndNeverProxied(t *testing.T) {
	srv, hs, bridgeHits := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/status", "")
	if resp.Status != http.StatusOK {
		t.Fatalf("status %d %s", resp.Status, resp.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(resp.Body, &body); err != nil || body["version"] != "0.15.0" {
		t.Fatalf("status body %s", resp.Body)
	}
	if hits := bridgeHits.Load(); hits != 0 {
		t.Fatalf("the gateway saw %d /bridge/ requests", hits)
	}
}

func TestBridgeStatusReportsVersionUptimeTransportAndHermesHealth(t *testing.T) {
	facts := goldenFacts()
	facts.Transport, facts.RelayURL, facts.RelayAttached = protocol.TransportDirect, "", false
	srv, hs, _ := statusTestServer(t, func(srv *Server) StatusProvider {
		return statusProvider{facts: facts, health: srv.Gateway.Health}
	})
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/status", "")
	var got struct {
		API       int     `json:"api"`
		Version   string  `json:"version"`
		StartedAt string  `json:"started_at"`
		Transport string  `json:"transport"`
		Relay     *string `json:"relay"`
		Gateway   struct {
			State             string  `json:"state"`
			Since             *string `json:"since"`
			LastProbeOKAt     *string `json:"last_probe_ok_at"`
			Restarts          int     `json:"restarts"`
			LastRestartReason *string `json:"last_restart_reason"`
		} `json:"gateway"`
	}
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		t.Fatalf("%v: %s", err, resp.Body)
	}
	if got.API != 1 || got.Version != "0.15.0" || got.StartedAt != "2026-09-27T10:00:00Z" {
		t.Fatalf("identity rows %+v", got)
	}
	if got.Transport != "direct" || got.Relay != nil {
		t.Fatalf("direct transport reported as %q relay=%v", got.Transport, got.Relay)
	}
	if got.Gateway.State != "ready" || got.Gateway.Since == nil || got.Gateway.Restarts != 0 ||
		got.Gateway.LastRestartReason != nil || got.Gateway.LastProbeOKAt != nil {
		t.Fatalf("hermes health %+v in %s", got.Gateway, resp.Body)
	}
}

func TestRelayStatusReportsURLAndAttachment(t *testing.T) {
	facts := goldenFacts()
	facts.RelayAttached = false
	srv, hs, _ := statusTestServer(t, fixed(facts))
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/status", "")
	var got struct {
		Transport string `json:"transport"`
		Relay     *struct {
			URL      string `json:"url"`
			Attached bool   `json:"attached"`
		} `json:"relay"`
	}
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Transport != "relay" || got.Relay == nil || got.Relay.URL != "wss://relay.hermote.app" || got.Relay.Attached {
		t.Fatalf("relay status %s", resp.Body)
	}
}

func TestUnknownBridgeRouteIs404(t *testing.T) {
	srv, hs, bridgeHits := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	for i, call := range []struct{ method, path string }{
		{http.MethodGet, "/bridge/v1/nope"},
		{http.MethodPost, "/bridge/v1/status"},
		{http.MethodGet, "/bridge/v2/status"},
	} {
		resp := bridgeCall(t, ctx, p, uint64(i+1), call.method, call.path, "")
		if resp.Status != http.StatusNotFound || string(resp.Body) != UnknownBridgeRouteError {
			t.Fatalf("%s %s = %d %s", call.method, call.path, resp.Status, resp.Body)
		}
	}
	if hits := bridgeHits.Load(); hits != 0 {
		t.Fatalf("the gateway saw %d /bridge/ requests", hits)
	}
}

func TestGatewayRoutesOutsideAllowListStillRefused(t *testing.T) {
	srv, hs, bridgeHits := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	for i, path := range []string{"/api/secret", "/bridge", "/bridgex/v1/status"} {
		resp := bridgeCall(t, ctx, p, uint64(i+1), http.MethodGet, path, "")
		if resp.Status != http.StatusForbidden || string(resp.Body) != RefusedRouteError {
			t.Fatalf("GET %s = %d %s", path, resp.Status, resp.Body)
		}
	}
	if resp := bridgeCall(t, ctx, p, 9, http.MethodGet, "/api/sessions", "limit=1"); resp.Status != http.StatusOK {
		t.Fatalf("allowed route %d %s", resp.Status, resp.Body)
	}
	if hits := bridgeHits.Load(); hits != 0 {
		t.Fatalf("the gateway saw %d /bridge/ requests", hits)
	}
}

func TestAcceptedAdvertisesBridgeCapability(t *testing.T) {
	srv, hs, _ := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	_, accepted := trustedPhone(t, ctx, srv, hs)
	want := `"bridge":{"api":1,"version":"0.15.0","incarnation":"` + testIncarnation +
		`","features":{"status":true,"update":false}}`
	if !strings.Contains(string(accepted), want) {
		t.Fatalf("accepted %s lacks %s", accepted, want)
	}

	// A bridge without a status provider is a legacy bridge: no capability
	// and no bridge routes.
	legacy, legacyHS, _ := statusTestServer(t, nil)
	p, legacyAccepted := trustedPhone(t, ctx, legacy, legacyHS)
	if strings.Contains(string(legacyAccepted), `"bridge"`) {
		t.Fatalf("legacy accepted %s advertises a bridge capability", legacyAccepted)
	}
	if resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/status", ""); resp.Status != http.StatusForbidden ||
		string(resp.Body) != RefusedRouteError {
		t.Fatalf("legacy bridge answered its status route: %d %s", resp.Status, resp.Body)
	}
}

func TestLegacyPhoneIgnoresBridgeCapabilityAndProxiesAsBefore(t *testing.T) {
	srv, hs, _ := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	p, accepted := trustedPhone(t, ctx, srv, hs)
	// The control message as a phone built before this change decodes it.
	var legacy struct {
		Ch   string `json:"ch"`
		Op   string `json:"op"`
		Caps *struct {
			AttachmentBlob *protocol.AttachmentBlobCapability `json:"attachment_blob,omitempty"`
		} `json:"caps,omitempty"`
	}
	if err := json.Unmarshal(accepted, &legacy); err != nil || legacy.Ch != protocol.ChCtl || legacy.Op != protocol.CtlAccepted {
		t.Fatalf("pre-change decode of %s: %v", accepted, err)
	}
	if legacy.Caps == nil || legacy.Caps.AttachmentBlob == nil || legacy.Caps.AttachmentBlob.Version != blobProtocolVersion {
		t.Fatalf("attachment capability lost beside the bridge capability: %s", accepted)
	}
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/api/sessions", "limit=3")
	if resp.Status != http.StatusOK || !strings.Contains(string(resp.Body), `"limit":"3"`) {
		t.Fatalf("allowed REST call through a new bridge: %d %s", resp.Status, resp.Body)
	}
}

func TestBridgeStatusGolden(t *testing.T) {
	srv, hs, _ := statusTestServer(t, fixed(goldenFacts()))
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/status", "")
	checkGolden(t, "bridge-status-v1.json", resp.Body)
}

// checkGolden compares a route body with testdata/<name>, stored indented.
// -update rewrites it and, inside the app repository, HermesKit's fixture copy.
func checkGolden(t *testing.T, name string, body json.RawMessage) {
	t.Helper()
	var indented bytes.Buffer
	if err := json.Indent(&indented, body, "", "  "); err != nil {
		t.Fatalf("route body is not JSON: %v: %s", err, body)
	}
	indented.WriteByte('\n')
	path := filepath.Join("testdata", name)
	if *updateGolden {
		if err := os.WriteFile(path, indented.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		mirror := filepath.Join("..", "..", "..", "HermesKit", "Tests", "HermesKitTests", "Fixtures")
		if _, err := os.Stat(mirror); err == nil {
			if err := os.WriteFile(filepath.Join(mirror, name), indented.Bytes(), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden (%v); run: go test ./internal/bridge -run Golden -update", err)
	}
	if !bytes.Equal(want, indented.Bytes()) {
		t.Fatalf("golden %s changed; if intentional run: go test ./internal/bridge -run Golden -update\n--- want\n%s\n--- got\n%s",
			name, want, indented.Bytes())
	}
}

// checkingProvider also answers the update check with a fixed result and
// records every force flag it was asked with.
type checkingProvider struct {
	statusProvider
	result update.Result
	forced chan bool
}

func (p checkingProvider) CheckForUpdate(_ context.Context, force bool) update.CheckResponse {
	p.forced <- force
	return p.result.Response(p.facts.Install)
}

func checking(result update.Result) (func(*Server) StatusProvider, chan bool) {
	forced := make(chan bool, 16)
	return func(*Server) StatusProvider {
		return checkingProvider{statusProvider: statusProvider{facts: goldenFacts()}, result: result, forced: forced}
	}, forced
}

func TestCheckGolden(t *testing.T) {
	provide, _ := checking(update.Result{
		Current: "0.15.0", CheckedAt: time.Date(2026, 9, 28, 9, 30, 0, 0, time.UTC),
		Latest: &update.LatestRelease{
			Version: "0.16.0", Tag: "v0.16.0", PublishedAt: time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC),
			Notes: []string{"Serve bridge status to paired phones"}, More: 0,
		},
	})
	srv, hs, bridgeHits := statusTestServer(t, provide)
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/update/check", "")
	if resp.Status != http.StatusOK || bridgeHits.Load() != 0 {
		t.Fatalf("check %d %s (gateway saw %d)", resp.Status, resp.Body, bridgeHits.Load())
	}
	checkGolden(t, "bridge-update-check-v1.json", resp.Body)
}

func TestFirstFailedCheckGolden(t *testing.T) {
	provide, _ := checking(update.Result{
		Current: "0.15.0",
		Err: &update.CheckError{
			Kind: update.ErrorNetwork, Message: "DNS lookup for api.github.com failed — check your connection or proxy.",
		},
	})
	srv, hs, _ := statusTestServer(t, provide)
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	resp := bridgeCall(t, ctx, p, 1, http.MethodGet, "/bridge/v1/update/check", "force=true")
	checkGolden(t, "bridge-update-check-cold-failure-v1.json", resp.Body)
}

func TestUpdateCheckForcesOnlyOnLiteralQuery(t *testing.T) {
	provide, forced := checking(update.Result{Current: "0.15.0"})
	srv, hs, _ := statusTestServer(t, provide)
	ctx := testContext(t)
	p, _ := trustedPhone(t, ctx, srv, hs)
	for i, tc := range []struct {
		query string
		force bool
	}{{"force=true", true}, {"", false}, {"force=1", false}, {"force=true&x=1", false}, {"force=TRUE", false}} {
		if resp := bridgeCall(t, ctx, p, uint64(i+1), http.MethodGet, "/bridge/v1/update/check", tc.query); resp.Status != http.StatusOK {
			t.Fatalf("?%s = %d %s", tc.query, resp.Status, resp.Body)
		}
		if got := <-forced; got != tc.force {
			t.Fatalf("?%s forced=%v, want %v", tc.query, got, tc.force)
		}
	}
}
