package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
	"github.com/ShravanthReddy/Hermote-bridge/internal/update"
)

// liveStatusRoot is the only root the status fixture accepts.
const liveStatusRoot = "/private/tmp/hermote-bridge-status-live"

// liveReleaseSource stands in for GitHub: the fixture never leaves loopback.
type liveReleaseSource struct{}

func (liveReleaseSource) Latest(context.Context) (update.Release, error) {
	return update.Release{
		Tag: "v0.16.0", PublishedAt: time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC),
		Body: "## Changelog\n* 0123456: Serve bridge status to paired phones (Fixture <f@x>)\n",
	}, nil
}

// liveStatusProvider is a daemon's status provider over the fixture's
// isolated home, test identities and fake release source.
type liveStatusProvider struct {
	srv     *Server
	started time.Time
	checker *update.Checker
}

func (p *liveStatusProvider) install() update.Install {
	return update.Install{
		Method: update.MethodManual, Path: "/Users/fixture/.local/bin/hermote-bridge",
		UpdateCommand: update.ManualUpdateCommand("/Users/fixture/.local/bin"),
	}
}

func (p *liveStatusProvider) BridgeCapability() protocol.BridgeCapability {
	return NewBridgeCapability("0.15.0", testIncarnation)
}

func (p *liveStatusProvider) BridgeStatus(context.Context) BridgeStatus {
	devices, _ := p.srv.Store.Devices()
	return NewBridgeStatus(StatusFacts{
		Version: "0.15.0", Incarnation: testIncarnation, StartedAt: p.started,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Transport: protocol.TransportDirect,
		Install: p.install(), ServiceLabel: "ai.hermes.remote", Gateway: p.srv.Gateway.Health(),
		Paired: len(devices), Connected: p.srv.ConnectionCount(),
	})
}

func (p *liveStatusProvider) CheckForUpdate(ctx context.Context, force bool) update.CheckResponse {
	return p.checker.Check(ctx, force).Response(p.install())
}

// Opt-in fixture for AppTests/BridgeStatusLiveTests: test identities, an
// isolated home and a loopback listener; never the installed bridge. The
// file <root>/mode selects ready (a fake gateway forced ready), never-ready
// (no gateway: starting, then down after 5 s) or legacy (no capability and
// no bridge routes). The app test creates <root>/stop to end it.
func TestBridgeStatusLiveFixture(t *testing.T) {
	root := os.Getenv("HERMOTE_BRIDGE_STATUS_LIVE_ROOT")
	if root == "" {
		t.Skip("isolated live fixture not requested")
	}
	if root != liveStatusRoot {
		t.Fatal("unexpected fixture root")
	}
	rawMode, err := os.ReadFile(filepath.Join(root, "mode"))
	if err != nil {
		t.Fatal(err)
	}
	mode := strings.TrimSpace(string(rawMode))
	if mode != "ready" && mode != "never-ready" && mode != "legacy" {
		t.Fatalf("unknown fixture mode %q", mode)
	}
	home := filepath.Join(root, "home")
	defer os.RemoveAll(home)
	st, err := state.OpenAt(filepath.Join(home, "remote"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bridgeID, err := protocol.IdentityFromSeed(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	phone, err := protocol.IdentityFromSeed(bytes.Repeat([]byte{0x41}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddTrusted(phone.Public(), "Bridge status fixture phone"); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sup, err := gateway.New(gateway.Options{Python: "/usr/bin/false", HermesHome: home, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(bridgeID, st, sup, logger)
	switch mode {
	case "ready":
		gw := fakeGateway(t, sup.Token())
		defer gw.Close()
		gateway.ForceReadyForTest(sup, gw.URL[strings.LastIndex(gw.URL, ":")+1:])
	case "never-ready":
		// Starting until 5 s after the phone connects, then down: the
		// gateway-dial loop reports each in `ctl gateway`, after accepted
		// and before its 60 s "gateway unavailable" close.
		gateway.ForceStateForTest(sup, gateway.StateStarting)
		go func() {
			for srv.ConnectionCount() == 0 {
				time.Sleep(50 * time.Millisecond)
			}
			time.Sleep(5 * time.Second)
			gateway.ForceStateForTest(sup, gateway.StateDown)
		}()
	case "legacy":
		gw := fakeGateway(t, sup.Token())
		defer gw.Close()
		gateway.ForceReadyForTest(sup, gw.URL[strings.LastIndex(gw.URL, ":")+1:])
	}
	if mode != "legacy" {
		srv.Status = &liveStatusProvider{
			srv: srv, started: time.Now().UTC(), checker: update.NewChecker(liveReleaseSource{}, "0.15.0"),
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:65443")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Close()

	ready, _ := json.Marshal(map[string]any{
		"mode": mode, "session_id": bridgeID.SessionID(), "url": "ws://127.0.0.1:65443/v1/bridge",
		"bridge_public_key": base64.RawURLEncoding.EncodeToString(bridgeID.Public()),
	})
	if err := os.WriteFile(filepath.Join(root, "fixture-ready.json"), ready, 0o600); err != nil {
		t.Fatal(err)
	}
	stop := filepath.Join(root, "stop")
	for {
		if _, err := os.Stat(stop); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	srv.DisconnectDevice(protocol.DeviceID(phone.Public()))
	deadline := time.Now().Add(10 * time.Second)
	for srv.ConnectionCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
}
