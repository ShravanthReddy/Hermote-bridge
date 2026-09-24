package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/bridge"
	"github.com/ShravanthReddy/Hermote-bridge/internal/control"
	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/push"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
	"github.com/ShravanthReddy/Hermote-bridge/internal/tailscale"
)

// daemon is the long-running process launchd keeps alive: gateway supervisor,
// bridge listener on loopback, control socket for the CLI.
type daemon struct {
	store     *state.Store
	id        *protocol.Identity
	cfg       state.Config
	sup       *gateway.Supervisor
	srv       *bridge.Server
	relay     *bridge.RelayDialer // relay transport only
	pushes    *push.Registry      // nil until startPush
	log       *slog.Logger
	startedAt time.Time
	launchID  string

	mu        sync.Mutex
	publicURL string
	publicAt  time.Time
}

func runDaemon(args []string) error {
	flags := flag.NewFlagSet("daemon", flag.ContinueOnError)
	launchID := flags.String("launch-id", "", "launch generation supplied by the service manager")
	if err := flags.Parse(args); err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)

	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, err := store.Config()
	if err != nil {
		return err
	}
	if cfg.Python == "" || cfg.BridgePort == 0 {
		return errors.New("not configured — run `hermote-bridge up` on macOS or create config.json for manual foreground use")
	}
	id, err := store.Identity()
	if err != nil {
		return err
	}
	hermesHome, err := state.HermesHome()
	if err != nil {
		return err
	}
	sup, err := gateway.New(gateway.Options{Python: cfg.Python, HermesHome: hermesHome, Logger: log})
	if err != nil {
		return err
	}
	d := &daemon{store: store, id: id, cfg: cfg, sup: sup, log: log, startedAt: time.Now().UTC(), launchID: *launchID}
	d.srv = bridge.New(id, store, sup, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = sup.Run(ctx) }()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.BridgePort))
	httpSrv := &http.Server{Addr: addr, Handler: d.srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("bridge listening", "addr", addr, "session", id.SessionID(), "transport", cfg.Transport)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("bridge listener failed", "err", err)
			stop()
		}
	}()

	if cfg.Transport == protocol.TransportRelay {
		d.relay = &bridge.RelayDialer{Server: d.srv, RelayURL: cfg.RelayURL, Logger: log}
		wg.Add(1)
		go func() { defer wg.Done(); d.relay.Run(ctx) }()
	}

	// Push: registrations are always recorded (a phone may register before
	// the APNs key is set up); the watcher only runs once a key exists.
	if err := d.startPush(ctx, &wg); err != nil {
		log.Warn("push disabled", "err", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := control.Serve(ctx, store.Path("control.sock"), d); err != nil {
			log.Error("control socket failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
	sup.Stop()
	wg.Wait()
	return nil
}

// startPush records phone registrations and, when an APNs key is configured,
// polls the gateway for moments that need a person and pushes them to phones
// that are not connected (ADR-023).
func (d *daemon) startPush(ctx context.Context, wg *sync.WaitGroup) error {
	registry, err := push.OpenRegistry(d.store.Path(""))
	if err != nil {
		return err
	}
	d.pushes = registry
	d.srv.OnPush = func(deviceID string, reg protocol.PushRegistration) {
		if err := registry.Set(deviceID, reg, time.Now().UTC()); err != nil {
			d.log.Warn("push registration not saved", "err", err)
			return
		}
		d.log.Info("push registration", "device", deviceID[:min(8, len(deviceID))], "kinds", len(reg.Kinds))
	}
	cfg, err := push.LoadConfig(d.store.Path(""))
	if err != nil {
		return err
	}
	client, err := push.NewClient(cfg)
	if err != nil {
		return err
	}
	watcher := &push.Watcher{
		Gateway:  &push.WSGateway{BaseURL: d.sup.BaseURL, Token: d.sup.Token},
		Sender:   client,
		Registry: registry,
		Online:   d.srv.OnlineDevices,
		Trusted:  d.pairedDeviceIDs,
		Logger:   d.log,
	}
	wg.Add(1)
	go func() { defer wg.Done(); watcher.Run(ctx) }()
	d.log.Info("push watcher running", "bundle", cfg.BundleID, "key", cfg.KeyID)
	return nil
}

// pairedDeviceIDs reads the trusted-device list without touching last_seen.
func (d *daemon) pairedDeviceIDs() (map[string]bool, error) {
	devices, err := d.store.Devices()
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(devices))
	for _, dev := range devices {
		ids[dev.ID] = true
	}
	return ids, nil
}

func logLevel() slog.Level {
	if os.Getenv("HERMOTE_BRIDGE_DEBUG") != "" || os.Getenv("HERMES_REMOTE_DEBUG") != "" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// PublicURL is where phones connect: the Tailscale HTTPS front (direct) or
// the relay's phone endpoint. Direct mode is re-read every minute so a
// renamed machine keeps pairing correctly.
func (d *daemon) PublicURL(ctx context.Context) (string, error) {
	if d.cfg.Transport == protocol.TransportRelay {
		return d.cfg.RelayURL + "/v1/phone", nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.publicURL != "" && time.Since(d.publicAt) < time.Minute {
		return d.publicURL, nil
	}
	cli, err := tailscale.Find()
	if err != nil {
		return "", err
	}
	st, err := cli.Status(ctx)
	if err != nil {
		return "", err
	}
	if st.BackendState != "Running" || st.DNSName == "" {
		return "", fmt.Errorf("Tailscale is %s", st.BackendState)
	}
	d.publicURL = fmt.Sprintf("wss://%s:%d/v1/bridge", st.DNSName, d.cfg.HTTPSPort)
	d.publicAt = time.Now()
	return d.publicURL, nil
}

// Status implements control.Backend.
func (d *daemon) Status(ctx context.Context) control.Status {
	port, _ := d.sup.Port()
	devices, _ := d.store.Devices()
	s := control.Status{
		LaunchID:    d.launchID,
		SessionID:   d.id.SessionID(),
		Transport:   string(d.cfg.Transport),
		Name:        d.cfg.Name,
		Gateway:     string(d.sup.State()),
		GatewayPort: port,
		BridgeAddr:  net.JoinHostPort("127.0.0.1", strconv.Itoa(d.cfg.BridgePort)),
		Connections: d.srv.ConnectionCount(),
		Pending:     d.srv.Pairings.Pending(),
		Devices:     devices,
		StartedAt:   d.startedAt,
		Version:     version,
	}
	if u, err := d.PublicURL(ctx); err == nil {
		s.PublicURL = u
	} else {
		s.Warnings = append(s.Warnings, err.Error())
	}
	if d.relay != nil {
		attached, lastErr := d.relay.Attached()
		s.Extra = map[string]string{"relay": d.cfg.RelayURL, "relay_attached": fmt.Sprint(attached)}
		if !attached {
			msg := "not attached to the relay"
			if lastErr != "" {
				msg += ": " + lastErr
			}
			s.Warnings = append(s.Warnings, msg)
		}
	}
	return s
}

// Pair implements control.Backend.
func (d *daemon) Pair(ctx context.Context) (control.PairResult, error) {
	u, err := d.PublicURL(ctx)
	if err != nil {
		return control.PairResult{}, err
	}
	code, exp, err := d.srv.Pairings.Issue(protocol.PairTTL)
	if err != nil {
		return control.PairResult{}, err
	}
	p := protocol.PairPayload{
		Version: protocol.Version, Transport: d.cfg.Transport, URL: u,
		SessionID: d.id.SessionID(), BridgeKey: d.id.Public(), Code: code, Expires: exp, Name: d.cfg.Name,
	}
	d.log.Info("pairing code issued", "expires", exp)
	return control.PairResult{URL: p.String(), Expires: exp}, nil
}

// Revoke implements control.Backend.
func (d *daemon) Revoke(_ context.Context, idOrPrefix string) (state.Device, error) {
	dev, err := d.srv.Revoke(idOrPrefix)
	if err != nil {
		return state.Device{}, err
	}
	d.log.Info("device revoked", "device", dev.ID)
	if d.pushes != nil {
		if err := d.pushes.Remove(dev.ID); err != nil {
			d.log.Warn("revoked device's push registration not removed", "device", dev.ID, "err", err)
		}
	}
	return dev, nil
}
