package main

import (
	"context"
	"errors"
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

	"github.com/ShravanthReddy/hermes-remote/internal/bridge"
	"github.com/ShravanthReddy/hermes-remote/internal/control"
	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
	"github.com/ShravanthReddy/hermes-remote/internal/tailscale"
)

// daemon is the long-running process launchd keeps alive: gateway supervisor,
// bridge listener on loopback, control socket for the CLI.
type daemon struct {
	store     *state.Store
	id        *protocol.Identity
	cfg       state.Config
	sup       *gateway.Supervisor
	srv       *bridge.Server
	log       *slog.Logger
	startedAt time.Time

	mu        sync.Mutex
	publicURL string
	publicAt  time.Time
}

func runDaemon(args []string) error {
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
		return errors.New("not configured — run `hermes-remote up` first")
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
	d := &daemon{store: store, id: id, cfg: cfg, sup: sup, log: log, startedAt: time.Now().UTC()}
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

func logLevel() slog.Level {
	if os.Getenv("HERMES_REMOTE_DEBUG") != "" {
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
	dev, err := d.store.Revoke(idOrPrefix)
	if err != nil {
		return state.Device{}, err
	}
	d.srv.DisconnectDevice(dev.ID)
	d.log.Info("device revoked", "device", dev.ID)
	return dev, nil
}
