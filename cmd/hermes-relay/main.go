// hermes-relay: the stateless meeting point for hermes-remote bridges and
// phones that cannot reach each other directly. Run behind a TLS terminator
// (Caddy) — see relay/README in the repo for the compose file.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/relay"
)

var version = "dev"

func main() {
	addr := flag.String("listen", envOr("RELAY_LISTEN", ":8080"), "listen address")
	maxPhones := flag.Int("max-phones", 0, "phones per bridge session (default 4)")
	perIP := flag.Int("per-ip-per-minute", 0, "new connections per IP per minute (default 60)")
	showVersion := flag.Bool("version", false, "print version")
	flag.Parse()
	if *showVersion {
		fmt.Println("hermes-relay", version)
		return
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := relay.New(relay.Limits{MaxPhonesPerSession: *maxPhones, ConnectionsPerIPPerMinute: *perIP}, log)
	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Info("hermes-relay listening", "addr", *addr, "version", version)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen failed", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	srv.Drain() // bridges reconnect with backoff; phones reconnect on their own
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
