package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

// Opt-in fixture only: a fresh supervisor/home and public deterministic test
// identities, never a production installation or a caller-supplied gateway token.
func TestAttachmentBlobLiveFixture(t *testing.T) {
	run := os.Getenv("HERMES_ATTACHMENT_LIVE_ROOT")
	if run == "" {
		t.Skip("isolated live fixture not requested")
	}
	if run != "/private/tmp/hermote-attachment-phase3-live" {
		t.Fatal("unexpected fixture root")
	}
	python := os.Getenv("HERMES_ATTACHMENT_LIVE_PYTHON")
	if python == "" {
		t.Fatal("fixture interpreter required")
	}
	home := filepath.Join(run, "hermes-home")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := state.OpenAt(filepath.Join(home, "remote"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bridgeID, err := protocol.IdentityFromSeed(bytes.Repeat([]byte{0x32}, 32))
	if err != nil {
		t.Fatal(err)
	}
	phone, err := protocol.IdentityFromSeed(bytes.Repeat([]byte{0x31}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddTrusted(phone.Public(), "Attachment fixture phone"); err != nil {
		t.Fatal(err)
	}
	sup, err := gateway.New(gateway.Options{Python: python, HermesHome: home, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sup.Run(ctx) }()
	defer sup.Stop()
	srv := New(bridgeID, st, sup, logger)
	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	mux.HandleFunc("GET /fixture/state", func(w http.ResponseWriter, _ *http.Request) {
		writeLiveFixtureJSON(w, liveBlobState(srv))
	})
	mux.HandleFunc("POST /fixture/verify", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Path string `json:"path"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 8192)).Decode(&request) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		result, err := liveHashAttachment(home, request.Path)
		if err != nil {
			http.Error(w, "fixture attachment verification failed", 400)
			return
		}
		writeLiveFixtureJSON(w, result)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:65442")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = httpServer.Serve(listener) }()
	defer httpServer.Close()
	relayURL := os.Getenv("HERMES_ATTACHMENT_LIVE_RELAY")
	dialer := &RelayDialer{Server: srv, RelayURL: relayURL, Logger: logger}
	if relayURL != "" {
		go dialer.Run(ctx)
	}
	deadline := time.Now().Add(100 * time.Second)
	for time.Now().Before(deadline) {
		attached := relayURL == ""
		if !attached {
			attached, _ = dialer.Attached()
		}
		if sup.State() == gateway.StateReady && attached {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if sup.State() != gateway.StateReady {
		t.Fatal("fixture gateway did not become ready")
	}
	if relayURL != "" {
		if ok, _ := dialer.Attached(); !ok {
			t.Fatal("fixture relay did not attach")
		}
	}
	ready := map[string]any{"ready": true, "pid": os.Getpid(), "session_id": bridgeID.SessionID(), "bridge_public_key": base64.RawURLEncoding.EncodeToString(bridgeID.Public())}
	data, _ := json.Marshal(ready)
	if err := os.WriteFile(filepath.Join(run, "fixture-ready.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	metrics, err := os.OpenFile(filepath.Join(run, "evidence", "bridge-metrics.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Close()
	encoder := json.NewEncoder(metrics)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		_ = encoder.Encode(liveBlobState(srv))
		if _, err := os.Stat(filepath.Join(run, "stop")); err == nil {
			break
		}
	}
	srv.DisconnectDevice(protocol.DeviceID(phone.Public()))
	cancel()
	cleanupDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(cleanupDeadline) {
		snapshot := liveBlobState(srv)
		if snapshot["active_uploads"] == 0 && snapshot["connections"] == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	final := liveBlobState(srv)
	if final["active_uploads"] != 0 || final["reserved_bytes"] != int64(0) || final["spool_files"] != 0 {
		t.Error("fixture upload accounting did not drain")
	}
	data, _ = json.Marshal(final)
	_ = os.WriteFile(filepath.Join(run, "evidence", "bridge-final.json"), data, 0600)
}

func writeLiveFixtureJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func liveBlobState(srv *Server) map[string]any {
	usage := srv.blobUsageSnapshot()
	files := 0
	_ = filepath.WalkDir(srv.blobs.root, func(_ string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			files++
		}
		return nil
	})
	return map[string]any{"unix_ms": time.Now().UnixMilli(), "active_uploads": usage.ActiveUploads, "reserved_bytes": usage.ReservedBytes, "spool_files": files, "connections": srv.ConnectionCount(),
		"peak_active_uploads": usage.PeakActiveUploads, "peak_reserved_bytes": usage.PeakReservedBytes,
		"auxiliary_dispatches": usage.AuxiliaryDispatches, "last_aux_write_calls": usage.LastAuxWriteCalls,
		"last_aux_bytes": usage.LastAuxBytes, "last_aux_max_write": usage.LastAuxMaxWrite, "last_commit_ns": usage.LastCommitNanos,
		"cleanup_failures": usage.CleanupFailures, "spool_unavailable": usage.Unavailable}
}

func liveHashAttachment(home, path string) (map[string]any, error) {
	root := filepath.Join(home, "attachments")
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") || filepath.IsAbs(relative) {
		return nil, os.ErrPermission
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != filepath.Clean(path) {
		return nil, os.ErrPermission
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, os.ErrPermission
	}
	hash := sha256.New()
	count, err := io.CopyBuffer(hash, io.LimitReader(file, 268435457), make([]byte, 524288))
	if err != nil {
		return nil, err
	}
	return map[string]any{"size": count, "sha256": hex.EncodeToString(hash.Sum(nil)), "outside_spool": true}, nil
}
