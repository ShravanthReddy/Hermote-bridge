package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

type blobGatewayRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params struct {
		SessionID string `json:"session_id"`
		DataURL   string `json:"data_url"`
		Name      string `json:"name"`
		Path      string `json:"path"`
	} `json:"params"`
}

type blobGatewayStub struct {
	destination string
	readyBytes  int
	mode        string
	hold        <-chan struct{}
	started     chan struct{}
	requests    chan blobGatewayRequest
	errors      chan error
	calls       atomic.Int32
}

func newBlobGatewayStub(t *testing.T, destination string) *blobGatewayStub {
	t.Helper()
	return &blobGatewayStub{
		destination: destination, started: make(chan struct{}, 8),
		requests: make(chan blobGatewayRequest, 8), errors: make(chan error, 8),
	}
}

func (s *blobGatewayStub) handler(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") != token {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(8 << 20)
		ctx := r.Context()
		ready, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "method": "event",
			"params": map[string]any{"type": "gateway.ready", "payload": map[string]any{"skin": strings.Repeat("x", s.readyBytes)}},
		})
		if err := ws.Write(ctx, websocket.MessageText, ready); err != nil {
			return
		}
		_, raw, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var request blobGatewayRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			s.errors <- err
			return
		}
		if request.Method != "file.attach" {
			s.errors <- fmt.Errorf("unexpected method %q", request.Method)
			return
		}
		s.calls.Add(1)
		s.requests <- request
		s.started <- struct{}{}
		if s.hold != nil {
			select {
			case <-s.hold:
			case <-time.After(5 * time.Second):
				return
			}
		}
		if s.mode == "status1009" {
			_ = ws.Close(websocket.StatusMessageTooBig, "attachment too large")
			return
		}
		if s.mode == "rpc" {
			reply, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": request.ID,
				"error": map[string]any{"code": 4001, "message": "session not found"},
			})
			_ = ws.Write(ctx, websocket.MessageText, reply)
			return
		}
		payload, decodeErr := decodeBlobDataURL(request.Params.DataURL)
		if decodeErr != nil || request.Params.Path != "" {
			s.errors <- fmt.Errorf("invalid streamed request: decode=%v path=%q", decodeErr, request.Params.Path)
			return
		}
		name := sanitizeBlobGatewayName(request.Params.Name)
		if name == "" {
			name = "attachment"
		}
		path := filepath.Join(s.destination, name)
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			s.errors <- err
			return
		}
		refPath := "attachments/" + name
		refText := formatBlobTestRef(refPath)
		result := map[string]any{
			"attached": true, "uploaded": true, "name": name, "path": path,
			"ref_path": refPath, "ref_text": refText,
		}
		if s.mode == "malformed" {
			result["ref_text"] = "@file:attachments/other"
		}
		unrelated, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "event", "params": map[string]any{"type": "other"}})
		_ = ws.Write(ctx, websocket.MessageText, unrelated)
		wrong, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "not-" + request.ID, "result": result})
		_ = ws.Write(ctx, websocket.MessageText, wrong)
		reply, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		_ = ws.Write(ctx, websocket.MessageText, reply)
	})
	return mux
}

func decodeBlobDataURL(value string) ([]byte, error) {
	const prefix = "data:application/octet-stream;base64,"
	if !strings.HasPrefix(value, prefix) {
		return nil, errorsNew("missing data URL prefix")
	}
	return base64.StdEncoding.DecodeString(strings.TrimPrefix(value, prefix))
}

func errorsNew(message string) error { return fmt.Errorf("%s", message) }

func sanitizeBlobGatewayName(value string) string {
	value = strings.Trim(strings.TrimSpace(value), ".")
	var sanitized strings.Builder
	inASCIIControl := false
	for _, r := range value {
		if r <= 0x1f || r == 0x7f {
			if !inASCIIControl {
				sanitized.WriteByte('_')
			}
			inASCIIControl = true
			continue
		}
		inASCIIControl = false
		sanitized.WriteRune(r)
	}
	return sanitized.String()
}

func formatBlobTestRef(value string) string {
	if !strings.ContainsAny(value, " \t\n()[]{}<>\"'`") {
		return "@file:" + value
	}
	for _, quote := range []string{"`", "\"", "'"} {
		if !strings.Contains(value, quote) {
			return "@file:" + quote + value + quote
		}
	}
	return "@file:" + value
}

type blobTestHarness struct {
	srv   *Server
	http  *httptest.Server
	stub  *blobGatewayStub
	store *state.Store
}

func newBlobTestHarness(t *testing.T, configure func(*bridgeDependencies, *blobGatewayStub)) *blobTestHarness {
	t.Helper()
	root := t.TempDir()
	store, err := state.OpenAt(filepath.Join(root, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	identity, err := store.Identity()
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: root})
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "attachments")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	stub := newBlobGatewayStub(t, destination)
	deps := productionBridgeDependencies()
	deps.blobSpoolRoot = filepath.Join(root, "state", "attachment-blob-spool-v1")
	if configure != nil {
		configure(&deps, stub)
	}
	gatewayServer := httptest.NewServer(stub.handler(supervisor.Token()))
	t.Cleanup(gatewayServer.Close)
	host, portText, err := net.SplitHostPort(gatewayServer.Listener.Addr().String())
	if err != nil || host == "" {
		t.Fatalf("gateway address: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatal(err)
	}
	gateway.ForceReadyForTest(supervisor, fmt.Sprint(port))
	srv := newServer(identity, store, supervisor, slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
	httpServer := httptest.NewServer(srv.Handler())
	t.Cleanup(httpServer.Close)
	return &blobTestHarness{srv: srv, http: httpServer, stub: stub, store: store}
}

func (h *blobTestHarness) connect(t *testing.T, ctx context.Context) (*phoneClient, protocol.CtlMessage) {
	t.Helper()
	phone, err := protocol.NewIdentity(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.AddTrusted(phone.Public(), "blob test phone"); err != nil {
		t.Fatal(err)
	}
	client, err := connectPhone(t, ctx, h.http, h.srv.Identity, phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.ws.Close(websocket.StatusNormalClosure, "") })
	ch, raw, err := client.recv(ctx)
	if err != nil || ch != protocol.ChCtl {
		t.Fatalf("accepted message: ch=%q err=%v", ch, err)
	}
	var accepted protocol.CtlMessage
	if err := json.Unmarshal(raw, &accepted); err != nil || accepted.Op != protocol.CtlAccepted {
		t.Fatalf("accepted message: %+v err=%v", accepted, err)
	}
	return client, accepted
}

func sendBlobBegin(t *testing.T, ctx context.Context, phone *phoneClient, id, sessionID, name string, size int64, digest string) {
	t.Helper()
	if err := phone.send(ctx, map[string]any{
		"ch": "blob", "op": "begin", "id": id, "session_id": sessionID,
		"name": name, "size": size, "sha256": digest,
	}); err != nil {
		t.Fatal(err)
	}
}

func waitBlob(t *testing.T, ctx context.Context, phone *phoneClient, id string) protocol.BlobMessage {
	t.Helper()
	raw, err := phone.waitFor(ctx, protocol.ChBlob, func(raw []byte) bool {
		var message protocol.BlobMessage
		return json.Unmarshal(raw, &message) == nil && message.ID == id
	})
	if err != nil {
		t.Fatal(err)
	}
	var message protocol.BlobMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		t.Fatal(err)
	}
	return message
}

func waitBlobUsage(t *testing.T, ctx context.Context, srv *Server, active int, reserved int64) blobUsageSnapshot {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := srv.blobUsageSnapshot()
		if snapshot.ActiveUploads == active && snapshot.ReservedBytes == reserved {
			return snapshot
		}
		select {
		case <-ctx.Done():
			t.Fatalf("blob usage=%+v, want active=%d reserved=%d", snapshot, active, reserved)
		case <-ticker.C:
		}
	}
}

func waitBlobUnavailable(
	t *testing.T, ctx context.Context, srv *Server, active int, reserved int64, cleanupFailures uint64,
) blobUsageSnapshot {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := srv.blobUsageSnapshot()
		if snapshot.ActiveUploads == active && snapshot.ReservedBytes == reserved &&
			snapshot.CleanupFailures == cleanupFailures && snapshot.Unavailable == 1 {
			return snapshot
		}
		select {
		case <-ctx.Done():
			t.Fatalf("blob unavailable state=%+v", snapshot)
		case <-ticker.C:
		}
	}
}

func waitBlobConnectionsIdle(t *testing.T, ctx context.Context, srv *Server) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		srv.mu.Lock()
		connections := make([]*conn, 0, len(srv.conns))
		for connection := range srv.conns {
			connections = append(connections, connection)
		}
		srv.mu.Unlock()
		idle := true
		for _, connection := range connections {
			if connection.blobs == nil {
				continue
			}
			connection.blobs.mu.Lock()
			idle = idle && connection.blobs.active == nil
			connection.blobs.mu.Unlock()
		}
		if idle {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("blob connection did not become idle")
		case <-ticker.C:
		}
	}
}

func TestBlobCapabilityAndMetadataBoundaries(t *testing.T) {
	h := newBlobTestHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, accepted := h.connect(t, ctx)
	capability := accepted.Caps.AttachmentBlob
	if capability == nil || capability.Version != 1 || capability.MaxFileBytes != defaultBlobMaxFileBytes || capability.ChunkBytes != defaultBlobChunkBytes {
		t.Fatalf("capability=%+v", capability)
	}

	maxID := "00000000-0000-4000-8000-000000000001"
	maxName := strings.Repeat("a", 251) + ".pdf"
	sendBlobBegin(t, ctx, phone, maxID, strings.Repeat("s", 1024), maxName, defaultBlobMaxFileBytes, strings.Repeat("0", 64))
	ready := waitBlob(t, ctx, phone, maxID)
	if ready.Op != "ready" || ready.Offset == nil || *ready.Offset != 0 || ready.ChunkBytes == nil || *ready.ChunkBytes != defaultBlobChunkBytes {
		t.Fatalf("ready=%+v", ready)
	}
	waitBlobUsage(t, ctx, h.srv, 1, defaultBlobMaxFileBytes)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "abort", "id": maxID})
	waitBlobUsage(t, ctx, h.srv, 0, 0)

	tests := []struct {
		id, session, name, digest string
		size                      int64
		kind                      string
	}{
		{"00000000-0000-4000-8000-000000000002", "s", "x", strings.Repeat("0", 64), defaultBlobMaxFileBytes + 1, "file_size_invalid"},
		{"00000000-0000-4000-8000-000000000003", strings.Repeat("s", 1025), "x", strings.Repeat("0", 64), 0, "invalid_request"},
		{"00000000-0000-4000-8000-000000000004", "s", strings.Repeat("a", 256), strings.Repeat("0", 64), 0, "invalid_request"},
		{"00000000-0000-4000-8000-000000000005", "s", "../x", strings.Repeat("0", 64), 0, "invalid_request"},
		{"00000000-0000-4000-8000-000000000006", "s", "x", strings.Repeat("A", 64), 0, "invalid_request"},
	}
	for _, test := range tests {
		sendBlobBegin(t, ctx, phone, test.id, test.session, test.name, test.size, test.digest)
		result := waitBlob(t, ctx, phone, test.id)
		if result.Error == nil || result.Error.Kind != test.kind {
			t.Fatalf("id=%s result=%+v", test.id, result)
		}
	}
}

func TestBlobStreamingSuccessDuplicateAndMetrics(t *testing.T) {
	h := newBlobTestHarness(t, func(_ *bridgeDependencies, stub *blobGatewayStub) { stub.readyBytes = 40 << 10 })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 80_000)
	digest := sha256.Sum256(payload)
	id := "10000000-0000-4000-8000-000000000001"
	name := "  résumé [final] $HOME.pdf  "
	sendBlobBegin(t, ctx, phone, id, "runtime-live", name, int64(len(payload)), hex.EncodeToString(digest[:]))
	ready := waitBlob(t, ctx, phone, id)
	if ready.Op != "ready" {
		t.Fatalf("ready=%+v", ready)
	}
	offset := int64(0)
	for offset < int64(len(payload)) {
		next := min(offset+int64(defaultBlobChunkBytes), int64(len(payload)))
		chunk := payload[offset:next]
		if err := phone.send(ctx, map[string]any{
			"ch": "blob", "op": "data", "id": id, "offset": offset,
			"d": base64.RawURLEncoding.EncodeToString(chunk),
		}); err != nil {
			t.Fatal(err)
		}
		ack := waitBlob(t, ctx, phone, id)
		if ack.Op != "ack" || ack.Offset == nil || *ack.Offset != next {
			t.Fatalf("ack=%+v want=%d", ack, next)
		}
		offset = next
	}
	if err := phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id}); err != nil {
		t.Fatal(err)
	}
	result := waitBlob(t, ctx, phone, id)
	if result.Op != "result" || result.Error != nil {
		t.Fatalf("result=%+v", result)
	}
	var receipt struct {
		Name     string `json:"name"`
		Path     string `json:"path"`
		RefPath  string `json:"ref_path"`
		RefText  string `json:"ref_text"`
		Attached bool   `json:"attached"`
		Uploaded bool   `json:"uploaded"`
	}
	if err := json.Unmarshal(result.Result, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Name != "résumé [final] $HOME.pdf" || !receipt.Attached || !receipt.Uploaded ||
		receipt.RefPath != "attachments/"+receipt.Name || receipt.RefText != formatBlobTestRef(receipt.RefPath) {
		t.Fatalf("receipt=%+v", receipt)
	}
	request := <-h.stub.requests
	if request.Params.Name != name || request.Params.SessionID != "runtime-live" || request.Params.Path != "" {
		t.Fatalf("request=%+v", request)
	}
	if err := phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id}); err != nil {
		t.Fatal(err)
	}
	replayed := waitBlob(t, ctx, phone, id)
	if !bytes.Equal(replayed.Result, result.Result) || h.stub.calls.Load() != 1 {
		t.Fatalf("duplicate result changed or redispatched: calls=%d", h.stub.calls.Load())
	}
	snapshot := waitBlobUsage(t, ctx, h.srv, 0, 0)
	if snapshot.AuxiliaryDispatches != 1 || snapshot.LastAuxWriteCalls < 2 ||
		snapshot.LastAuxMaxWrite > defaultBlobChunkBytes || snapshot.LastAuxBytes <= int64(len(payload)) ||
		snapshot.LastCommitNanos <= 0 {
		t.Fatalf("metrics=%+v", snapshot)
	}
	select {
	case err := <-h.stub.errors:
		t.Fatal(err)
	default:
	}
}

func TestBlobZeroByteSuccess(t *testing.T) {
	h := newBlobTestHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)
	id := "10000000-0000-4000-8000-000000000002"
	digest := sha256.Sum256(nil)
	sendBlobBegin(t, ctx, phone, id, "runtime-empty", "empty.txt", 0, hex.EncodeToString(digest[:]))
	if ready := waitBlob(t, ctx, phone, id); ready.Op != "ready" {
		t.Fatalf("ready=%+v", ready)
	}
	if err := phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id}); err != nil {
		t.Fatal(err)
	}
	result := waitBlob(t, ctx, phone, id)
	if result.Error != nil || len(result.Result) == 0 || h.stub.calls.Load() != 1 {
		t.Fatalf("zero-byte result=%+v calls=%d", result, h.stub.calls.Load())
	}
	request := <-h.stub.requests
	if request.Params.DataURL != "data:application/octet-stream;base64," || request.Params.Name != "empty.txt" {
		t.Fatalf("zero-byte request=%+v", request)
	}
	var receipt struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(result.Result, &receipt); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(receipt.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("zero-byte destination size=%d", info.Size())
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)
}

func TestBlobOriginalControlNameUsesAuthoritativeSanitizedReceipt(t *testing.T) {
	h := newBlobTestHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)
	id := "10000000-0000-4000-8000-000000000003"
	originalName := "draft\n\tfinal.txt"
	emptyDigest := sha256.Sum256(nil)
	sendBlobBegin(t, ctx, phone, id, "runtime", originalName, 0, hex.EncodeToString(emptyDigest[:]))
	if ready := waitBlob(t, ctx, phone, id); ready.Op != "ready" {
		t.Fatalf("ready=%+v", ready)
	}
	if err := phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id}); err != nil {
		t.Fatal(err)
	}
	result := waitBlob(t, ctx, phone, id)
	if result.Error != nil {
		t.Fatalf("result=%+v", result)
	}
	request := <-h.stub.requests
	if request.Params.Name != originalName {
		t.Fatalf("gateway request name=%q want original %q", request.Params.Name, originalName)
	}
	var receipt struct {
		Name    string `json:"name"`
		RefPath string `json:"ref_path"`
		RefText string `json:"ref_text"`
	}
	if err := json.Unmarshal(result.Result, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Name != "draft_final.txt" || receipt.RefPath != "attachments/draft_final.txt" ||
		receipt.RefText != "@file:attachments/draft_final.txt" {
		t.Fatalf("authoritative sanitized receipt=%+v", receipt)
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)
}

func TestBlobStateErrorsReleaseReservations(t *testing.T) {
	h := newBlobTestHarness(t, func(deps *bridgeDependencies, _ *blobGatewayStub) { deps.blobChunkBytes = 4 })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)

	id := "20000000-0000-4000-8000-000000000001"
	digest := sha256.Sum256([]byte("abc"))
	sendBlobBegin(t, ctx, phone, id, "runtime", "a.txt", 3, hex.EncodeToString(digest[:]))
	_ = waitBlob(t, ctx, phone, id)
	wrong := "20000000-0000-4000-8000-000000000002"
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "data", "id": wrong, "offset": 0, "d": "YQ"})
	if result := waitBlob(t, ctx, phone, wrong); result.Error == nil || result.Error.Kind != "unknown_upload" {
		t.Fatalf("wrong-id result=%+v", result)
	}
	waitBlobUsage(t, ctx, h.srv, 1, 3)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "data", "id": id, "offset": 1, "d": "YQ"})
	if result := waitBlob(t, ctx, phone, id); result.Error == nil || result.Error.Kind != "offset_mismatch" {
		t.Fatalf("offset result=%+v", result)
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)

	oversize := "20000000-0000-4000-8000-000000000003"
	sendBlobBegin(t, ctx, phone, oversize, "runtime", "b.txt", 5, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, phone, oversize)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "data", "id": oversize, "offset": 0, "d": base64.RawURLEncoding.EncodeToString([]byte("12345"))})
	if result := waitBlob(t, ctx, phone, oversize); result.Error == nil || result.Error.Kind != "chunk_too_large" {
		t.Fatalf("oversize result=%+v", result)
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)

	short := "20000000-0000-4000-8000-000000000004"
	sendBlobBegin(t, ctx, phone, short, "runtime", "c.txt", 2, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, phone, short)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": short})
	if result := waitBlob(t, ctx, phone, short); result.Error == nil || result.Error.Kind != "size_mismatch" {
		t.Fatalf("short result=%+v", result)
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)

	hashID := "20000000-0000-4000-8000-000000000005"
	sendBlobBegin(t, ctx, phone, hashID, "runtime", "d.txt", 1, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, phone, hashID)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "data", "id": hashID, "offset": 0, "d": "eA"})
	_ = waitBlob(t, ctx, phone, hashID)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": hashID})
	if result := waitBlob(t, ctx, phone, hashID); result.Error == nil || result.Error.Kind != "hash_mismatch" {
		t.Fatalf("hash result=%+v", result)
	}
	if h.stub.calls.Load() != 0 {
		t.Fatalf("invalid upload reached gateway %d times", h.stub.calls.Load())
	}
}

func TestBlobQuotaSlotsAbortAndDisconnect(t *testing.T) {
	h := newBlobTestHarness(t, func(deps *bridgeDependencies, _ *blobGatewayStub) {
		deps.blobMaxFileBytes = 10
		deps.blobReservedBytes = 10
		deps.blobProcessWide = 2
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p1, _ := h.connect(t, ctx)
	p2, _ := h.connect(t, ctx)
	p3, _ := h.connect(t, ctx)

	id1 := "30000000-0000-4000-8000-000000000001"
	id2 := "30000000-0000-4000-8000-000000000002"
	id3 := "30000000-0000-4000-8000-000000000003"
	sendBlobBegin(t, ctx, p1, id1, "r", "one", 6, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, p1, id1)
	sendBlobBegin(t, ctx, p2, id2, "r", "two", 5, strings.Repeat("0", 64))
	if result := waitBlob(t, ctx, p2, id2); result.Error == nil || result.Error.Kind != "spool_quota_exceeded" {
		t.Fatalf("quota result=%+v", result)
	}
	sendBlobBegin(t, ctx, p2, id2, "r", "two", 4, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, p2, id2)
	snapshot := waitBlobUsage(t, ctx, h.srv, 2, 10)
	if snapshot.PeakActiveUploads != 2 || snapshot.PeakReservedBytes != 10 {
		t.Fatalf("peak usage=%+v", snapshot)
	}
	sendBlobBegin(t, ctx, p3, id3, "r", "three", 0, strings.Repeat("0", 64))
	if result := waitBlob(t, ctx, p3, id3); result.Error == nil || result.Error.Kind != "process_busy" {
		t.Fatalf("process slot result=%+v", result)
	}

	busyID := "30000000-0000-4000-8000-000000000004"
	sendBlobBegin(t, ctx, p1, busyID, "r", "busy", 0, strings.Repeat("0", 64))
	if result := waitBlob(t, ctx, p1, busyID); result.Error == nil || result.Error.Kind != "upload_busy" {
		t.Fatalf("connection slot result=%+v", result)
	}
	_ = p1.send(ctx, map[string]any{"ch": "blob", "op": "abort", "id": id1})
	waitBlobUsage(t, ctx, h.srv, 1, 4)

	sendBlobBegin(t, ctx, p3, id3, "r", "three", 6, strings.Repeat("0", 64))
	_ = waitBlob(t, ctx, p3, id3)
	waitBlobUsage(t, ctx, h.srv, 2, 10)
	_ = p2.ws.Close(websocket.StatusNormalClosure, "disconnect during upload")
	waitBlobUsage(t, ctx, h.srv, 1, 6)
	_ = p3.send(ctx, map[string]any{"ch": "blob", "op": "abort", "id": id3})
	waitBlobUsage(t, ctx, h.srv, 0, 0)
	entries, err := os.ReadDir(h.srv.blobs.root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spool entries=%d err=%v", len(entries), err)
	}
}

func TestBlobCommitHoldsReservationAndRejectsBegin(t *testing.T) {
	release := make(chan struct{})
	h := newBlobTestHarness(t, func(_ *bridgeDependencies, stub *blobGatewayStub) { stub.hold = release })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)
	payload := []byte("held")
	digest := sha256.Sum256(payload)
	id := "40000000-0000-4000-8000-000000000001"
	sendBlobBegin(t, ctx, phone, id, "runtime", "held.txt", int64(len(payload)), hex.EncodeToString(digest[:]))
	_ = waitBlob(t, ctx, phone, id)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "data", "id": id, "offset": 0, "d": base64.RawURLEncoding.EncodeToString(payload)})
	_ = waitBlob(t, ctx, phone, id)
	_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id})
	select {
	case <-h.stub.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitBlobUsage(t, ctx, h.srv, 1, int64(len(payload)))
	malformed := []map[string]any{
		{"ch": "blob", "op": "data", "id": id, "offset": 0, "d": "eA", "extra": true},
		{"ch": "blob", "op": "commit", "id": id, "extra": true},
		{"ch": "blob", "op": "data", "id": id, "offset": 0, "d": strings.Repeat("A", protocol.ChunkThreshold)},
	}
	for _, message := range malformed {
		if err := phone.send(ctx, message); err != nil {
			t.Fatal(err)
		}
		if result := waitBlob(t, ctx, phone, id); result.Error == nil || result.Error.Kind != "commit_in_progress" {
			t.Fatalf("malformed request during commit=%+v", result)
		}
		waitBlobUsage(t, ctx, h.srv, 1, int64(len(payload)))
		if h.stub.calls.Load() != 1 {
			t.Fatalf("malformed request changed auxiliary calls=%d", h.stub.calls.Load())
		}
	}
	other := "40000000-0000-4000-8000-000000000002"
	sendBlobBegin(t, ctx, phone, other, "runtime", "other.txt", 0, hex.EncodeToString(sha256.New().Sum(nil)))
	if result := waitBlob(t, ctx, phone, other); result.Error == nil || result.Error.Kind != "upload_busy" {
		t.Fatalf("begin during commit=%+v", result)
	}
	close(release)
	if result := waitBlob(t, ctx, phone, id); result.Error != nil || len(result.Result) == 0 {
		t.Fatalf("held commit result=%+v", result)
	}
	waitBlobUsage(t, ctx, h.srv, 0, 0)
}

func TestBlobTimeoutsAndGatewayErrors(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		h := newBlobTestHarness(t, func(deps *bridgeDependencies, _ *blobGatewayStub) {
			deps.blobIdleTimeout = 40 * time.Millisecond
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		phone, _ := h.connect(t, ctx)
		id := "50000000-0000-4000-8000-000000000001"
		sendBlobBegin(t, ctx, phone, id, "runtime", "idle", 1, strings.Repeat("0", 64))
		_ = waitBlob(t, ctx, phone, id)
		if result := waitBlob(t, ctx, phone, id); result.Error == nil || result.Error.Kind != "upload_idle_timeout" {
			t.Fatalf("idle result=%+v", result)
		}
		waitBlobUsage(t, ctx, h.srv, 0, 0)
	})

	tests := []struct {
		name, mode, kind string
		code             int
	}{
		{"backend code", "rpc", "gateway_rpc", 4001},
		{"message too big", "status1009", "gateway_update_required", blobCodeGatewayUpdate},
		{"malformed receipt", "malformed", "invalid_gateway_receipt", blobCodeGateway},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newBlobTestHarness(t, func(_ *bridgeDependencies, stub *blobGatewayStub) { stub.mode = test.mode })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			phone, _ := h.connect(t, ctx)
			id := fmt.Sprintf("50000000-0000-4000-8000-%012d", i+2)
			emptyDigest := sha256.Sum256(nil)
			sendBlobBegin(t, ctx, phone, id, "runtime", "empty.txt", 0, hex.EncodeToString(emptyDigest[:]))
			_ = waitBlob(t, ctx, phone, id)
			_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id})
			result := waitBlob(t, ctx, phone, id)
			if result.Error == nil || result.Error.Kind != test.kind || result.Error.Code != test.code {
				t.Fatalf("gateway result=%+v", result)
			}
			waitBlobUsage(t, ctx, h.srv, 0, 0)
		})
	}

	t.Run("commit deadline", func(t *testing.T) {
		release := make(chan struct{})
		h := newBlobTestHarness(t, func(deps *bridgeDependencies, stub *blobGatewayStub) {
			deps.blobCommitTimeout = 50 * time.Millisecond
			stub.hold = release
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		phone, _ := h.connect(t, ctx)
		id := "50000000-0000-4000-8000-000000000009"
		emptyDigest := sha256.Sum256(nil)
		sendBlobBegin(t, ctx, phone, id, "runtime", "empty.txt", 0, hex.EncodeToString(emptyDigest[:]))
		_ = waitBlob(t, ctx, phone, id)
		_ = phone.send(ctx, map[string]any{"ch": "blob", "op": "commit", "id": id})
		result := waitBlob(t, ctx, phone, id)
		close(release)
		if result.Error == nil || result.Error.Kind != "commit_timeout" || result.Error.Code != blobCodeTimeout {
			t.Fatalf("deadline result=%+v", result)
		}
		waitBlobUsage(t, ctx, h.srv, 0, 0)
	})
}

func TestBlobCleanupFailureFailsClosedAndPreservesAccounting(t *testing.T) {
	h := newBlobTestHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	phone, _ := h.connect(t, ctx)
	id := "59000000-0000-4000-8000-000000000001"
	sendBlobBegin(t, ctx, phone, id, "runtime", "blocked.txt", 4, strings.Repeat("0", 64))
	if ready := waitBlob(t, ctx, phone, id); ready.Op != "ready" {
		t.Fatalf("ready=%+v", ready)
	}
	entries, err := os.ReadDir(h.srv.blobs.root)
	if err != nil {
		t.Fatal(err)
	}
	var ownedDir string
	for _, entry := range entries {
		if entry.IsDir() && isCanonicalUUID(entry.Name()) {
			if ownedDir != "" {
				t.Fatal("multiple owned spool directories found")
			}
			ownedDir = filepath.Join(h.srv.blobs.root, entry.Name())
		}
	}
	if ownedDir == "" {
		t.Fatal("owned spool directory not found")
	}
	unexpected := filepath.Join(ownedDir, "unexpected")
	if err := os.WriteFile(unexpected, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := phone.send(ctx, map[string]any{"ch": "blob", "op": "abort", "id": id}); err != nil {
		t.Fatal(err)
	}
	cleanupResult := waitBlob(t, ctx, phone, id)
	if cleanupResult.Error == nil || cleanupResult.Error.Code != blobCodeIO || cleanupResult.Error.Kind != "spool_cleanup_failed" {
		t.Fatalf("cleanup result=%+v", cleanupResult)
	}
	snapshot := waitBlobUnavailable(t, ctx, h.srv, 1, 4, 1)
	if snapshot.AuxiliaryDispatches != 0 {
		t.Fatalf("cleanup failure dispatched to gateway: %+v", snapshot)
	}
	waitBlobConnectionsIdle(t, ctx, h.srv)
	for _, path := range []string{filepath.Join(ownedDir, blobPayloadName), unexpected} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("suspicious owned entry %q was removed: %v", filepath.Base(path), err)
		}
	}

	nextID := "59000000-0000-4000-8000-000000000002"
	emptyDigest := sha256.Sum256(nil)
	sendBlobBegin(t, ctx, phone, nextID, "runtime", "next.txt", 0, hex.EncodeToString(emptyDigest[:]))
	if result := waitBlob(t, ctx, phone, nextID); result.Error == nil || result.Error.Kind != "spool_unavailable" {
		t.Fatalf("begin after cleanup failure=%+v", result)
	}
	if snapshot := h.srv.blobUsageSnapshot(); snapshot.ActiveUploads != 1 || snapshot.ReservedBytes != 4 ||
		snapshot.CleanupFailures != 1 || snapshot.Unavailable != 1 || snapshot.AuxiliaryDispatches != 0 {
		t.Fatalf("accounting changed after rejected begin: %+v", snapshot)
	}

	_, accepted := h.connect(t, ctx)
	if accepted.Caps != nil && accepted.Caps.AttachmentBlob != nil {
		t.Fatalf("attachment capability advertised after cleanup failure: %+v", accepted.Caps.AttachmentBlob)
	}
}

func TestBlobSuccessfulCleanupHoldsReservationThroughResultDelivery(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.blobSpoolRoot = filepath.Join(t.TempDir(), "spool")
	manager := newBlobManager(deps.blobSpoolRoot, deps)
	if err := manager.reserve(4); err != nil {
		t.Fatal(err)
	}
	dirID, dir, path, file, err := manager.createPayload()
	if err != nil {
		t.Fatal(err)
	}
	link := newBlockingWritePhoneLink()
	bridgeSuite, err := protocol.SuiteFromKeys(bytes.Repeat([]byte{21}, 32), bytes.Repeat([]byte{22}, 32))
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{blobs: manager}
	connection := &conn{srv: server, ws: link, suite: bridgeSuite}
	blobs := newBlobConnection(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uploadCtx, uploadCancel := context.WithCancel(ctx)
	upload := &blobUpload{
		id: "59000000-0000-4000-8000-000000000005", size: 4, dirID: dirID, path: path, file: file,
		parent: ctx, ctx: uploadCtx, cancel: uploadCancel,
	}
	blobs.active = upload
	response := protocol.BlobMessage{Ch: protocol.ChBlob, Op: "result", ID: upload.id, Result: json.RawMessage(`{"ok":true}`)}
	done := make(chan struct{})
	go func() {
		blobs.finish(upload, &response, true)
		close(done)
	}()
	select {
	case <-link.entered:
	case <-ctx.Done():
		t.Fatal("result delivery did not start")
	}
	if snapshot := manager.snapshot(); snapshot.ActiveUploads != 1 || snapshot.ReservedBytes != 4 {
		t.Fatalf("result delivery released reservation early: %+v", snapshot)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("owned spool was not cleaned before result delivery: %v", err)
	}
	close(link.release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("result delivery did not finish")
	}
	if snapshot := manager.snapshot(); snapshot.ActiveUploads != 0 || snapshot.ReservedBytes != 0 {
		t.Fatalf("result delivery did not release reservation: %+v", snapshot)
	}
	if blobs.active != nil {
		t.Fatal("connection upload remained active after result delivery")
	}
}

func TestBlobStartupCleanupFailureDisablesCapability(t *testing.T) {
	var ownedDir, unexpected string
	h := newBlobTestHarness(t, func(deps *bridgeDependencies, _ *blobGatewayStub) {
		ownedDir = filepath.Join(deps.blobSpoolRoot, "59000000-0000-4000-8000-000000000003")
		if err := os.MkdirAll(ownedDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(ownedDir, blobPayloadName), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		unexpected = filepath.Join(ownedDir, "unexpected")
		if err := os.WriteFile(unexpected, []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snapshot := h.srv.blobUsageSnapshot()
	if snapshot.ActiveUploads != 0 || snapshot.ReservedBytes != 0 || snapshot.CleanupFailures != 1 || snapshot.Unavailable != 1 {
		t.Fatalf("startup cleanup state=%+v", snapshot)
	}
	for _, path := range []string{filepath.Join(ownedDir, blobPayloadName), unexpected} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("startup removed suspicious entry %q: %v", filepath.Base(path), err)
		}
	}
	phone, accepted := h.connect(t, ctx)
	if accepted.Caps != nil && accepted.Caps.AttachmentBlob != nil {
		t.Fatalf("attachment capability advertised after startup cleanup failure: %+v", accepted.Caps.AttachmentBlob)
	}
	emptyDigest := sha256.Sum256(nil)
	id := "59000000-0000-4000-8000-000000000004"
	sendBlobBegin(t, ctx, phone, id, "runtime", "next.txt", 0, hex.EncodeToString(emptyDigest[:]))
	if result := waitBlob(t, ctx, phone, id); result.Error == nil || result.Error.Kind != "spool_unavailable" {
		t.Fatalf("begin with unavailable startup spool=%+v", result)
	}
}

func TestBlobSpoolOwnershipAndStartupCleanup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "spool")
	owned := "60000000-0000-4000-8000-000000000001"
	ownedDir := filepath.Join(root, owned)
	if err := os.MkdirAll(ownedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownedDir, blobPayloadName), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(root, "do-not-remove")
	if err := os.MkdirAll(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "payload"), []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps := productionBridgeDependencies()
	manager := newBlobManager(root, deps)
	if !manager.available() {
		t.Fatalf("manager unavailable: %+v", manager.snapshot())
	}
	if _, err := os.Stat(ownedDir); !os.IsNotExist(err) {
		t.Fatalf("stale owned directory remains: %v", err)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("foreign directory removed: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("spool mode=%v err=%v", info.Mode().Perm(), err)
	}
	dirID, dir, path, file, err := manager.createPayload()
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if !isCanonicalUUID(dirID) || filepath.Base(path) != blobPayloadName || filepath.Dir(path) != dir {
		t.Fatalf("created dir=%q path=%q id=%q", dir, path, dirID)
	}
	payloadInfo, err := os.Stat(path)
	if err != nil || payloadInfo.Mode().Perm() != 0o600 {
		t.Fatalf("payload mode=%v err=%v", payloadInfo.Mode().Perm(), err)
	}
	if err := cleanupOwnedUpload(root, dirID); err != nil {
		t.Fatal(err)
	}

	symlinkRoot := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(root, symlinkRoot); err != nil {
		t.Fatal(err)
	}
	if got := newBlobManager(symlinkRoot, deps); got.available() {
		t.Fatal("symlink spool root accepted")
	}
}

func TestBlobPrimitiveValidation(t *testing.T) {
	for _, raw := range []string{"", "-1", "1.0", "01", "1e2", "9223372036854775808"} {
		if _, err := strictNonnegativeInt(json.RawMessage(raw)); err == nil {
			t.Fatalf("integer %q accepted", raw)
		}
	}
	for _, raw := range []string{"0", "1", "268435456"} {
		if _, err := strictNonnegativeInt(json.RawMessage(raw)); err != nil {
			t.Fatalf("integer %q rejected: %v", raw, err)
		}
	}
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "bad\x00name"} {
		if validOriginalAttachmentName(name) {
			t.Fatalf("original name %q accepted", name)
		}
	}
	if !validOriginalAttachmentName("bad\n\tname") {
		t.Fatal("original control-character name rejected before gateway sanitization")
	}
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "bad\u0085name"} {
		if validAttachmentName(name) {
			t.Fatalf("receipt name %q accepted", name)
		}
	}
	if !validAttachmentName("résumé [final].pdf") {
		t.Fatal("valid Unicode basename rejected")
	}
}

func TestBlobAllocationFailureSettlesCleanupBeforeReleasingReservation(t *testing.T) {
	for _, stage := range []string{"open", "chmod"} {
		for _, cleanupFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cleanup-fails-%v", stage, cleanupFails), func(t *testing.T) {
				h := newBlobTestHarness(t, nil)
				h.srv.blobs.openPayload = func(path string) (*os.File, error) {
					var file *os.File
					if stage == "chmod" {
						var err error
						file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
						if err != nil {
							return nil, err
						}
						if _, err := file.Write([]byte("part")); err != nil {
							_ = file.Close()
							return nil, err
						}
					}
					if cleanupFails {
						if err := os.WriteFile(filepath.Join(filepath.Dir(path), "unexpected"), []byte("preserve"), 0o600); err != nil {
							if file != nil {
								_ = file.Close()
							}
							return nil, err
						}
					}
					return file, fmt.Errorf("injected %s failure", stage)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				phone, _ := h.connect(t, ctx)
				id := "59000000-0000-4000-8000-000000000006"
				sendBlobBegin(t, ctx, phone, id, "runtime", "allocation.txt", 4, strings.Repeat("0", 64))
				result := waitBlob(t, ctx, phone, id)
				if result.Error == nil {
					t.Fatalf("allocation result=%+v", result)
				}
				if cleanupFails {
					if result.Error.Kind != "spool_cleanup_failed" {
						t.Fatalf("allocation cleanup result=%+v", result)
					}
					waitBlobUnavailable(t, ctx, h.srv, 1, 4, 1)
					if err := h.srv.blobs.reserve(0); err == nil {
						t.Fatal("failed cleanup allowed another reservation")
					}
				} else {
					if result.Error.Kind != "spool_io" {
						t.Fatalf("allocation result=%+v", result)
					}
					waitBlobUsage(t, ctx, h.srv, 0, 0)
					entries, err := os.ReadDir(h.srv.blobs.root)
					if err != nil || len(entries) != 0 {
						t.Fatalf("successful cleanup left entries=%v err=%v", entries, err)
					}
					if !h.srv.blobs.available() {
						t.Fatal("successful cleanup disabled manager")
					}
				}
				if h.srv.blobs.snapshot().AuxiliaryDispatches != 0 {
					t.Fatal("failed allocation reached gateway")
				}
			})
		}
	}
}
