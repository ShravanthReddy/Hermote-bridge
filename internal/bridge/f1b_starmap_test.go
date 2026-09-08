package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/relay"
	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
)

func TestStarmapMutationRoutesAreExact(t *testing.T) {
	accepted := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/learning/graph"},
		{http.MethodGet, "/api/learning/node"},
		{http.MethodPut, "/api/learning/node"},
		{http.MethodDelete, "/api/learning/node"},
	}
	for _, test := range accepted {
		if !routeAllowed(test.method, test.path) {
			t.Errorf("expected %s %s to be allowed", test.method, test.path)
		}
	}
	rejected := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/api/learning"},
		{http.MethodDelete, "/api/learning"},
		{http.MethodPut, "/api/learning/node/"},
		{http.MethodDelete, "/api/learning/node/child"},
		{http.MethodPut, "/api/learning/graph"},
		{http.MethodDelete, "/api/learning/graph"},
		{http.MethodPatch, "/api/learning/node"},
	}
	for _, test := range rejected {
		if routeAllowed(test.method, test.path) {
			t.Errorf("expected %s %s to be refused", test.method, test.path)
		}
	}
	for _, raw := range []string{
		"/api/learning/%6eode", "/api/learning/%2e/node", "/api/learning/node%2fchild",
		"/api/learning/node?profile=research", "/api//learning/node",
	} {
		if canonical, err := canonicalizePath(raw); err == nil && routeAllowed(http.MethodDelete, canonical.Path) {
			t.Errorf("ambiguous DELETE path %q was admitted as %+v", raw, canonical)
		}
	}
}

func TestStarmapDeleteBodyReachesGatewayThroughDirectAndRelayTunnels(t *testing.T) {
	for _, transport := range []string{"direct", "relay"} {
		t.Run(transport, func(t *testing.T) {
			captured := make(chan capturedStarmapRequest, 1)
			srv, hs := newStarmapTestServer(t, captured)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			phone, _ := protocol.NewIdentity(nil)
			code, _, err := srv.Pairings.Issue(time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			var client *phoneClient
			if transport == "direct" {
				client, err = connectPhone(t, ctx, hs, srv.Identity, phone, code)
			} else {
				client = connectStarmapRelayPhone(t, ctx, srv, phone, code)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.waitFor(ctx, protocol.ChWS, func(body []byte) bool {
				return strings.Contains(string(body), "gateway.ready")
			}); err != nil {
				t.Fatalf("gateway readiness: %v", err)
			}
			body := json.RawMessage(`{"id":"memory:fixture:1","profile":"research"}`)
			if err = client.send(ctx, protocol.HTTPRequest{
				Ch: protocol.ChHTTP, ID: 41, Method: http.MethodDelete,
				Path: "/api/learning/node", Body: body,
			}); err != nil {
				t.Fatal(err)
			}
			plain, err := client.waitFor(ctx, protocol.ChHTTP, func([]byte) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			var response protocol.HTTPResponse
			if err := json.Unmarshal(plain, &response); err != nil || response.ID != 41 || response.Status != http.StatusOK {
				t.Fatalf("response = %+v, %v", response, err)
			}
			got := receiveTestValue(t, ctx, captured, "learning DELETE upstream request")
			if got.method != http.MethodDelete || got.path != "/api/learning/node" ||
				got.contentType != "application/json" || !bytes.Equal(got.body, body) {
				t.Fatalf("upstream request = %+v", got)
			}
			_ = client.ws.Close(websocket.StatusNormalClosure, "")
		})
	}
}

type capturedStarmapRequest struct {
	method      string
	path        string
	contentType string
	body        []byte
}

func newStarmapTestServer(t *testing.T, captured chan<- capturedStarmapRequest) (*Server, *httptest.Server) {
	t.Helper()
	t.Setenv("HERMES_HOME", t.TempDir())
	store, err := state.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close Store: %v", err)
		}
	})
	identity, err := store.Identity()
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: os.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	authorized := func(request *http.Request) bool {
		return request.Header.Get("X-Hermes-Session-Token") == supervisor.Token() ||
			request.URL.Query().Get("token") == supervisor.Token()
	}
	mux.HandleFunc("/api/ws", func(writer http.ResponseWriter, request *http.Request) {
		if !authorized(request) {
			http.Error(writer, "forbidden", http.StatusForbidden)
			return
		}
		socket, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer socket.Close(websocket.StatusNormalClosure, "")
		_ = socket.Write(request.Context(), websocket.MessageText, []byte(
			`{"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{}}}`,
		))
		for {
			if _, _, err := socket.Read(request.Context()); err != nil {
				return
			}
		}
	})
	mux.HandleFunc("/api/learning/node", func(writer http.ResponseWriter, request *http.Request) {
		if !authorized(request) {
			http.Error(writer, "forbidden", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, "read body", http.StatusBadRequest)
			return
		}
		captured <- capturedStarmapRequest{
			method: request.Method, path: request.URL.EscapedPath(),
			contentType: request.Header.Get("Content-Type"), body: body,
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true,"message":"deleted"}`))
	})
	gatewayServer := httptest.NewServer(mux)
	t.Cleanup(gatewayServer.Close)
	port := gatewayServer.Listener.Addr().String()
	port = port[strings.LastIndex(port, ":")+1:]
	gateway.ForceReadyForTest(supervisor, port)
	server := New(identity, store, supervisor, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	return server, httpServer
}

func connectStarmapRelayPhone(
	t *testing.T, ctx context.Context, server *Server, phone *protocol.Identity, code []byte,
) *phoneClient {
	t.Helper()
	relayServer := relay.New(
		relay.Limits{MaxPhonesPerSession: 2}, slog.New(slog.NewTextHandler(os.Stderr, nil)),
	)
	httpRelay := httptest.NewServer(relayServer.Handler())
	t.Cleanup(httpRelay.Close)
	relayURL := "ws" + strings.TrimPrefix(httpRelay.URL, "http")
	dialer := &RelayDialer{Server: server, RelayURL: relayURL, Logger: server.Logger}
	dialContext, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go dialer.Run(dialContext)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if attached, _ := dialer.Attached(); attached {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attached, err := dialer.Attached(); !attached {
		t.Fatalf("relay did not attach: %v", err)
	}
	url := relayURL + "/v1/phone?s=" + server.Identity.SessionID()
	client, err := connectPhoneURL(t, ctx, url, server.Identity, phone, code)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
