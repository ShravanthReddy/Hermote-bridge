package bridge

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

const (
	publicRelayNew    = "wss://relay.hermote.app"
	publicRelayLegacy = "wss://129-213-134-110.sslip.io"
)

// This opt-in test uses only fresh test identities and the fake loopback
// gateway. It never reads local bridge state or interrupts the public relay.
func TestPublicRelayHostnameInteroperability(t *testing.T) {
	if os.Getenv("HERMOTE_TEST_PUBLIC_RELAY") != "1" {
		t.Skip("public relay interoperability test not requested")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	srv, _ := newTestServer(t)
	phone, err := protocol.NewIdentity(nil)
	if err != nil {
		t.Fatal(err)
	}
	code, _, err := srv.Pairings.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	first := startPublicRelayDialer(t, ctx, srv, publicRelayNew)
	defer first.stop(t)
	phoneURL := publicRelayLegacy + "/v1/phone?s=" + srv.Identity.SessionID()
	p, err := connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, code)
	if err != nil {
		t.Fatalf("new bridge host to legacy phone host: %v", err)
	}
	assertPublicRelayGateway(t, ctx, p, 7101)
	_ = p.ws.Close(websocket.StatusNormalClosure, "")
	first.stop(t)

	second := startPublicRelayDialer(t, ctx, srv, publicRelayLegacy)
	defer second.stop(t)
	phoneURL = publicRelayNew + "/v1/phone?s=" + srv.Identity.SessionID()
	p, err = connectPhoneURL(t, ctx, phoneURL, srv.Identity, phone, nil)
	if err != nil {
		t.Fatalf("legacy bridge host to new phone host: %v", err)
	}
	defer p.ws.Close(websocket.StatusNormalClosure, "")
	assertPublicRelayGateway(t, ctx, p, 7102)
}

type publicRelayDialerHandle struct {
	cancel context.CancelFunc
	done   <-chan struct{}
	once   sync.Once
}

func startPublicRelayDialer(t *testing.T, parent context.Context, srv *Server, relayURL string) *publicRelayDialerHandle {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	dialer := &RelayDialer{Server: srv, RelayURL: relayURL, Logger: srv.Logger}
	go func() {
		defer close(done)
		dialer.Run(ctx)
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if attached, _ := dialer.Attached(); attached {
			return &publicRelayDialerHandle{cancel: cancel, done: done}
		}
		select {
		case <-parent.Done():
			cancel()
			t.Fatalf("attach to %s: %v", relayURL, parent.Err())
		case <-ticker.C:
		}
	}
}

func (h *publicRelayDialerHandle) stop(t *testing.T) {
	t.Helper()
	h.once.Do(h.cancel)
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		t.Fatal("test relay dialer did not stop")
	}
}

func assertPublicRelayGateway(t *testing.T, ctx context.Context, phone *phoneClient, id int) {
	t.Helper()
	if _, err := phone.waitFor(ctx, protocol.ChWS, func(raw []byte) bool {
		return json.Valid(raw) && containsGatewayReady(raw)
	}); err != nil {
		t.Fatalf("gateway.ready through public relay: %v", err)
	}

	request, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": "session.list", "params": map[string]any{}})
	if err := phone.send(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: string(request)}); err != nil {
		t.Fatalf("send session.list through public relay: %v", err)
	}
	if _, err := phone.waitFor(ctx, protocol.ChWS, func(raw []byte) bool {
		var frame protocol.WSMessage
		if json.Unmarshal(raw, &frame) != nil {
			return false
		}
		var response struct {
			ID     int `json:"id"`
			Result struct {
				Echo string `json:"echo"`
			} `json:"result"`
		}
		return json.Unmarshal([]byte(frame.Data), &response) == nil && response.ID == id && response.Result.Echo == "session.list"
	}); err != nil {
		t.Fatalf("session.list response through public relay: %v", err)
	}
}

func containsGatewayReady(raw []byte) bool {
	var frame protocol.WSMessage
	if json.Unmarshal(raw, &frame) != nil {
		return false
	}
	var event struct {
		Method string `json:"method"`
		Params struct {
			Type string `json:"type"`
		} `json:"params"`
	}
	return json.Unmarshal([]byte(frame.Data), &event) == nil && event.Method == "event" && event.Params.Type == "gateway.ready"
}
