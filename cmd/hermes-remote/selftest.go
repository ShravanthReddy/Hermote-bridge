package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/control"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

// cmdSelftest pairs a throw-away in-process "phone" with the running daemon
// over loopback and exercises the tunnel: handshake, gateway.ready, one RPC,
// one REST call. It then revokes the throw-away device. Useful after `up`
// and for bug reports ("does the Mac side work at all?").
func cmdSelftest() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, err := store.Config()
	if err != nil {
		return err
	}
	client := control.NewClient(store.Path("control.sock"))
	st, err := client.Status(ctx)
	if err != nil {
		return err
	}
	bold("Self-test")
	pair, err := client.Pair(ctx)
	if err != nil {
		return err
	}
	payload, err := protocol.ParsePair(pair.URL)
	if err != nil {
		return fmt.Errorf("pair payload: %w", err)
	}
	ok("Pair payload parses: session %s, transport %s, url %s", payload.SessionID, payload.Transport, payload.URL)

	phone, _ := protocol.NewIdentity(nil)
	ws, _, err := websocket.Dial(ctx, "ws://"+st.BridgeAddr+"/v1/bridge", nil)
	if err != nil {
		return fmt.Errorf("dial bridge on loopback: %w", err)
	}
	defer ws.Close(websocket.StatusNormalClosure, "")
	ws.SetReadLimit(protocol.MaxPlaintext + 64)

	hello, ps, err := protocol.PhoneHello(phone, payload.SessionID, nil)
	if err != nil {
		return err
	}
	raw, _ := json.Marshal(hello)
	if err := ws.Write(ctx, websocket.MessageText, raw); err != nil {
		return err
	}
	_, acceptRaw, err := ws.Read(ctx)
	if err != nil {
		return fmt.Errorf("no accept: %w", err)
	}
	var accept protocol.Accept
	if err := json.Unmarshal(acceptRaw, &accept); err != nil {
		return err
	}
	confirm, suite, err := ps.Finish(accept, payload.BridgeKey, payload.Code)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, confirm); err != nil {
		return err
	}
	ok("Handshake complete (bridge key verified, pairing code accepted)")
	defer func() {
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		if _, err := client.Revoke(rctx, protocol.DeviceID(phone.Public())); err == nil {
			ok("Throw-away test device revoked")
		}
	}()

	var asm protocol.ChunkAssembler
	recv := func(want string, pred func([]byte) bool) ([]byte, error) {
		for {
			_, frame, err := ws.Read(ctx)
			if err != nil {
				return nil, err
			}
			plain, err := suite.Open(frame)
			if err != nil {
				return nil, err
			}
			ch, err := protocol.PeekChannel(plain)
			if err != nil {
				return nil, err
			}
			if ch == protocol.ChChunk {
				var c protocol.Chunk
				_ = json.Unmarshal(plain, &c)
				if plain, err = asm.Add(c); err != nil {
					return nil, err
				} else if plain == nil {
					continue
				}
				ch, _ = protocol.PeekChannel(plain)
			}
			if ch == protocol.ChCtl {
				var m protocol.CtlMessage
				_ = json.Unmarshal(plain, &m)
				if m.Op == protocol.CtlClose {
					return nil, errors.New("bridge closed: " + m.Reason)
				}
			}
			if ch == want && pred(plain) {
				return plain, nil
			}
		}
	}
	send := func(v any) error {
		raw, _ := json.Marshal(v)
		frame, err := suite.Seal(raw)
		if err != nil {
			return err
		}
		return ws.Write(ctx, websocket.MessageBinary, frame)
	}

	if _, err := recv(protocol.ChWS, func(b []byte) bool { return strings.Contains(string(b), "gateway.ready") }); err != nil {
		return fmt.Errorf("no gateway.ready through the tunnel: %w", err)
	}
	ok("gateway.ready received through the encrypted tunnel")

	if err := send(protocol.WSMessage{Ch: protocol.ChWS, Data: `{"jsonrpc":"2.0","id":1,"method":"session.list","params":{"limit":1}}`}); err != nil {
		return err
	}
	plain, err := recv(protocol.ChWS, func(b []byte) bool {
		var m protocol.WSMessage
		_ = json.Unmarshal(b, &m)
		return strings.Contains(m.Data, `"id": 1`) || strings.Contains(m.Data, `"id":1`)
	})
	if err != nil {
		return fmt.Errorf("session.list: %w", err)
	}
	var m protocol.WSMessage
	_ = json.Unmarshal(plain, &m)
	ok("RPC session.list answered (%d bytes)", len(m.Data))

	if err := send(protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: 1, Method: "GET", Path: "/api/status"}); err != nil {
		return err
	}
	plain, err = recv(protocol.ChHTTP, func([]byte) bool { return true })
	if err != nil {
		return err
	}
	var resp protocol.HTTPResponse
	_ = json.Unmarshal(plain, &resp)
	var status struct {
		Version      string `json:"version"`
		AuthRequired bool   `json:"auth_required"`
	}
	_ = json.Unmarshal(resp.Body, &status)
	if resp.Status != 200 {
		return fmt.Errorf("REST /api/status returned %d: %s", resp.Status, resp.Body)
	}
	ok("REST /api/status → 200, Hermes %s, gateway gate off on loopback (auth_required=%v) as designed", status.Version, status.AuthRequired)
	if cfg.Transport == protocol.TransportDirect {
		fmt.Printf("  Phones connect via %s (needs Tailscale on the phone).\n", payload.URL)
	}
	return nil
}
