package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// tunnel forwards frames between the phone and this connection's own gateway
// WebSocket until either side closes.
func (c *conn) tunnel(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gw, err := c.dialGateway(ctx)
	if err != nil {
		_ = c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlClose, Reason: "gateway unavailable"})
		return err
	}
	defer gw.Close(websocket.StatusNormalClosure, "")
	gw.SetReadLimit(maxFrame)
	c.terminals = newTerminalManager(c.sendJSON)
	defer c.terminals.closeAll()
	_ = c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlGateway, State: string(gateway.StateReady)})

	errc := make(chan error, 3)

	// gateway → phone
	go func() {
		for {
			typ, data, err := gw.Read(ctx)
			if err != nil {
				errc <- fmt.Errorf("gateway read: %w", err)
				return
			}
			if typ != websocket.MessageText {
				data = []byte(string(data)) // gateway only speaks text; tolerate binary as UTF-8
			}
			if err := c.sendJSON(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: string(data)}); err != nil {
				errc <- err
				return
			}
		}
	}()

	// phone → gateway / http / ctl
	go func() {
		var asm protocol.ChunkAssembler
		for {
			plain, err := c.recv(ctx, &asm)
			if err != nil {
				errc <- err
				return
			}
			if err := c.dispatch(ctx, gw, plain); err != nil {
				errc <- err
				return
			}
		}
	}()

	// liveness + gateway state relay
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		states := c.srv.Gateway.Watch()
		defer c.srv.Gateway.Unwatch(states)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlPing}); err != nil {
					errc <- err
					return
				}
			case st := <-states:
				_ = c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlGateway, State: string(st)})
				if st != gateway.StateReady {
					// This connection's gateway socket is gone with the child; the
					// phone reconnects and replays (ADR-008) once the child is back.
					errc <- errors.New("gateway restarted")
					return
				}
			}
		}
	}()

	return <-errc
}

// dialGateway opens this connection's private socket to the gateway child,
// waiting for the child to become ready if it is (re)starting.
func (c *conn) dialGateway(ctx context.Context) (*websocket.Conn, error) {
	deadline := time.Now().Add(gatewayWait)
	for {
		if base, ok := c.srv.Gateway.BaseURL(); ok {
			u := "ws" + strings.TrimPrefix(base, "http") + "/api/ws?token=" + url.QueryEscape(c.srv.Gateway.Token())
			dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			gw, _, err := websocket.Dial(dctx, u, nil)
			cancel()
			if err == nil {
				return gw, nil
			}
			c.srv.Logger.Warn("gateway dial failed", "err", err)
		} else {
			_ = c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlGateway, State: string(c.srv.Gateway.State())})
		}
		if time.Now().After(deadline) {
			return nil, errors.New("gateway not ready")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (c *conn) dispatch(ctx context.Context, gw *websocket.Conn, plain []byte) error {
	ch, err := protocol.PeekChannel(plain)
	if err != nil {
		return err
	}
	switch ch {
	case protocol.ChWS:
		var m protocol.WSMessage
		if err := json.Unmarshal(plain, &m); err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return gw.Write(wctx, websocket.MessageText, []byte(m.Data))
	case protocol.ChHTTP:
		var req protocol.HTTPRequest
		if err := json.Unmarshal(plain, &req); err != nil {
			return err
		}
		go c.proxyHTTP(ctx, req)
		return nil
	case protocol.ChCtl:
		var m protocol.CtlMessage
		if err := json.Unmarshal(plain, &m); err != nil {
			return err
		}
		switch m.Op {
		case protocol.CtlPing:
			return c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlPong})
		case protocol.CtlPong:
			return nil
		case protocol.CtlClose:
			return errors.New("phone closed: " + m.Reason)
		case protocol.CtlName:
			return c.srv.Store.Trust(mustDecodeID(c.deviceID), m.Reason)
		case protocol.CtlPush:
			if m.Push != nil && c.srv.OnPush != nil {
				c.srv.OnPush(c.deviceID, *m.Push)
			}
			return nil
		}
		return nil
	case protocol.ChPTY:
		var m protocol.PTYMessage
		if err := json.Unmarshal(plain, &m); err != nil {
			return err
		}
		if c.terminals == nil {
			return nil
		}
		return c.terminals.handle(ctx, m)
	case protocol.ChConfirm:
		return errors.New("unexpected confirm after handshake")
	}
	return protocol.ErrUnknownChannel
}

// proxyHTTP performs one REST call against the loopback gateway with the
// session token and returns the response on the http channel.
func (c *conn) proxyHTTP(ctx context.Context, req protocol.HTTPRequest) {
	reply := func(status int, body []byte) {
		_ = c.sendJSON(ctx, protocol.HTTPResponse{Ch: protocol.ChHTTP, ID: req.ID, Status: status, Body: body})
	}
	if !routeAllowed(req.Method, req.Path) {
		reply(http.StatusForbidden, []byte(RefusedRouteError))
		return
	}
	if req.Method == http.MethodGet && req.Path == "/api/fs/list" {
		// Answered here, off the gateway's event loop (see fs.go).
		status, body := fsList(ctx, req.Query)
		reply(status, body)
		return
	}
	base, ok := c.srv.Gateway.BaseURL()
	if !ok {
		reply(http.StatusServiceUnavailable, []byte(`{"error":"gateway not ready"}`))
		return
	}
	target := base + req.Path
	if req.Query != "" {
		target += "?" + req.Query
	}
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, target, body)
	if err != nil {
		reply(http.StatusBadRequest, []byte(`{"error":`+strconv.Quote(err.Error())+`}`))
		return
	}
	hreq.Header.Set("X-Hermes-Session-Token", c.srv.Gateway.Token())
	if body != nil {
		hreq.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.srv.httpClient.Do(hreq)
	if err != nil {
		reply(http.StatusBadGateway, []byte(`{"error":`+strconv.Quote(err.Error())+`}`))
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, protocol.MaxPlaintext))
	if err != nil {
		reply(http.StatusBadGateway, []byte(`{"error":"read failed"}`))
		return
	}
	if !json.Valid(data) {
		// Non-JSON bodies (e.g. /api/media bytes) travel as a JSON string.
		data, _ = json.Marshal(string(data))
	}
	reply(resp.StatusCode, data)
}

func mustDecodeID(id string) []byte {
	raw, _ := protocol.DecodeDeviceID(id)
	return raw
}
