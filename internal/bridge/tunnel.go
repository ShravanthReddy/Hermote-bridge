package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
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
	defer c.blobs.shutdownAndWait()

	gw, err := c.dialGateway(ctx)
	if err != nil {
		_ = c.sendJSON(ctx, protocol.CtlMessage{Ch: protocol.ChCtl, Op: protocol.CtlClose, Reason: "gateway unavailable"})
		return err
	}
	defer gw.Close(websocket.StatusNormalClosure, "")
	gw.SetReadLimit(maxFrame)
	var failOnce sync.Once
	var firstErr error
	failed := make(chan struct{})
	fail := func(err error) {
		if err == nil {
			return
		}
		failOnce.Do(func() {
			firstErr = err
			cancel()
			close(failed)
		})
	}
	c.terminals = newTerminalManager(func(sendCtx context.Context, value any) error {
		err := c.sendJSON(sendCtx, value)
		if err != nil {
			fail(err)
		}
		return err
	})
	defer c.terminals.closeAll()
	if err := c.sendJSON(ctx, protocol.CtlMessage{
		Ch: protocol.ChCtl, Op: protocol.CtlGateway, State: string(gateway.StateReady),
	}); err != nil {
		return err
	}

	var loops sync.WaitGroup
	loops.Add(3)

	// gateway → phone
	go func() {
		defer loops.Done()
		for {
			typ, data, err := gw.Read(ctx)
			if err != nil {
				fail(fmt.Errorf("gateway read: %w", err))
				return
			}
			if typ != websocket.MessageText {
				data = []byte(string(data)) // gateway only speaks text; tolerate binary as UTF-8
			}
			if err := c.sendJSON(ctx, protocol.WSMessage{Ch: protocol.ChWS, Data: string(data)}); err != nil {
				if errors.Is(err, errPlaintextLimit) {
					c.bestEffortGatewayLimitClose()
				}
				fail(err)
				return
			}
		}
	}()

	// phone → gateway / http / ctl
	go func() {
		defer loops.Done()
		var asm protocol.ChunkAssembler
		for {
			plain, err := c.recv(ctx, &asm)
			if err != nil {
				fail(err)
				return
			}
			if err := c.dispatch(ctx, gw, plain, fail); err != nil {
				fail(err)
				return
			}
		}
	}()

	// liveness + gateway state relay
	go func() {
		defer loops.Done()
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
					fail(err)
					return
				}
			case st, ok := <-states:
				if !ok {
					fail(errors.New("gateway state watcher closed"))
					return
				}
				if err := c.sendJSON(ctx, protocol.CtlMessage{
					Ch: protocol.ChCtl, Op: protocol.CtlGateway, State: string(st),
				}); err != nil {
					fail(err)
					return
				}
				if st != gateway.StateReady {
					// This connection's gateway socket is gone with the child; the
					// phone reconnects and replays (ADR-008) once the child is back.
					fail(errors.New("gateway restarted"))
					return
				}
			}
		}
	}()

	select {
	case <-failed:
	case <-ctx.Done():
		fail(ctx.Err())
		<-failed
	}
	loops.Wait()
	return firstErr
}

func (c *conn) bestEffortGatewayLimitClose() {
	if !c.sendMu.TryLock() {
		return
	}
	defer c.sendMu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.Background(), c.srv.deps.closeWriteTimeout)
	defer cancel()
	raw, err := json.Marshal(protocol.CtlMessage{
		Ch: protocol.ChCtl, Op: protocol.CtlClose, Reason: "gateway frame exceeds bridge limit",
	})
	if err == nil {
		_ = c.sendLocked(closeCtx, raw)
	}
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

func (c *conn) dispatch(
	ctx context.Context, gw *websocket.Conn, plain []byte, fail func(error),
) error {
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
		return c.startProxyHTTP(ctx, req, fail)
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
	case protocol.ChBlob:
		return c.blobs.handle(ctx, plain, fail)
	case protocol.ChConfirm:
		return errors.New("unexpected confirm after handshake")
	}
	return protocol.ErrUnknownChannel
}

func mustDecodeID(id string) []byte {
	raw, _ := protocol.DecodeDeviceID(id)
	return raw
}
