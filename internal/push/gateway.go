package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// WSGateway calls the loopback gateway's JSON-RPC over one WebSocket that
// is dialed lazily and redialed after any failure. One call at a time: the
// watcher is the only caller and issues a handful of RPCs per poll.
type WSGateway struct {
	// BaseURL returns the gateway child's http base and whether it is up.
	BaseURL func() (string, bool)
	// Token is the gateway session token (`?token=`).
	Token func() string

	mu   sync.Mutex
	conn *websocket.Conn
	next atomic.Int64
}

// Call sends one request and waits for its reply, skipping unrelated frames
// (events for sessions this connection never resumed still arrive as
// broadcasts).
func (g *WSGateway) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	conn, err := g.connection(ctx)
	if err != nil {
		return nil, err
	}
	id := g.next.Add(1)
	frame, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := conn.Write(callCtx, websocket.MessageText, frame); err != nil {
		g.reset()
		return nil, err
	}
	for {
		_, raw, err := conn.Read(callCtx)
		if err != nil {
			g.reset()
			return nil, err
		}
		var reply struct {
			ID     json.Number     `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &reply); err != nil || reply.ID.String() != fmt.Sprint(id) {
			continue
		}
		if reply.Error != nil {
			return nil, fmt.Errorf("%s: %d %s", method, reply.Error.Code, reply.Error.Message)
		}
		return reply.Result, nil
	}
}

func (g *WSGateway) connection(ctx context.Context) (*websocket.Conn, error) {
	if g.conn != nil {
		return g.conn, nil
	}
	base, ok := g.BaseURL()
	if !ok {
		return nil, errors.New("gateway not ready")
	}
	u := "ws" + strings.TrimPrefix(base, "http") + "/api/ws?token=" + url.QueryEscape(g.Token())
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, u, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(8 << 20)
	g.conn = conn
	return conn, nil
}

func (g *WSGateway) reset() {
	if g.conn != nil {
		_ = g.conn.Close(websocket.StatusNormalClosure, "")
		g.conn = nil
	}
}

// Close drops the connection.
func (g *WSGateway) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reset()
}
