package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// RelayDialer keeps one multiplexed WebSocket to the relay (docs/REMOTE-ACCESS.md
// §6) and turns every channel the relay opens into a phone connection served
// by the bridge exactly like a direct one. Reconnects with backoff 1 s → 30 s.
type RelayDialer struct {
	Server   *Server
	RelayURL string // wss://relay.example (no path)
	Logger   *slog.Logger

	mu       sync.RWMutex
	attached bool
	lastErr  string
}

// Attached reports whether the relay currently holds this bridge's session.
func (d *RelayDialer) Attached() (bool, string) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.attached, d.lastErr
}

func (d *RelayDialer) setAttached(v bool, err error) {
	d.mu.Lock()
	d.attached = v
	if err != nil {
		d.lastErr = err.Error()
	} else {
		d.lastErr = ""
	}
	d.mu.Unlock()
}

// Run dials until ctx ends.
func (d *RelayDialer) Run(ctx context.Context) {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	backoff := time.Second
	for {
		started := time.Now()
		err := d.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		d.setAttached(false, err)
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		d.Logger.Warn("relay connection ended", "err", err, "retry_in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (d *RelayDialer) runOnce(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	url := strings.TrimRight(d.RelayURL, "/") + "/v1/bridge"
	dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
	ws, _, err := websocket.Dial(dctx, url, nil)
	dcancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer ws.Close(websocket.StatusNormalClosure, "")
	ws.SetReadLimit(maxFrame + 64)

	// Challenge → attach → attached.
	hctx, hcancel := context.WithTimeout(ctx, 15*time.Second)
	defer hcancel()
	typ, raw, err := ws.Read(hctx)
	if err != nil {
		return fmt.Errorf("challenge: %w", err)
	}
	var chal protocol.RelayChallenge
	if typ != websocket.MessageText || json.Unmarshal(raw, &chal) != nil || chal.T != "challenge" {
		return errors.New("relay: expected challenge")
	}
	attach, _ := json.Marshal(protocol.RelayAttach{
		T: "attach", S: d.Server.Identity.SessionID(), K: d.Server.Identity.Public(),
		Sig: d.Server.Identity.SignRelayAttach(chal.Nonce),
	})
	if err := ws.Write(hctx, websocket.MessageText, attach); err != nil {
		return err
	}
	typ, raw, err = ws.Read(hctx)
	if err != nil {
		return fmt.Errorf("attach rejected: %w", err)
	}
	var ack struct {
		T string `json:"t"`
	}
	if typ != websocket.MessageText || json.Unmarshal(raw, &ack) != nil || ack.T != "attached" {
		return errors.New("relay: attach not acknowledged")
	}
	d.setAttached(true, nil)
	d.Logger.Info("attached to relay", "relay", d.RelayURL)

	mux := &relayMux{ws: ws, channels: map[uint16]*relayChannel{}}
	defer func() {
		// Drop the socket first so in-flight writes fail, then finish channels.
		ws.Close(websocket.StatusGoingAway, "")
		mux.closeAll()
	}()

	for {
		rctx, rcancel := context.WithTimeout(ctx, idleTimeout+30*time.Second)
		typ, frame, err := ws.Read(rctx)
		rcancel()
		if err != nil {
			return err
		}
		if typ != websocket.MessageBinary {
			continue
		}
		ch, kind, payload, err := protocol.ParseRelayFrame(frame)
		if err != nil {
			continue
		}
		if ch == protocol.RelayControlChannel {
			var c protocol.RelayControl
			if json.Unmarshal(payload, &c) != nil {
				continue
			}
			switch c.T {
			case "open":
				rc := mux.open(c.C)
				go d.Server.serveLink(ctx, rc, fmt.Sprintf("relay:%d", c.C))
			case "close":
				mux.remoteClosed(c.C)
			}
			continue
		}
		mux.deliver(ch, kind, payload)
	}
}

// relayMux owns the relay socket's write side and the live channels.
type relayMux struct {
	ws       *websocket.Conn
	writeMu  sync.Mutex
	mu       sync.Mutex
	channels map[uint16]*relayChannel
}

type relayMsg struct {
	typ  websocket.MessageType
	data []byte
}

func (m *relayMux) write(ctx context.Context, frame []byte) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return m.ws.Write(wctx, websocket.MessageBinary, frame)
}

func (m *relayMux) open(ch uint16) *relayChannel {
	rc := &relayChannel{mux: m, ch: ch, inbox: make(chan relayMsg, 256), done: make(chan struct{})}
	m.mu.Lock()
	if old := m.channels[ch]; old != nil {
		old.finish()
	}
	m.channels[ch] = rc
	m.mu.Unlock()
	return rc
}

func (m *relayMux) deliver(ch uint16, kind byte, payload []byte) {
	m.mu.Lock()
	rc := m.channels[ch]
	m.mu.Unlock()
	if rc == nil {
		return
	}
	typ := websocket.MessageBinary
	if kind == protocol.RelayKindText {
		typ = websocket.MessageText
	}
	select {
	case rc.inbox <- relayMsg{typ: typ, data: payload}:
	default:
		// The phone is flooding faster than the gateway drains; drop the channel.
		_ = rc.Close(websocket.StatusPolicyViolation, "backlog")
	}
}

func (m *relayMux) remoteClosed(ch uint16) {
	m.mu.Lock()
	rc := m.channels[ch]
	delete(m.channels, ch)
	m.mu.Unlock()
	if rc != nil {
		rc.finish()
	}
}

func (m *relayMux) closeAll() {
	m.mu.Lock()
	chans := m.channels
	m.channels = map[uint16]*relayChannel{}
	m.mu.Unlock()
	for _, rc := range chans {
		rc.finish()
	}
}

// relayChannel is one phone seen through the relay; implements phoneLink.
type relayChannel struct {
	mux   *relayMux
	ch    uint16
	inbox chan relayMsg
	done  chan struct{}
	once  sync.Once
}

func (rc *relayChannel) finish() { rc.once.Do(func() { close(rc.done) }) }

func (rc *relayChannel) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case m := <-rc.inbox:
		return m.typ, m.data, nil
	case <-rc.done:
		return 0, nil, errors.New("relay channel closed")
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (rc *relayChannel) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	select {
	case <-rc.done:
		return errors.New("relay channel closed")
	default:
	}
	kind := protocol.RelayKindBinary
	if typ == websocket.MessageText {
		kind = protocol.RelayKindText
	}
	// Abort the write as soon as the channel (or the whole relay socket) goes
	// away, instead of waiting out the socket write timeout.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-rc.done:
			cancel()
		case <-wctx.Done():
		}
	}()
	return rc.mux.write(wctx, protocol.RelayFrame(rc.ch, kind, p))
}

func (rc *relayChannel) Close(_ websocket.StatusCode, reason string) error {
	var err error
	rc.once.Do(func() {
		close(rc.done)
		rc.mux.mu.Lock()
		if rc.mux.channels[rc.ch] == rc {
			delete(rc.mux.channels, rc.ch)
		}
		rc.mux.mu.Unlock()
		raw, _ := json.Marshal(protocol.RelayControl{T: "close", C: rc.ch, Reason: reason})
		err = rc.mux.write(context.Background(), protocol.RelayFrame(protocol.RelayControlChannel, protocol.RelayKindText, raw))
	})
	return err
}
