package bridge

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// recorder collects the frames a terminal manager sends to the "phone".
type recorder struct {
	mu     sync.Mutex
	frames []protocol.PTYMessage
	got    chan struct{}
}

func newRecorder() *recorder { return &recorder{got: make(chan struct{}, 64)} }

func (r *recorder) send(_ context.Context, v any) error {
	raw, _ := json.Marshal(v)
	var m protocol.PTYMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	r.mu.Lock()
	r.frames = append(r.frames, m)
	r.mu.Unlock()
	select {
	case r.got <- struct{}{}:
	default:
	}
	return nil
}

func (r *recorder) output() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	for _, f := range r.frames {
		if f.Op == protocol.PTYData {
			b.Write(f.Data)
		}
	}
	return b.String()
}

func (r *recorder) has(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.frames {
		if f.Op == op {
			return true
		}
	}
	return false
}

func (r *recorder) waitFor(t *testing.T, op string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for !r.has(op) {
		select {
		case <-r.got:
		case <-deadline:
			t.Fatalf("no %q frame arrived; frames: %+v", op, r.frames)
		}
	}
}

func TestTerminalRunsAShellAndReportsExit(t *testing.T) {
	rec := newRecorder()
	m := newTerminalManager(rec.send)
	m.shell = "/bin/sh"
	ctx := context.Background()
	if err := m.handle(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYOpen, ID: "t1", Cols: 80, Rows: 24}); err != nil {
		t.Fatal(err)
	}
	if err := m.handle(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYData, ID: "t1", Data: []byte("echo hermes-pty-ok; exit 7\n")}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, protocol.PTYExit)
	if !strings.Contains(rec.output(), "hermes-pty-ok") {
		t.Fatalf("shell output missing: %q", rec.output())
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, f := range rec.frames {
		if f.Op == protocol.PTYExit && f.Code != 7 {
			t.Fatalf("exit code = %d, want 7", f.Code)
		}
	}
	if len(m.open) != 0 {
		t.Fatalf("terminal not forgotten after exit")
	}
}

func TestTerminalRefusesDuplicateIDsAndClosesOnDemand(t *testing.T) {
	rec := newRecorder()
	m := newTerminalManager(rec.send)
	m.shell = "/bin/sh"
	ctx := context.Background()
	open := protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYOpen, ID: "dup"}
	if err := m.handle(ctx, open); err != nil {
		t.Fatal(err)
	}
	if err := m.handle(ctx, open); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, protocol.PTYClose)
	m.handle(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYClose, ID: "dup"})
	rec.waitFor(t, protocol.PTYExit)
	if err := m.handle(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: "bogus", ID: "x"}); err == nil {
		t.Fatal("unknown op should error")
	}
}

func TestPTYChannelIsRecognised(t *testing.T) {
	raw, _ := json.Marshal(protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYData, ID: "a", Data: []byte{1, 2, 3}})
	ch, err := protocol.PeekChannel(raw)
	if err != nil || ch != protocol.ChPTY {
		t.Fatalf("peek = %q, %v", ch, err)
	}
	var back protocol.PTYMessage
	if err := json.Unmarshal(raw, &back); err != nil || string(back.Data) != "\x01\x02\x03" {
		t.Fatalf("round trip lost data: %v %v", err, back.Data)
	}
}
