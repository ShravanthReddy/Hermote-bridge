package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func newTestSupervisor(t *testing.T) *Supervisor {
	t.Helper()
	s, err := New(Options{
		Python:        "/usr/bin/true",
		Logger:        slog.New(slog.DiscardHandler),
		ProbeInterval: 5 * time.Millisecond,
		ProbeFailures: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWatchEndsAHungChildAfterConsecutiveMisses(t *testing.T) {
	s := newTestSupervisor(t)
	calls := 0
	s.probeFn = func(context.Context, int) error {
		calls++
		return errors.New("context deadline exceeded")
	}
	exited := make(chan struct{})
	if !s.watch(context.Background(), 1, exited) {
		t.Fatal("expected the watchdog to ask for a kill")
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 probes before the kill, got %d", calls)
	}
}

func TestWatchForgivesIntermittentMisses(t *testing.T) {
	s := newTestSupervisor(t)
	calls := 0
	s.probeFn = func(context.Context, int) error {
		calls++
		if calls%2 == 0 {
			return errors.New("slow")
		}
		return nil
	}
	exited := make(chan struct{})
	go func() {
		time.Sleep(60 * time.Millisecond)
		close(exited)
	}()
	if s.watch(context.Background(), 1, exited) {
		t.Fatal("alternating misses must not count as consecutive")
	}
	if calls < 4 {
		t.Fatalf("expected several probes, got %d", calls)
	}
}

func TestWatchStopsWithTheChild(t *testing.T) {
	s := newTestSupervisor(t)
	s.probeFn = func(context.Context, int) error { return nil }
	exited := make(chan struct{})
	close(exited)
	if s.watch(context.Background(), 1, exited) {
		t.Fatal("an exited child needs no kill")
	}
}
