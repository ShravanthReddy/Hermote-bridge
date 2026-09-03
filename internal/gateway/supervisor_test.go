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

func TestUnwatchDropsTheWatcher(t *testing.T) {
	s := newTestSupervisor(t)
	kept := s.Watch()
	dropped := s.Watch()
	s.Unwatch(dropped)
	s.setState(StateReady, 1234)
	select {
	case st := <-kept:
		if st != StateReady {
			t.Fatalf("kept watcher saw %q", st)
		}
	default:
		t.Fatal("kept watcher received nothing")
	}
	select {
	case st := <-dropped:
		t.Fatalf("dropped watcher still received %q", st)
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.watchers) != 1 {
		t.Fatalf("expected 1 registered watcher, got %d", len(s.watchers))
	}
}

func TestUnwatchIgnoresAnUnknownChannel(t *testing.T) {
	s := newTestSupervisor(t)
	_ = s.Watch()
	s.Unwatch(make(chan State))
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.watchers) != 1 {
		t.Fatalf("expected the registered watcher to survive, got %d", len(s.watchers))
	}
}
