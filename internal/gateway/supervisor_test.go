package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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

// scriptedChild writes a stand-in interpreter whose nth start follows plan[n-1]:
// "fail" exits before the readiness line, "hang" reports ready and never exits,
// "exit" reports ready and exits shortly after. Every start after the first
// waits for the file go-<n> so the test can read Health between restarts.
func scriptedChild(t *testing.T, port int, plan []string) (script, dir string) {
	t.Helper()
	dir = t.TempDir()
	var cases strings.Builder
	for i, step := range plan {
		n := i + 1
		fmt.Fprintf(&cases, "%d)\n", n)
		if n > 1 {
			fmt.Fprintf(&cases, "  while [ ! -f %q/go-%d ]; do sleep 0.01; done\n", dir, n)
		}
		switch step {
		case "fail":
			cases.WriteString("  exit 3 ;;\n")
		case "hang":
			fmt.Fprintf(&cases, "  echo HERMES_BACKEND_READY port=%d\n  exec sleep 60 ;;\n", port)
		case "exit":
			fmt.Fprintf(&cases, "  echo HERMES_BACKEND_READY port=%d\n  sleep 0.2\n  exit 0 ;;\n", port)
		}
	}
	body := fmt.Sprintf(`#!/bin/sh
n=$(cat %[1]q/count 2>/dev/null || echo 0)
n=$((n+1))
echo $n > %[1]q/count
case $n in
%[2]s*)
  while :; do sleep 1; done ;;
esac
`, dir, cases.String())
	script = filepath.Join(dir, "python")
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return script, dir
}

func waitForHealth(t *testing.T, s *Supervisor, want func(Health) bool) Health {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h := s.Health(); want(h) {
			return h
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("health never reached the wanted state; last %+v", s.Health())
	return Health{}
}

func TestHealthCountsRestartsWithReasons(t *testing.T) {
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"sessions":[]}`))
	}))
	defer gw.Close()
	port := gw.Listener.Addr().(*net.TCPAddr).Port
	script, dir := scriptedChild(t, port, []string{"fail", "hang", "exit"})
	s, err := New(Options{
		Python: script, HermesHome: dir, Logger: slog.New(slog.DiscardHandler),
		ProbeInterval: 5 * time.Millisecond, ProbeFailures: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.restartBackoff = time.Millisecond
	// The second child answers its readiness probe, then stops answering, so
	// the watch ends it; every other probe succeeds.
	var secondChildProbed atomic.Bool
	s.probeFn = func(context.Context, int) error {
		raw, _ := os.ReadFile(filepath.Join(dir, "count"))
		if strings.TrimSpace(string(raw)) == "2" && secondChildProbed.Swap(true) {
			return errors.New("context deadline exceeded")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	defer func() { cancel(); s.Stop(); <-done }()

	if h := s.Health(); h.Restarts != 0 || h.LastRestartReason != "" {
		t.Fatalf("fresh supervisor reports restarts: %+v", h)
	}
	h := waitForHealth(t, s, func(h Health) bool { return h.Restarts == 1 })
	if h.LastRestartReason != RestartStartFailed {
		t.Fatalf("an exit before the readiness line is %q, want %q", h.LastRestartReason, RestartStartFailed)
	}

	if err := os.WriteFile(filepath.Join(dir, "go-2"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	h = waitForHealth(t, s, func(h Health) bool { return h.Restarts == 2 })
	if h.LastRestartReason != RestartUnresponsive {
		t.Fatalf("a child ended by the probe watch is %q, want %q", h.LastRestartReason, RestartUnresponsive)
	}

	if err := os.WriteFile(filepath.Join(dir, "go-3"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	h = waitForHealth(t, s, func(h Health) bool { return h.Restarts == 3 })
	if h.LastRestartReason != RestartExited {
		t.Fatalf("a ready child that exits is %q, want %q", h.LastRestartReason, RestartExited)
	}
}

func TestHealthRecordsLastSuccessfulProbe(t *testing.T) {
	s := newTestSupervisor(t)
	if !s.Health().LastProbeOK.IsZero() {
		t.Fatal("no probe has run yet")
	}
	fail := true
	s.probeFn = func(context.Context, int) error {
		if fail {
			return errors.New("slow")
		}
		return nil
	}
	ForceReadyForTest(s, "1")
	if err := s.ProbeNow(context.Background()); err == nil {
		t.Fatal("a failing probe reported success")
	}
	if !s.Health().LastProbeOK.IsZero() {
		t.Fatal("a failed probe was recorded as successful")
	}
	fail = false
	before := time.Now()
	if err := s.ProbeNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	first := s.Health().LastProbeOK
	if first.Before(before) || first.After(after) {
		t.Fatalf("last probe %v outside [%v, %v]", first, before, after)
	}

	exited := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(exited)
	}()
	s.watch(context.Background(), 1, exited)
	if !s.Health().LastProbeOK.After(first) {
		t.Fatal("successful watch probes did not advance the last successful probe")
	}
}

func TestProbeNowUsesCurrentPortAndToken(t *testing.T) {
	s, err := New(Options{Python: "/usr/bin/true", Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	var sawToken atomic.Value
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawToken.Store(r.Header.Get("X-Hermes-Session-Token"))
		if r.Header.Get("X-Hermes-Session-Token") != s.Token() {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"sessions":[]}`))
	}))
	defer good.Close()
	gated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer gated.Close()

	if err := s.ProbeNow(context.Background()); err == nil {
		t.Fatal("a supervisor that is not ready has no port to probe")
	}
	ForceReadyForTest(s, strconv.Itoa(good.Listener.Addr().(*net.TCPAddr).Port))
	if err := s.ProbeNow(context.Background()); err != nil {
		t.Fatalf("probe of the current child failed: %v", err)
	}
	if got, _ := sawToken.Load().(string); got != s.Token() {
		t.Fatalf("probe sent token %q", got)
	}
	ForceReadyForTest(s, strconv.Itoa(gated.Listener.Addr().(*net.TCPAddr).Port))
	if err := s.ProbeNow(context.Background()); err == nil {
		t.Fatal("probe ignored the current port and still reported the old child")
	}
}
