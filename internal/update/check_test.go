package update

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var checkEpoch = time.Date(2026, 9, 28, 9, 0, 0, 0, time.UTC)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeSource answers Latest from its fields; with gate set, each call waits
// for the gate (or its context) first.
type fakeSource struct {
	mu      sync.Mutex
	calls   int
	release Release
	err     error
	gate    chan struct{}
	started chan context.Context
}

func (s *fakeSource) Latest(ctx context.Context) (Release, error) {
	s.mu.Lock()
	s.calls++
	gate, started := s.gate, s.started
	release, err := s.release, s.err
	s.mu.Unlock()
	if started != nil {
		started <- ctx
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return Release{}, ctx.Err()
		}
	}
	return release, err
}

func (s *fakeSource) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeSource) Set(release Release, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.release, s.err = release, err
}

func release(tag string) Release {
	return Release{Tag: tag, PublishedAt: checkEpoch.Add(-time.Hour), Body: "* abc1234: Serve bridge status to paired phones (A <a@b>)"}
}

func newTestChecker(source Source, current string) (*Checker, *testClock) {
	clock := &testClock{now: checkEpoch}
	checker := NewChecker(source, current)
	checker.now = clock.Now
	return checker, clock
}

var manual = Install{Method: MethodManual, Path: "/Users/x/.local/bin/hermote-bridge", UpdateCommand: ManualUpdateCommand("/Users/x/.local/bin")}

func TestLatestReleaseParsedAndSubjectsStripped(t *testing.T) {
	var body strings.Builder
	body.WriteString("## Changelog\n")
	body.WriteString("* d4150acc9b74c49806d3b65824329a76f1145586: Serve bridge status to paired phones (Shravanth Reddy <a@b.c>)\n")
	body.WriteString("* 99533ac: Bridge: allow DELETE /api/messaging/ (plan 10 / WP9) (Shravanth Reddy <a@b.c>)\n")
	body.WriteString("- 4d9fed5d: Check for bridge releases on the Mac\n")
	for i := 0; i < 57; i++ {
		fmt.Fprintf(&body, "* %07x: Change %d (A <a@b>)\n", i+1, i)
	}
	body.WriteString("\nInstall the bridge:\n\n    curl -fsSL https://raw.githubusercontent.com/ShravanthReddy/Hermote-bridge/main/install.sh | bash\n\n")
	source, _ := tlsSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != latestReleasePath {
			http.NotFound(w, r)
			return
		}
		payload, _ := json.Marshal(map[string]any{
			"tag_name": "v0.16.0", "published_at": "2026-09-28T08:00:00Z", "body": body.String(),
			"html_url": "https://github.com/ShravanthReddy/Hermote-bridge/releases/tag/v0.16.0",
		})
		_, _ = w.Write(payload)
	})
	checker, _ := newTestChecker(source, "0.15.0")
	got := checker.Check(context.Background(), false).Response(manual)
	if got.Error != nil || got.Latest == nil || *got.Latest != "0.16.0" || got.Release == nil {
		t.Fatalf("check %+v", got)
	}
	if got.Release.Tag != "v0.16.0" || !got.Release.PublishedAt.Equal(checkEpoch.Add(-time.Hour)) {
		t.Fatalf("release %+v", got.Release)
	}
	want := []string{
		"Serve bridge status to paired phones",
		"Bridge: allow DELETE /api/messaging/ (plan 10 / WP9)",
		"Check for bridge releases on the Mac",
		"Change 0",
	}
	for i, subject := range want {
		if got.Release.Notes[i] != subject {
			t.Fatalf("note %d = %q, want %q", i, got.Release.Notes[i], subject)
		}
	}
	if len(got.Release.Notes) != 50 || got.Release.More != 10 {
		t.Fatalf("%d notes and %d more, want 50 and 10", len(got.Release.Notes), got.Release.More)
	}
}

func TestCheckCachedForADay(t *testing.T) {
	source := &fakeSource{release: release("v0.16.0")}
	checker, clock := newTestChecker(source, "0.15.0")
	first := checker.Check(context.Background(), false)
	clock.Advance(23 * time.Hour)
	cached := checker.Check(context.Background(), false)
	if source.Calls() != 1 || !cached.CheckedAt.Equal(first.CheckedAt) {
		t.Fatalf("a check within a day reached the network: %d calls", source.Calls())
	}
	clock.Advance(time.Hour + time.Second)
	fresh := checker.Check(context.Background(), false)
	if source.Calls() != 2 || !fresh.CheckedAt.Equal(clock.Now()) {
		t.Fatalf("a day-old check was not refreshed: %d calls", source.Calls())
	}
}

func TestForcedCheckReachesNetworkAtMostEveryTwoMinutes(t *testing.T) {
	source := &fakeSource{release: release("v0.16.0")}
	checker, clock := newTestChecker(source, "0.15.0")
	checker.Check(context.Background(), true)
	clock.Advance(time.Minute + 59*time.Second)
	checker.Check(context.Background(), true)
	if source.Calls() != 1 {
		t.Fatalf("a forced check %v after the last reached the network", time.Minute+59*time.Second)
	}
	clock.Advance(time.Second)
	checker.Check(context.Background(), true)
	if source.Calls() != 2 {
		t.Fatalf("a forced check two minutes after the last did not reach the network")
	}
}

func TestConcurrentChecksJoinOneRequest(t *testing.T) {
	source := &fakeSource{release: release("v0.16.0"), gate: make(chan struct{}), started: make(chan context.Context, 8)}
	checker, _ := newTestChecker(source, "0.15.0")
	results := make(chan Result, 5)
	for i := 0; i < 5; i++ {
		go func(force bool) { results <- checker.Check(context.Background(), force) }(i%2 == 0)
	}
	<-source.started
	time.Sleep(20 * time.Millisecond)
	close(source.gate)
	for i := 0; i < 5; i++ {
		if got := <-results; got.Latest == nil || got.Latest.Version != "0.16.0" {
			t.Fatalf("a joined check got %+v", got)
		}
	}
	if source.Calls() != 1 {
		t.Fatalf("%d concurrent checks made %d requests", 5, source.Calls())
	}
}

func TestCancelledJoinerDoesNotCancelSharedCheck(t *testing.T) {
	source := &fakeSource{release: release("v0.16.0"), gate: make(chan struct{}), started: make(chan context.Context, 1)}
	checker, _ := newTestChecker(source, "0.15.0")
	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan Result, 1)
	go func() { left <- checker.Check(ctx, false) }()
	shared := <-source.started
	cancel()
	if got := <-left; got.Latest != nil {
		t.Fatalf("the caller that left saw a result it did not wait for: %+v", got)
	}
	if shared.Err() != nil {
		t.Fatalf("the caller that left cancelled the shared request: %v", shared.Err())
	}
	stayed := make(chan Result, 1)
	go func() { stayed <- checker.Check(context.Background(), false) }()
	time.Sleep(20 * time.Millisecond)
	close(source.gate)
	if got := <-stayed; got.Latest == nil || source.Calls() != 1 {
		t.Fatalf("the remaining caller got %+v after %d requests", got, source.Calls())
	}
}

func TestSlowBodyHitsTotalDeadline(t *testing.T) {
	source, _ := tlsSource(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tag_name":"v0.16.0","body":"`))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	checker, _ := newTestChecker(source, "0.15.0")
	checker.timeout = 200 * time.Millisecond
	started := time.Now()
	got := checker.Check(context.Background(), false)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("a stalled body held the check for %v", elapsed)
	}
	if got.Err == nil || got.Err.Kind != ErrorTimeout || got.Err.Message != "api.github.com did not answer within 20 seconds." {
		t.Fatalf("a stalled body gave %+v", got.Err)
	}
}

func TestRateLimitWaitsForReset(t *testing.T) {
	clock := &testClock{now: checkEpoch}
	var calls atomic.Int32
	var reset atomic.Int64
	reset.Store(checkEpoch.Add(10 * time.Minute).Unix())
	source, _ := tlsSource(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Load(), 10))
		w.WriteHeader(http.StatusForbidden)
	})
	checker := NewChecker(source, "0.15.0")
	checker.now = clock.Now
	got := checker.Check(context.Background(), false)
	if got.Err == nil || got.Err.Kind != ErrorRateLimited || !strings.Contains(got.Err.Message, "rate limit") {
		t.Fatalf("a spent rate limit gave %+v", got.Err)
	}
	clock.Advance(3 * time.Minute)
	if got := checker.Check(context.Background(), true); calls.Load() != 1 || got.Err == nil || got.Err.Kind != ErrorRateLimited {
		t.Fatalf("a check before the reset reached GitHub (%d requests): %+v", calls.Load(), got.Err)
	}
	clock.Advance(7*time.Minute + time.Second)
	reset.Store(clock.Now().Add(5 * time.Hour).Unix())
	checker.Check(context.Background(), true)
	if calls.Load() != 2 {
		t.Fatalf("a check after the reset did not reach GitHub")
	}
	clock.Advance(59 * time.Minute)
	checker.Check(context.Background(), true)
	if calls.Load() != 2 {
		t.Fatal("a far reset was not bounded to an hour: checked before it")
	}
	clock.Advance(time.Minute + time.Second)
	checker.Check(context.Background(), true)
	if calls.Load() != 3 {
		t.Fatal("the wait for a far reset lasted longer than an hour")
	}
}

func TestFailedCheckKeepsLastResultWithCause(t *testing.T) {
	source := &fakeSource{release: release("v0.16.0")}
	checker, clock := newTestChecker(source, "0.15.0")
	good := checker.Check(context.Background(), false)
	clock.Advance(25 * time.Hour)
	source.Set(Release{}, &SourceError{Kind: ErrorNetwork, Message: "DNS lookup for api.github.com failed — check your connection or proxy."})
	got := checker.Check(context.Background(), false)
	if got.Latest == nil || got.Latest.Version != "0.16.0" || !got.CheckedAt.Equal(good.CheckedAt) {
		t.Fatalf("a failed check dropped the last good result: %+v", got)
	}
	if got.Err == nil || got.Err.Kind != ErrorNetwork {
		t.Fatalf("a failed check hid its cause: %+v", got.Err)
	}
	response := got.Response(manual)
	if response.Latest == nil || response.Error == nil || response.UpdateAvailable == nil || !*response.UpdateAvailable {
		t.Fatalf("response %+v", response)
	}
}

func TestOlderOrEqualLatestIsNotAnUpdate(t *testing.T) {
	cases := []struct {
		latest string
		want   bool
	}{{"v0.15.0", false}, {"v0.14.9", false}, {"v0.15.1", true}, {"v1.0.0", true}, {"v0.15.10", true}}
	for _, tc := range cases {
		checker, _ := newTestChecker(&fakeSource{release: release(tc.latest)}, "0.15.0")
		got := checker.Check(context.Background(), false).Response(manual)
		if got.UpdateAvailable == nil || *got.UpdateAvailable != tc.want {
			t.Fatalf("latest %s on 0.15.0: update_available %v, want %v", tc.latest, got.UpdateAvailable, tc.want)
		}
	}
	checker, _ := newTestChecker(&fakeSource{release: release("v0.16.0-rc1")}, "0.15.0")
	if got := checker.Check(context.Background(), false); got.Err == nil || got.Err.Kind != ErrorMalformed || got.Latest != nil {
		t.Fatalf("a non-release tag was accepted: %+v", got)
	}
}

func TestReleaseLinkBuiltLocally(t *testing.T) {
	source, _ := tlsSource(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"tag_name":"v0.16.0","published_at":"2026-09-28T08:00:00Z","body":"","html_url":"https://evil.example/x"}`))
	})
	checker, _ := newTestChecker(source, "0.15.0")
	got := checker.Check(context.Background(), false).Response(manual)
	if got.Release == nil || got.Release.URL != "https://github.com/ShravanthReddy/Hermote-bridge/releases/tag/v0.16.0" {
		t.Fatalf("release link %+v", got.Release)
	}
	if got.Release.Notes == nil {
		t.Fatal("an empty changelog must encode as [] not null")
	}
}
