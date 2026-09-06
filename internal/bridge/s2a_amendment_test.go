package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/hermes-remote/internal/gateway"
	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

type pastDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c pastDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (pastDeadlineContext) Done() <-chan struct{}         { return make(chan struct{}) }
func (pastDeadlineContext) Err() error                    { return nil }

func TestAdmissionValidationRollsBackExpiredSuccess(t *testing.T) {
	active := context.Background()
	past := pastDeadlineContext{Context: active, deadline: time.Now().Add(-time.Second)}
	future, cancelFuture := context.WithDeadline(active, time.Now().Add(time.Minute))
	defer cancelFuture()

	for _, tc := range []struct {
		name    string
		parent  context.Context
		waitCtx context.Context
		want    admissionResult
	}{
		{"shared deadline", active, past, admissionBusy},
		{"parent deadline", past, future, admissionCanceled},
		{"active", active, future, admissionGranted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second := newWorkPool(1), newWorkPool(1)
			lease := &workLease{pools: []*workPool{first, second}}
			first.slots <- struct{}{}
			second.slots <- struct{}{}
			gotLease, got := validateAdmission(tc.parent, tc.waitCtx, lease)
			if got != tc.want {
				t.Fatalf("validation = %v, want %v", got, tc.want)
			}
			if tc.want == admissionGranted {
				if gotLease == nil || len(first.slots) != 1 || len(second.slots) != 1 {
					t.Fatal("active admission did not retain its exact reservations")
				}
				gotLease.release()
			} else if gotLease != nil || len(first.slots) != 0 || len(second.slots) != 0 {
				t.Fatal("expired admission did not roll back every reservation")
			}
		})
	}
}

func TestAcquirePoolsRefusesPastDeadlineEvenWhenSlotsAreReady(t *testing.T) {
	past := pastDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	first, second := newWorkPool(1), newWorkPool(1)
	lease, got := acquirePools(past, 0, first, second)
	if got != admissionCanceled || lease != nil {
		t.Fatalf("uncontended parent-past admission = (%v, %v)", lease, got)
	}
	if len(first.slots) != 0 || len(second.slots) != 0 {
		t.Fatal("uncontended parent-past admission leaked capacity")
	}

	ready := newWorkPool(1)
	waitCtx := pastDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	lease = &workLease{pools: make([]*workPool, 0, 1)}
	if ready.tryAcquire() {
		lease.pools = append(lease.pools, ready)
	}
	lease, got = validateAdmission(context.Background(), waitCtx, lease)
	if got != admissionBusy || lease != nil || len(ready.slots) != 0 {
		t.Fatalf("ready-slot expired shared deadline = (%v, %v), slots=%d", lease, got, len(ready.slots))
	}
}

func TestAcquirePoolsSharedDeadlineCannotBecomeLateSuccess(t *testing.T) {
	first, second := newWorkPool(1), newWorkPool(1)
	second.slots <- struct{}{}
	type outcome struct {
		lease *workLease
		state admissionResult
	}
	result := make(chan outcome, 1)
	go func() {
		lease, state := acquirePools(context.Background(), 20*time.Millisecond, first, second)
		result <- outcome{lease, state}
	}()
	deadline := time.Now().Add(time.Second)
	for len(first.slots) != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(first.slots) != 1 {
		t.Fatal("admission did not acquire the first pool before waiting")
	}
	<-time.After(30 * time.Millisecond)
	awaitS2TestValue(t, second.slots, "held second pool slot")
	got := awaitS2TestValue(t, result, "admission result")
	if got.state != admissionBusy || got.lease != nil {
		t.Fatalf("post-deadline release became admission success: %+v", got)
	}
	if len(first.slots) != 0 || len(second.slots) != 0 {
		t.Fatal("late admission failure did not roll back")
	}
}

func TestCanonicalPathGrammarAndRoundTrip(t *testing.T) {
	rejected := []string{
		"/api/sessions/raw space", "/api/sessions/raw-✓", "/api/sessions/[x]", "/api/sessions/{x}",
		"/api/sessions/x|y", "/api/sessions/x^y", "/api/sessions/\"x\"", "/api/sessions/<x>",
		"/api/sessions/%00x", "/api/sessions/%0Ax", "/api/sessions/%0Dx", "/api/sessions/%1Fx",
		"/api/sessions/%7Fx", "/api/sessions/x/%0A", "/api/sessions/x/%7F",
	}
	for _, raw := range rejected {
		if got, err := canonicalizePath(raw); err == nil {
			t.Errorf("canonicalizePath(%q) = %+v, want error", raw, got)
		}
	}

	accepted := []string{
		"/api/sessions/session-1", "/api/sessions/a!$&'()*+,;=:@b",
		"/api/sessions/session%20%E2%9C%93", "/api/sessions/a%3Fb%23c",
	}
	for _, raw := range accepted {
		canonical, err := canonicalizePath(raw)
		if err != nil {
			t.Errorf("canonicalizePath(%q): %v", raw, err)
			continue
		}
		u := &url.URL{Scheme: "http", Host: "127.0.0.1", Path: canonical.Path, RawPath: canonical.RawPath}
		request, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			t.Errorf("round-trip request %q: %v", raw, err)
			continue
		}
		if request.URL.RequestURI() != raw {
			t.Errorf("round-trip RequestURI = %q, want %q", request.URL.RequestURI(), raw)
		}
	}
}

func TestInvalidInitialPathsNeverReachUpstream(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	var calls atomic.Int32
	connection, link, suite := newProxyTestConnection(t, deps, roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}))
	for id, path := range []string{
		"/api/sessions/raw space", "/api/sessions/raw-✓", "/api/sessions/%0A", "/api/sessions/%7F",
	} {
		if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
			Ch: protocol.ChHTTP, ID: uint64(id + 1), Method: http.MethodGet, Path: path,
		}, nil); err != nil {
			t.Fatal(err)
		}
		if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusBadRequest {
			t.Fatalf("invalid path %q response = %+v", path, response)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid paths reached upstream %d times", calls.Load())
	}
}

func TestRedirectPolicyRejectsControlsCrossHostnameAndOverLimitWithoutToken(t *testing.T) {
	base, _ := url.Parse("http://127.0.0.1:4321")
	policy := redirectPolicy(base, "secret")
	for _, raw := range []string{
		"http://127.0.0.1:4321/api/sessions/%0A", "http://127.0.0.1:4321/api/sessions/%7F",
		"http://localhost:4321/api/sessions/ok",
	} {
		req, err := http.NewRequest(http.MethodGet, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Hermes-Session-Token", "leak")
		if err := policy(req, nil); err == nil {
			t.Errorf("redirect %q was accepted", raw)
		}
		if req.Header.Get("X-Hermes-Session-Token") != "" {
			t.Errorf("redirect %q retained the token", raw)
		}
	}

	allowed, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:4321/api/sessions/session%20%E2%9C%93?x=a%2Fb", nil)
	if err := policy(allowed, nil); err != nil {
		t.Fatalf("encoded Unicode redirect: %v", err)
	}
	if allowed.URL.EscapedPath() != "/api/sessions/session%20%E2%9C%93" || allowed.URL.RawQuery != "x=a%2Fb" {
		t.Fatalf("allowed redirect changed representation: %s", allowed.URL.RequestURI())
	}
	if allowed.Header.Get("X-Hermes-Session-Token") != "secret" {
		t.Fatal("allowed redirect did not regain the token")
	}

	over, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:4321/api/status", nil)
	over.Header.Set("X-Hermes-Session-Token", "leak")
	via := make([]*http.Request, 10)
	if err := policy(over, via); err == nil {
		t.Fatal("over-limit redirect was accepted")
	}
	if over.Header.Get("X-Hermes-Session-Token") != "" {
		t.Fatal("over-limit redirect retained the token")
	}
}

func TestProxyRedirectChainStopsAtTenRequestsAndRemainsUsable(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	var hits atomic.Int32
	var gatewayServer *httptest.Server
	gatewayServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/status" {
			t.Errorf("unexpected gateway route %s", r.URL.Path)
			return
		}
		if r.URL.Query().Get("case") == "ok" {
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		hop := hits.Add(1)
		http.Redirect(w, r, "/api/status?hop="+strconv.Itoa(int(hop)), http.StatusFound)
	}))
	defer gatewayServer.Close()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	connection, link, suite := newProxyTestConnectionForServer(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet, Path: "/api/status",
	}, nil); err != nil {
		t.Fatal(err)
	}
	response := readTestHTTPResponse(t, link, suite)
	if response.Status != http.StatusBadGateway || hits.Load() != 10 {
		t.Fatalf("redirect response=%+v requests=%d, want 502 and 10", response, hits.Load())
	}
	if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 2, Method: http.MethodGet, Path: "/api/status", Query: "case=ok",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if response = readTestHTTPResponse(t, link, suite); response.Status != http.StatusOK {
		t.Fatalf("request after redirect limit = %+v", response)
	}
}

type blockingWritePhoneLink struct {
	writes  chan []byte
	entered chan struct{}
	release chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingWritePhoneLink() *blockingWritePhoneLink {
	return &blockingWritePhoneLink{
		writes: make(chan []byte, 32), entered: make(chan struct{}, 32),
		release: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (l *blockingWritePhoneLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-l.closed:
		return 0, nil, errors.New("closed")
	}
}

func (l *blockingWritePhoneLink) Write(ctx context.Context, _ websocket.MessageType, data []byte) error {
	l.entered <- struct{}{}
	select {
	case <-l.release:
		l.writes <- bytes.Clone(data)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-l.closed:
		return errors.New("closed")
	}
}

func (l *blockingWritePhoneLink) Close(websocket.StatusCode, string) error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func newBlockingProxyConnection(t *testing.T, deps bridgeDependencies, transport http.RoundTripper) (*conn, *blockingWritePhoneLink, *protocol.Suite) {
	t.Helper()
	supervisor, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gateway.ForceReadyForTest(supervisor, "1")
	server := newServer(nil, nil, supervisor, slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
	server.httpClient = &http.Client{Transport: transport, Timeout: time.Second}
	link := newBlockingWritePhoneLink()
	bridgeSuite, _ := protocol.SuiteFromKeys(bytes.Repeat([]byte{11}, 32), bytes.Repeat([]byte{12}, 32))
	phoneSuite, _ := protocol.SuiteFromKeys(bytes.Repeat([]byte{12}, 32), bytes.Repeat([]byte{11}, 32))
	connection := server.newConnection(link, "blocking")
	connection.suite = bridgeSuite
	return connection, link, phoneSuite
}

func readBlockingHTTPResponse(t *testing.T, link *blockingWritePhoneLink, suite *protocol.Suite) protocol.HTTPResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case frame := <-link.writes:
		plain, err := suite.Open(frame)
		if err != nil {
			t.Fatal(err)
		}
		var response protocol.HTTPResponse
		if err := json.Unmarshal(plain, &response); err != nil {
			t.Fatal(err)
		}
		return response
	case <-ctx.Done():
		t.Fatal("timed out waiting for blocked HTTP response")
		return protocol.HTTPResponse{}
	}
}

func TestHTTPLeaseCoversCompletePhoneWrite(t *testing.T) {
	for _, tc := range []struct {
		name       string
		firstRound func() (*http.Response, error)
		wantStatus int
	}{
		{"success", func() (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
		}, http.StatusOK},
		{"error", func() (*http.Response, error) { return nil, errors.New("upstream failed") }, http.StatusBadGateway},
		{"oversize", func() (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(strings.Repeat("x", protocol.MaxPlaintext+1)))}, nil
		}, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := productionBridgeDependencies()
			deps.httpPerConnection, deps.httpProcessWide, deps.httpAdmissionWait = 1, 1, 0
			var calls atomic.Int32
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					return tc.firstRound()
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
			})
			connection, link, suite := newBlockingProxyConnection(t, deps, transport)
			second, secondLink, secondSuite := newProxyTestConnectionForServer(t, connection.srv)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			request := func(id uint64) protocol.HTTPRequest {
				return protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status"}
			}
			if err := connection.startProxyHTTP(ctx, request(1), nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-link.entered:
			case <-ctx.Done():
				t.Fatal("HTTP worker did not enter the phone write")
			}
			if len(connection.httpSlots.slots) != 1 || len(connection.srv.httpSlots.slots) != 1 {
				t.Fatal("HTTP lease was released before the phone write completed")
			}
			if lease, result := acquirePools(ctx, 0, connection.httpSlots, connection.srv.httpSlots); result != admissionBusy || lease != nil {
				t.Fatalf("production zero-wait acquire while response blocked = (%v, %v)", lease, result)
			}
			if err := second.startProxyHTTP(ctx, request(2), nil); err != nil {
				t.Fatal(err)
			}
			if busy := readTestHTTPResponse(t, secondLink, secondSuite); busy.Status != http.StatusTooManyRequests || busy.ID != 2 {
				t.Fatalf("cross-connection busy response = %+v", busy)
			}
			if calls.Load() != 1 {
				t.Fatalf("busy request reached upstream: calls=%d", calls.Load())
			}
			close(link.release)
			if response := readBlockingHTTPResponse(t, link, suite); response.Status != tc.wantStatus || response.ID != 1 {
				t.Fatalf("first response = %+v", response)
			}
			capacityCtx, capacityCancel := context.WithTimeout(ctx, time.Second)
			lease, result := acquirePools(capacityCtx, time.Second, connection.httpSlots, connection.srv.httpSlots)
			capacityCancel()
			if result != admissionGranted {
				t.Fatalf("capacity not restored after write: %v", result)
			}
			lease.release()
			if err := connection.startProxyHTTP(ctx, request(3), nil); err != nil {
				t.Fatal(err)
			}
			if response := readBlockingHTTPResponse(t, link, suite); response.Status != http.StatusOK || response.ID != 3 {
				t.Fatalf("request after release = %+v", response)
			}
		})
	}
}

type closeSignalBody struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func (b *closeSignalBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func TestHTTPLeaseCoversPreparedResponseWaitingForSendGate(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection, deps.httpProcessWide, deps.httpAdmissionWait = 1, 1, 0
	body := &closeSignalBody{Reader: strings.NewReader(`{"ok":true}`), closed: make(chan struct{})}
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}, nil
	})
	connection, link, suite := newProxyTestConnection(t, deps, transport)
	second, secondLink, secondSuite := newProxyTestConnectionForServer(t, connection.srv)
	connection.sendMu.Lock()
	locked := true
	defer func() {
		if locked {
			connection.sendMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := func(id uint64) protocol.HTTPRequest {
		return protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status"}
	}
	if err := connection.startProxyHTTP(ctx, request(1), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-body.closed:
	case <-ctx.Done():
		t.Fatal("upstream response did not complete")
	}
	if len(connection.httpSlots.slots) != 1 || len(connection.srv.httpSlots.slots) != 1 {
		t.Fatal("prepared response waiting for sendMu escaped HTTP capacity")
	}
	if err := second.startProxyHTTP(ctx, request(2), nil); err != nil {
		t.Fatal(err)
	}
	if response := readTestHTTPResponse(t, secondLink, secondSuite); response.Status != http.StatusTooManyRequests {
		t.Fatalf("second connection while sendMu blocked = %+v", response)
	}
	connection.sendMu.Unlock()
	locked = false
	if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusOK {
		t.Fatalf("held response after release = %+v", response)
	}
	capacityCtx, capacityCancel := context.WithTimeout(ctx, time.Second)
	lease, result := acquirePools(capacityCtx, time.Second, connection.httpSlots, connection.srv.httpSlots)
	capacityCancel()
	if result != admissionGranted {
		t.Fatalf("capacity after held sendMu = %v", result)
	}
	lease.release()
}

func TestClosedPhoneWriteReportsFailureAndReleasesLease(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection, deps.httpProcessWide, deps.httpAdmissionWait = 1, 1, 0
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})
	connection, link, _ := newBlockingProxyConnection(t, deps, transport)
	failures := make(chan error, 1)
	if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet, Path: "/api/status",
	}, func(err error) { failures <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-link.entered:
	case <-time.After(time.Second):
		t.Fatal("response did not reach the phone write")
	}
	_ = link.Close(websocket.StatusNormalClosure, "closed")
	select {
	case err := <-failures:
		if !strings.Contains(err.Error(), "closed") {
			t.Fatalf("closed phone failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed phone write did not invoke failure callback")
	}
	waitForEmptyPools(t, connection.httpSlots, connection.srv.httpSlots)
}

type contextPhoneLink struct {
	inbound chan relayMsg
	writes  chan []byte
	fail    atomic.Bool
	closed  chan struct{}
	once    sync.Once
}

func newContextPhoneLink() *contextPhoneLink {
	return &contextPhoneLink{inbound: make(chan relayMsg, 8), writes: make(chan []byte, 8), closed: make(chan struct{})}
}

func (l *contextPhoneLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case msg := <-l.inbound:
		return msg.typ, msg.data, nil
	case <-l.closed:
		return 0, nil, errors.New("closed")
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (l *contextPhoneLink) Write(ctx context.Context, _ websocket.MessageType, data []byte) error {
	if l.fail.Load() {
		return errors.New("forced phone write failure")
	}
	select {
	case l.writes <- bytes.Clone(data):
		return nil
	case <-l.closed:
		return errors.New("closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *contextPhoneLink) Close(websocket.StatusCode, string) error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func TestTunnelHTTPWriteFailureCancelsAndDrainsRunningLoops(t *testing.T) {
	accepted := make(chan struct{})
	httpReached := make(chan struct{})
	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			close(httpReached)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		if r.URL.Path != "/api/ws" {
			http.NotFound(w, r)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept gateway socket: %v", err)
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "")
		close(accepted)
		<-r.Context().Done()
	}))
	defer gatewayServer.Close()
	deps := productionBridgeDependencies()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	link := newContextPhoneLink()
	bridgeSuite, _ := protocol.SuiteFromKeys(bytes.Repeat([]byte{21}, 32), bytes.Repeat([]byte{22}, 32))
	phoneSuite, _ := protocol.SuiteFromKeys(bytes.Repeat([]byte{22}, 32), bytes.Repeat([]byte{21}, 32))
	connection := server.newConnection(link, "context-aware")
	connection.suite = bridgeSuite
	result := make(chan error, 1)
	go func() { result <- connection.tunnel(context.Background()) }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("gateway WebSocket was not accepted")
	}
	select {
	case <-link.writes: // initial gateway-ready control
	case <-time.After(time.Second):
		t.Fatal("initial gateway control was not written")
	}
	link.fail.Store(true)
	raw, _ := json.Marshal(protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 9, Method: http.MethodGet, Path: "/api/status",
	})
	frame, err := phoneSuite.Seal(raw)
	if err != nil {
		t.Fatal(err)
	}
	link.inbound <- relayMsg{typ: websocket.MessageBinary, data: frame}
	select {
	case <-httpReached:
	case <-time.After(time.Second):
		t.Fatal("tunnel HTTP request did not reach the live gateway endpoint")
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "forced phone write failure") {
			t.Fatalf("tunnel result = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP write failure did not cancel and drain the tunnel")
	}
	deadline := time.Now().Add(time.Second)
	for (len(connection.httpSlots.slots) != 0 || len(server.httpSlots.slots) != 0) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(connection.httpSlots.slots) != 0 || len(server.httpSlots.slots) != 0 {
		t.Fatal("failed response write retained HTTP capacity")
	}
}

type blockingDirEntry struct {
	nameEntered chan struct{}
	releaseName chan struct{}
	once        sync.Once
}

func (e *blockingDirEntry) Name() string {
	e.once.Do(func() { close(e.nameEntered) })
	<-e.releaseName
	return "blocked.txt"
}
func (*blockingDirEntry) IsDir() bool                { return false }
func (*blockingDirEntry) Type() os.FileMode          { return 0 }
func (*blockingDirEntry) Info() (os.FileInfo, error) { return nil, errors.New("unused") }

func waitForEmptyPools(t *testing.T, pools ...*workPool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		empty := true
		for _, pool := range pools {
			empty = empty && len(pool.slots) == 0
		}
		if empty {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("work pools did not become empty")
}

func TestFSResolverAndResultConstructionRetainOwnedCapacity(t *testing.T) {
	t.Run("resolver timeout and recovery", func(t *testing.T) {
		deps := productionBridgeDependencies()
		deps.fsListTimeout = 20 * time.Millisecond
		deps.fsWarningAfter = 5 * time.Millisecond
		resolverEntered := make(chan struct{})
		releaseResolver := make(chan struct{})
		var resolveCalls, readCalls atomic.Int32
		deps.fsResolvePath = func(string) (string, error) {
			resolveCalls.Add(1)
			close(resolverEntered)
			<-releaseResolver
			return "/resolved", nil
		}
		deps.fsReadDir = func(string) ([]os.DirEntry, error) {
			readCalls.Add(1)
			return nil, nil
		}
		first, second := newWorkPool(1), newWorkPool(1)
		lease, state := acquirePools(context.Background(), 0, first, second)
		if state != admissionGranted {
			t.Fatal(state)
		}
		handler := &warningCaptureHandler{messages: make(chan string, 2)}
		result := make(chan []byte, 1)
		go func() {
			_, body, _ := fsListWithDependencies(context.Background(), "path=/secret/path", deps, lease, slog.New(handler))
			result <- body
		}()
		awaitS2TestValue(t, resolverEntered, "resolver entry")
		listing := decodeListing(t, awaitS2TestValue(t, result, "filesystem result"))
		if listing.Error != "ETIMEDOUT" || readCalls.Load() != 0 {
			t.Fatalf("blocked resolver result=%+v readCalls=%d", listing, readCalls.Load())
		}
		if len(first.slots) != 1 || len(second.slots) != 1 {
			t.Fatal("blocked resolver released FS capacity")
		}
		if busyLease, busy := acquirePools(context.Background(), 0, first, second); busy != admissionBusy || busyLease != nil {
			t.Fatalf("blocked resolver second admission = (%v, %v)", busyLease, busy)
		}
		select {
		case message := <-handler.messages:
			if message != "filesystem listing still holds bridge capacity" || strings.Contains(message, "/secret/path") {
				t.Fatalf("watchdog message = %q", message)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked resolver emitted no watchdog warning")
		}
		close(releaseResolver)
		waitForEmptyPools(t, first, second)
		if resolveCalls.Load() != 1 || readCalls.Load() != 1 {
			t.Fatalf("worker calls resolve=%d read=%d", resolveCalls.Load(), readCalls.Load())
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		deps := productionBridgeDependencies()
		deps.fsListTimeout = time.Minute
		deps.fsWarningAfter = 0
		entered, release := make(chan struct{}), make(chan struct{})
		deps.fsResolvePath = func(string) (string, error) {
			close(entered)
			<-release
			return "/resolved", nil
		}
		first, second := newWorkPool(1), newWorkPool(1)
		lease, _ := acquirePools(context.Background(), 0, first, second)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, _, err := fsListWithDependencies(ctx, "path=/x", deps, lease, slog.Default())
			result <- err
		}()
		awaitS2TestValue(t, entered, "worker entry")
		cancel()
		if err := awaitS2TestValue(t, result, "canceled filesystem result"); !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation = %v", err)
		}
		if len(first.slots) != 1 || len(second.slots) != 1 {
			t.Fatal("canceled caller released blocked resolver capacity")
		}
		close(release)
		waitForEmptyPools(t, first, second)
	})

	t.Run("resolver error", func(t *testing.T) {
		deps := productionBridgeDependencies()
		deps.fsResolvePath = func(string) (string, error) { return "", errors.New("Invalid path") }
		first, second := newWorkPool(1), newWorkPool(1)
		lease, _ := acquirePools(context.Background(), 0, first, second)
		status, body, err := fsListWithDependencies(context.Background(), "path=/x", deps, lease, slog.Default())
		if err != nil || status != http.StatusBadRequest || decodeListing(t, body).Detail != "Invalid path" {
			t.Fatalf("resolver error = status %d body %s err %v", status, body, err)
		}
		waitForEmptyPools(t, first, second)
	})

	t.Run("entry construction timeout", func(t *testing.T) {
		deps := productionBridgeDependencies()
		deps.fsListTimeout = 20 * time.Millisecond
		deps.fsWarningAfter = 0
		entry := &blockingDirEntry{nameEntered: make(chan struct{}), releaseName: make(chan struct{})}
		deps.fsResolvePath = func(string) (string, error) { return "/resolved", nil }
		deps.fsReadDir = func(string) ([]os.DirEntry, error) { return []os.DirEntry{entry}, nil }
		first, second := newWorkPool(1), newWorkPool(1)
		lease, _ := acquirePools(context.Background(), 0, first, second)
		result := make(chan []byte, 1)
		go func() {
			_, body, _ := fsListWithDependencies(context.Background(), "path=/x", deps, lease, slog.Default())
			result <- body
		}()
		awaitS2TestValue(t, entry.nameEntered, "entry construction")
		if listing := decodeListing(t, awaitS2TestValue(t, result, "filesystem result")); listing.Error != "ETIMEDOUT" {
			t.Fatalf("blocked entry result = %+v", listing)
		}
		if len(first.slots) != 1 || len(second.slots) != 1 {
			t.Fatal("result construction did not retain FS capacity")
		}
		close(entry.releaseName)
		waitForEmptyPools(t, first, second)
	})
}

func TestBlockedFSResolverMakesNextListingBusyWithoutStartingResolver(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.fsPerConnection, deps.fsProcessWide = 1, 1
	deps.fsListTimeout, deps.fsWarningAfter = 20*time.Millisecond, 0
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	deps.fsResolvePath = func(string) (string, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return "/resolved", nil
	}
	deps.fsReadDir = func(string) ([]os.DirEntry, error) { return nil, nil }
	connection, link, suite := newProxyTestConnection(t, deps, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := func(id uint64) protocol.HTTPRequest {
		return protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/fs/list", Query: "path=/x"}
	}
	if err := connection.startProxyHTTP(ctx, request(1), nil); err != nil {
		t.Fatal(err)
	}
	awaitS2TestValue(t, entered, "worker entry")
	if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusOK || !strings.Contains(string(response.Body), "ETIMEDOUT") {
		t.Fatalf("blocked resolver response = %+v", response)
	}
	if err := connection.startProxyHTTP(ctx, request(2), nil); err != nil {
		t.Fatal(err)
	}
	if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusOK || !strings.Contains(string(response.Body), "EBUSY") {
		t.Fatalf("second listing response = %+v", response)
	}
	if calls.Load() != 1 {
		t.Fatalf("busy listing started resolver: calls=%d", calls.Load())
	}
	close(release)
	waitForEmptyPools(t, connection.fsSlots, connection.srv.fsSlots)
}

func TestFSQueryParseErrorReleasesLeaseWithoutWorker(t *testing.T) {
	deps := productionBridgeDependencies()
	var resolves atomic.Int32
	deps.fsResolvePath = func(string) (string, error) {
		resolves.Add(1)
		return "/unused", nil
	}
	first, second := newWorkPool(1), newWorkPool(1)
	lease, _ := acquirePools(context.Background(), 0, first, second)
	status, _, err := fsListWithDependencies(context.Background(), "%zz", deps, lease, slog.Default())
	if err != nil || status != http.StatusBadRequest || resolves.Load() != 0 {
		t.Fatalf("query parse result status=%d err=%v resolves=%d", status, err, resolves.Load())
	}
	waitForEmptyPools(t, first, second)
}

func TestRelayWriteGateHonorsCancellationAndRecovers(t *testing.T) {
	mux := newRelayMux(nil)
	awaitS2TestValue(t, mux.writeGate, "relay write token")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- mux.write(ctx, []byte("blocked")) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked write cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued relay write ignored cancellation")
	}
	mux.writeGate <- struct{}{}

	preCanceled, cancelPre := context.WithCancel(context.Background())
	cancelPre()
	if err := mux.write(preCanceled, []byte("pre-canceled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled relay write = %v", err)
	}
	if len(mux.writeGate) != 1 {
		t.Fatal("pre-canceled relay write stranded the gate token")
	}
}

func TestRelayWriteGateSerializesRealWebSocketAfterCanceledWaiter(t *testing.T) {
	received := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "")
		for i := 0; i < 3; i++ {
			_, data, err := ws.Read(r.Context())
			if err != nil {
				return
			}
			received <- string(data)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close(websocket.StatusNormalClosure, "")
	mux := newRelayMux(ws)
	awaitS2TestValue(t, mux.writeGate, "relay write token")
	canceled, cancelWaiter := context.WithCancel(ctx)
	waiter := make(chan error, 1)
	go func() { waiter <- mux.write(canceled, []byte("canceled")) }()
	cancelWaiter()
	if err := awaitS2TestValue(t, waiter, "relay canceled waiter"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled real-socket waiter = %v", err)
	}
	mux.writeGate <- struct{}{}

	errs := make(chan error, 2)
	go func() { errs <- mux.write(ctx, []byte("one")) }()
	go func() { errs <- mux.write(ctx, []byte("two")) }()
	for i := 0; i < 2; i++ {
		if err := awaitS2TestValue(t, errs, "relay write result"); err != nil {
			t.Fatal(err)
		}
	}
	if err := mux.write(ctx, []byte("after")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case value := <-received:
			seen[value] = true
		case <-ctx.Done():
			t.Fatal("timed out reading serialized relay frames")
		}
	}
	for _, want := range []string{"one", "two", "after"} {
		if !seen[want] {
			t.Fatalf("serialized relay frames = %v, missing %q", seen, want)
		}
	}
}
