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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type capturePhoneLink struct {
	writes   chan []byte
	writeErr error
	closed   chan struct{}
	once     sync.Once
}

func newCapturePhoneLink() *capturePhoneLink {
	return &capturePhoneLink{writes: make(chan []byte, 32), closed: make(chan struct{})}
}

func (l *capturePhoneLink) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-l.closed:
		return 0, nil, errors.New("closed")
	}
}

func (l *capturePhoneLink) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	if l.writeErr != nil {
		return l.writeErr
	}
	l.writes <- bytes.Clone(data)
	return nil
}

func (l *capturePhoneLink) Close(websocket.StatusCode, string) error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func newProxyTestConnection(
	t *testing.T, deps bridgeDependencies, transport http.RoundTripper,
) (*conn, *capturePhoneLink, *protocol.Suite) {
	t.Helper()
	supervisor, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	gateway.ForceReadyForTest(supervisor, "1")
	server := newServer(nil, nil, supervisor, slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
	if transport != nil {
		server.httpClient = &http.Client{Transport: transport, Timeout: time.Second}
	}
	link := newCapturePhoneLink()
	bridgeSuite, err := protocol.SuiteFromKeys(bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	phoneSuite, err := protocol.SuiteFromKeys(bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	connection := server.newConnection(link, "test")
	connection.suite = bridgeSuite
	return connection, link, phoneSuite
}

func newProxyTestConnectionForServer(
	t *testing.T, server *Server,
) (*conn, *capturePhoneLink, *protocol.Suite) {
	t.Helper()
	link := newCapturePhoneLink()
	bridgeSuite, err := protocol.SuiteFromKeys(bytes.Repeat([]byte{3}, 32), bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	phoneSuite, err := protocol.SuiteFromKeys(bytes.Repeat([]byte{4}, 32), bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	connection := server.newConnection(link, "test")
	connection.suite = bridgeSuite
	return connection, link, phoneSuite
}

func newProxyTestServerAtGateway(
	t *testing.T, deps bridgeDependencies, gatewayServer *httptest.Server,
) *Server {
	t.Helper()
	supervisor, err := gateway.New(gateway.Options{Python: "/usr/bin/true", HermesHome: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(gatewayServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	gateway.ForceReadyForTest(supervisor, parsed.Port())
	return newServer(nil, nil, supervisor, slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
}

func readTestPlain(t *testing.T, link *capturePhoneLink, suite *protocol.Suite) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var assembler protocol.ChunkAssembler
	for {
		var frame []byte
		select {
		case frame = <-link.writes:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for bridge write: %v", ctx.Err())
		}
		plain, err := suite.Open(frame)
		if err != nil {
			t.Fatal(err)
		}
		channel, err := protocol.PeekChannel(plain)
		if err != nil {
			t.Fatal(err)
		}
		if channel != protocol.ChChunk {
			return plain
		}
		var chunk protocol.Chunk
		if err := json.Unmarshal(plain, &chunk); err != nil {
			t.Fatal(err)
		}
		whole, err := assembler.Add(chunk)
		if err != nil {
			t.Fatal(err)
		}
		if whole != nil {
			return whole
		}
	}
}

func readTestHTTPResponse(t *testing.T, link *capturePhoneLink, suite *protocol.Suite) protocol.HTTPResponse {
	t.Helper()
	plain := readTestPlain(t, link, suite)
	var response protocol.HTTPResponse
	if err := json.Unmarshal(plain, &response); err != nil {
		t.Fatalf("decode HTTP response %q: %v", plain, err)
	}
	return response
}

func TestHTTPResponsePlaintextBoundaries(t *testing.T) {
	responseAtSize := func(size int) protocol.HTTPResponse {
		t.Helper()
		base := protocol.HTTPResponse{
			Ch: protocol.ChHTTP, ID: 41, Status: http.StatusOK, Body: json.RawMessage(`""`),
		}
		baseRaw, err := json.Marshal(base)
		if err != nil {
			t.Fatal(err)
		}
		payload := size - len(baseRaw)
		if payload < 0 {
			t.Fatalf("target size %d below envelope overhead %d", size, len(baseRaw))
		}
		base.Body = json.RawMessage(strconv.Quote(strings.Repeat("x", payload)))
		return base
	}
	for _, size := range []int{protocol.MaxPlaintext - 1, protocol.MaxPlaintext} {
		raw, err := marshalHTTPResponse(responseAtSize(size))
		if err != nil || len(raw) != size {
			t.Fatalf("size %d: len=%d err=%v", size, len(raw), err)
		}
	}
	_, err := marshalHTTPResponse(responseAtSize(protocol.MaxPlaintext + 1))
	var limitErr plaintextLimitError
	if !errors.As(err, &limitErr) || limitErr.size != protocol.MaxPlaintext+1 {
		t.Fatalf("oversize error = %#v", err)
	}
}

func TestHTTPResponseOverflowBecomesSmallCorrelated502(t *testing.T) {
	body := json.RawMessage(strconv.Quote(strings.Repeat("x", protocol.MaxPlaintext)))
	raw, err := prepareHTTPResponse(73, http.StatusOK, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) >= protocol.MaxPlaintext {
		t.Fatalf("fallback response remains oversized: %d", len(raw))
	}
	var response protocol.HTTPResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != 73 || response.Status != http.StatusBadGateway ||
		!strings.Contains(string(response.Body), gatewayResponseLimitDetail) {
		t.Fatalf("fallback response = %+v body=%s", response, response.Body)
	}
}

func TestPreparedHTTPResponseBytesAreSentUnchanged(t *testing.T) {
	deps := productionBridgeDependencies()
	connection, link, suite := newProxyTestConnection(t, deps, nil)
	want, err := prepareHTTPResponse(74, http.StatusCreated, json.RawMessage(`{"created":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.writeHTTPResponse(context.Background(), want, nil); err != nil {
		t.Fatal(err)
	}
	if got := readTestPlain(t, link, suite); !bytes.Equal(got, want) {
		t.Fatalf("sent plaintext differs from prepared bytes\n got: %s\nwant: %s", got, want)
	}
}

func TestSendRejectsOversizePlaintextBeforeWriting(t *testing.T) {
	deps := productionBridgeDependencies()
	connection, link, _ := newProxyTestConnection(t, deps, nil)
	err := connection.send(context.Background(), make([]byte, protocol.MaxPlaintext+1))
	if !errors.Is(err, errPlaintextLimit) {
		t.Fatalf("send error = %v", err)
	}
	if len(link.writes) != 0 {
		t.Fatalf("oversize send emitted %d frames", len(link.writes))
	}
}

func TestHTTPValidationPrecedesCapacityAndSeparatesMalformedFromRefused(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection = 1
	deps.httpProcessWide = 1
	deps.httpAdmissionWait = 0
	connection, link, suite := newProxyTestConnection(t, deps, nil)
	connection.httpSlots.slots <- struct{}{}
	connection.srv.httpSlots.slots <- struct{}{}
	defer func() {
		<-connection.httpSlots.slots
		<-connection.srv.httpSlots.slots
	}()

	if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet, Path: "/api/fs/%6cist",
	}, nil); err != nil {
		t.Fatal(err)
	}
	malformed := readTestHTTPResponse(t, link, suite)
	if malformed.Status != http.StatusBadRequest || string(malformed.Body) == RefusedRouteError {
		t.Fatalf("malformed response = %+v body=%s", malformed, malformed.Body)
	}
	if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 2, Method: http.MethodGet, Path: "/api/secret",
	}, nil); err != nil {
		t.Fatal(err)
	}
	refused := readTestHTTPResponse(t, link, suite)
	if refused.Status != http.StatusForbidden || string(refused.Body) != RefusedRouteError {
		t.Fatalf("refused response = %+v body=%s", refused, refused.Body)
	}
	if len(connection.httpSlots.slots) != 1 || len(connection.srv.httpSlots.slots) != 1 {
		t.Fatal("validation consumed HTTP capacity")
	}
}

func TestHTTPZeroWaitCeilingAndEventualReleaseAfterReply(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection = 1
	deps.httpProcessWide = 1
	deps.httpAdmissionWait = 0
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})
	connection, link, suite := newProxyTestConnection(t, deps, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := func(id uint64) protocol.HTTPRequest {
		return protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status"}
	}
	if err := connection.startProxyHTTP(ctx, request(1), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := connection.startProxyHTTP(ctx, request(2), nil); err != nil {
		t.Fatal(err)
	}
	busy := readTestHTTPResponse(t, link, suite)
	if busy.ID != 2 || busy.Status != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("busy response=%+v upstream calls=%d", busy, calls.Load())
	}
	close(release)
	first := readTestHTTPResponse(t, link, suite)
	if first.ID != 1 || first.Status != http.StatusOK {
		t.Fatalf("first response = %+v", first)
	}
	capacityCtx, capacityCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	lease, result := acquirePools(capacityCtx, 500*time.Millisecond, connection.httpSlots, connection.srv.httpSlots)
	capacityCancel()
	if result != admissionGranted {
		t.Fatalf("HTTP capacity was not restored after response write: %v", result)
	}
	lease.release()
	if err := connection.startProxyHTTP(ctx, request(3), nil); err != nil {
		t.Fatal(err)
	}
	third := readTestHTTPResponse(t, link, suite)
	if third.ID != 3 || third.Status != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("third response=%+v upstream calls=%d", third, calls.Load())
	}
}

func TestHTTPProcessWideCeilingSpansConnections(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection = 2
	deps.httpProcessWide = 1
	deps.httpAdmissionWait = 0
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})
	first, firstLink, firstSuite := newProxyTestConnection(t, deps, transport)
	second, secondLink, secondSuite := newProxyTestConnectionForServer(t, first.srv)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := func(id uint64) protocol.HTTPRequest {
		return protocol.HTTPRequest{Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status"}
	}
	if err := first.startProxyHTTP(ctx, request(1), nil); err != nil {
		t.Fatal(err)
	}
	awaitS2TestValue(t, entered, "worker entry")
	if err := second.startProxyHTTP(ctx, request(2), nil); err != nil {
		t.Fatal(err)
	}
	if response := readTestHTTPResponse(t, secondLink, secondSuite); response.Status != http.StatusTooManyRequests {
		t.Fatalf("second connection above process ceiling = %+v", response)
	}
	if calls.Load() != 1 || len(second.httpSlots.slots) != 0 {
		t.Fatalf("busy request reached upstream or retained local capacity: calls=%d local=%d", calls.Load(), len(second.httpSlots.slots))
	}
	close(release)
	if response := readTestHTTPResponse(t, firstLink, firstSuite); response.Status != http.StatusOK {
		t.Fatalf("first response = %+v", response)
	}
	// The phone observes the response before the worker's deferred release.
	// Await actual capacity restoration before exercising zero-wait admission.
	capacityCtx, capacityCancel := context.WithTimeout(ctx, time.Second)
	lease, result := acquirePools(capacityCtx, time.Second, first.httpSlots, first.srv.httpSlots)
	capacityCancel()
	if result != admissionGranted {
		t.Fatalf("capacity not restored after first response: %v", result)
	}
	lease.release()
	if err := second.startProxyHTTP(ctx, request(3), nil); err != nil {
		t.Fatal(err)
	}
	awaitS2TestValue(t, entered, "worker entry")
	if response := readTestHTTPResponse(t, secondLink, secondSuite); response.Status != http.StatusOK {
		t.Fatalf("second connection after release = %+v", response)
	}
	if calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want 2", calls.Load())
	}
}

func TestHTTPAdmissionWaitSharesDeadlineAndHonorsCancellation(t *testing.T) {
	first := newWorkPool(1)
	second := newWorkPool(1)
	first.slots <- struct{}{}
	type result struct {
		lease *workLease
		state admissionResult
	}
	resultCh := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		lease, state := acquirePools(ctx, 500*time.Millisecond, first, second)
		resultCh <- result{lease, state}
	}()
	awaitS2TestValue(t, first.slots, "held pool slot")
	got := awaitS2TestValue(t, resultCh, "admission result")
	if got.state != admissionGranted {
		t.Fatalf("released waiter state = %v", got.state)
	}
	got.lease.release()

	first.slots <- struct{}{}
	canceled, cancelWaiter := context.WithCancel(context.Background())
	cancelResult := make(chan admissionResult, 1)
	go func() {
		_, state := acquirePools(canceled, time.Second, first, second)
		cancelResult <- state
	}()
	cancelWaiter()
	if state := awaitS2TestValue(t, cancelResult, "canceled admission result"); state != admissionCanceled {
		t.Fatalf("canceled waiter state = %v", state)
	}
	if len(first.slots) != 1 || len(second.slots) != 0 {
		t.Fatal("canceled waiter leaked capacity")
	}
	awaitS2TestValue(t, first.slots, "held pool slot")
}

func TestHTTPResponseWriteFailureReportsTunnelFailure(t *testing.T) {
	deps := productionBridgeDependencies()
	connection, link, _ := newProxyTestConnection(t, deps, nil)
	link.writeErr = errors.New("forced write failure")
	failed := make(chan error, 1)
	raw, err := prepareHTTPResponse(8, http.StatusOK, json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.writeHTTPResponse(context.Background(), raw, func(err error) { failed <- err }); err == nil {
		t.Fatal("forced response write succeeded")
	}
	select {
	case err := <-failed:
		if !strings.Contains(err.Error(), "forced write failure") {
			t.Fatalf("reported failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("response write failure was not reported")
	}
}

type timeoutNetworkError struct{}

func (timeoutNetworkError) Error() string   { return "timed out" }
func (timeoutNetworkError) Timeout() bool   { return true }
func (timeoutNetworkError) Temporary() bool { return true }

type timeoutBody struct {
	closed atomic.Bool
}

func (*timeoutBody) Read([]byte) (int, error) { return 0, timeoutNetworkError{} }
func (b *timeoutBody) Close() error {
	b.closed.Store(true)
	return nil
}

type closeCountingBody struct {
	io.ReadCloser
	closes *atomic.Int32
}

func (b *closeCountingBody) Close() error {
	b.closes.Add(1)
	return b.ReadCloser.Close()
}

type warningCaptureHandler struct {
	messages chan string
}

func (h *warningCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *warningCaptureHandler) Handle(_ context.Context, record slog.Record) error {
	h.messages <- record.Message
	return nil
}
func (h *warningCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warningCaptureHandler) WithGroup(string) slog.Handler      { return h }

func TestHTTPBodyReadTimeoutMapsTo504ClosesBodyAndKeepsConnectionUsable(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	body := &timeoutBody{}
	var calls atomic.Int32
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}, nil
	})
	connection, link, suite := newProxyTestConnection(t, deps, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for id := uint64(1); id <= 2; id++ {
		if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
			Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status",
		}, nil); err != nil {
			t.Fatal(err)
		}
		response := readTestHTTPResponse(t, link, suite)
		want := http.StatusGatewayTimeout
		if id == 2 {
			want = http.StatusOK
		}
		if response.ID != id || response.Status != want {
			t.Fatalf("response %d = %+v", id, response)
		}
	}
	if !body.closed.Load() {
		t.Fatal("timed-out response body was not closed")
	}
}

func TestProxyHTTPDetectsRawAndQuotedOverflowThenContinues(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	var calls atomic.Int32
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		var body string
		switch calls.Add(1) {
		case 1:
			body = `{"value":"` + strings.Repeat("x", protocol.MaxPlaintext) + `"}`
		case 2:
			// This body fits under the raw read cap, but JSON quoting plus the
			// correlated response envelope exceeds the plaintext limit.
			body = strings.Repeat("x", protocol.MaxPlaintext-8)
		default:
			body = `{"ok":true}`
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)),
		}, nil
	})
	connection, link, suite := newProxyTestConnection(t, deps, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for id := uint64(1); id <= 3; id++ {
		if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
			Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/status",
		}, nil); err != nil {
			t.Fatal(err)
		}
		response := readTestHTTPResponse(t, link, suite)
		if id < 3 {
			if response.Status != http.StatusBadGateway ||
				!strings.Contains(string(response.Body), gatewayResponseLimitDetail) {
				t.Fatalf("overflow response %d = %+v body=%s", id, response, response.Body)
			}
		} else if response.Status != http.StatusOK || !strings.Contains(string(response.Body), `"ok":true`) {
			t.Fatalf("small response after overflow = %+v body=%s", response, response.Body)
		}
	}
}

func TestProxyPreservesEscapedPathQueryAndConstrainsRedirects(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	var targetHits atomic.Int32
	var refusedHits atomic.Int32
	var externalHits atomic.Int32
	received := make(chan string, 4)
	external := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		externalHits.Add(1)
	}))
	defer external.Close()
	var gatewayServer *httptest.Server
	gatewayServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/status":
			switch r.URL.Query().Get("case") {
			case "allowed":
				http.Redirect(w, r, "/api/sessions?next=a%2Fb+z", http.StatusFound)
			case "encoded":
				http.Redirect(w, r, "/api/profiles/%6cist", http.StatusFound)
			case "refused":
				http.Redirect(w, r, "/api/secret", http.StatusFound)
			case "external":
				http.Redirect(w, r, external.URL+"/capture", http.StatusFound)
			}
		case r.URL.Path == "/api/sessions":
			targetHits.Add(1)
			received <- r.RequestURI + " token=" + r.Header.Get("X-Hermes-Session-Token")
			_, _ = w.Write([]byte(`{"ok":true}`))
		case r.URL.Path == "/api/secret":
			refusedHits.Add(1)
			_, _ = w.Write([]byte(`{"leak":true}`))
		default:
			received <- r.RequestURI + " token=" + r.Header.Get("X-Hermes-Session-Token")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer gatewayServer.Close()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	connection, link, suite := newProxyTestConnectionForServer(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := "name=%E2%9C%93&x=a/b?c:d@e+z"
	if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet,
		Path: "/api/memory/providers/%E2%9C%93/config", Query: query,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusOK {
		t.Fatalf("escaped initial request = %+v", response)
	}
	requestURI := awaitS2TestValue(t, received, "upstream request")
	if !strings.Contains(requestURI, "/api/memory/providers/%E2%9C%93/config?"+query) ||
		!strings.Contains(requestURI, "token="+server.Gateway.Token()) {
		t.Fatalf("escaped target = %q", requestURI)
	}

	for id, scenario := range []string{"allowed", "encoded", "refused", "external"} {
		requestID := uint64(id + 2)
		if err := connection.startProxyHTTP(ctx, protocol.HTTPRequest{
			Ch: protocol.ChHTTP, ID: requestID, Method: http.MethodGet, Path: "/api/status",
			Query: "case=" + scenario,
		}, nil); err != nil {
			t.Fatal(err)
		}
		response := readTestHTTPResponse(t, link, suite)
		want := http.StatusBadGateway
		if scenario == "allowed" {
			want = http.StatusOK
			redirected := awaitS2TestValue(t, received, "redirected upstream request")
			if !strings.Contains(redirected, "/api/sessions?next=a%2Fb+z") ||
				!strings.Contains(redirected, "token="+server.Gateway.Token()) {
				t.Fatalf("allowed redirect = %q", redirected)
			}
		}
		if response.Status != want {
			t.Fatalf("%s redirect response = %+v", scenario, response)
		}
	}
	if targetHits.Load() != 1 || refusedHits.Load() != 0 || externalHits.Load() != 0 {
		t.Fatalf("redirect hits: target=%d refused=%d external=%d", targetHits.Load(), refusedHits.Load(), externalHits.Load())
	}
}

func TestProxyRejectedRedirectBodyIsClosedExactlyOnce(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	var destinationHits atomic.Int32
	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			http.Redirect(w, r, "/api/secret", http.StatusFound)
			return
		}
		destinationHits.Add(1)
		_, _ = w.Write([]byte(`{"unexpected":true}`))
	}))
	defer gatewayServer.Close()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	var closes atomic.Int32
	server.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if response != nil && response.Body != nil {
			response.Body = &closeCountingBody{ReadCloser: response.Body, closes: &closes}
		}
		return response, err
	})
	connection, link, suite := newProxyTestConnectionForServer(t, server)
	if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet, Path: "/api/status",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if response := readTestHTTPResponse(t, link, suite); response.Status != http.StatusBadGateway {
		t.Fatalf("rejected redirect response = %+v", response)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("rejected redirect response body closes = %d, want 1", got)
	}
	if destinationHits.Load() != 0 {
		t.Fatal("rejected redirect reached its destination")
	}
}

func TestProxyHTTPDoTimeoutMapsTo504AndParentCancellationRepliesNothing(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpAdmissionWait = 0
	transportExited := make(chan struct{}, 2)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		transportExited <- struct{}{}
		return nil, request.Context().Err()
	})
	connection, link, suite := newProxyTestConnection(t, deps, transport)
	connection.srv.httpClient.Timeout = 20 * time.Millisecond
	if err := connection.startProxyHTTP(context.Background(), protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 1, Method: http.MethodGet, Path: "/api/status",
	}, nil); err != nil {
		t.Fatal(err)
	}
	response := readTestHTTPResponse(t, link, suite)
	if response.Status != http.StatusGatewayTimeout {
		t.Fatalf("Do timeout response = %+v body=%s", response, response.Body)
	}

	connection.srv.httpClient.Timeout = time.Second
	parent, cancel := context.WithCancel(context.Background())
	if err := connection.startProxyHTTP(parent, protocol.HTTPRequest{
		Ch: protocol.ChHTTP, ID: 2, Method: http.MethodGet, Path: "/api/status",
	}, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-transportExited:
	case <-time.After(time.Second):
		t.Fatal("canceled transport did not exit")
	}
	// One transport-exit signal belonged to the completed timeout. Drain both
	// deterministically before inspecting the output queue.
	select {
	case <-transportExited:
	case <-time.After(time.Second):
		t.Fatal("parent-canceled transport did not exit")
	}
	if len(link.writes) != 0 {
		t.Fatal("parent cancellation emitted an HTTP response")
	}
	capacityCtx, capacityCancel := context.WithTimeout(context.Background(), time.Second)
	defer capacityCancel()
	lease, result := acquirePools(capacityCtx, time.Second, connection.httpSlots, connection.srv.httpSlots)
	if result != admissionGranted {
		t.Fatalf("parent cancellation retained HTTP capacity: admission=%v", result)
	}
	lease.release()
}

func TestFSBlockedWorkerRetainsCeilingReturnsBusyAndRestoresCapacity(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.httpPerConnection = 2
	deps.httpProcessWide = 2
	deps.httpAdmissionWait = 0
	deps.fsPerConnection = 1
	deps.fsProcessWide = 1
	deps.fsListTimeout = 15 * time.Millisecond
	deps.fsWarningAfter = 0
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	deps.fsReadDir = func(string) ([]os.DirEntry, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return nil, nil
	}
	connection, link, suite := newProxyTestConnection(t, deps, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	query := "path=" + url.QueryEscape(t.TempDir())
	request := func(id uint64) protocol.HTTPRequest {
		return protocol.HTTPRequest{
			Ch: protocol.ChHTTP, ID: id, Method: http.MethodGet, Path: "/api/fs/list", Query: query,
		}
	}
	if err := connection.startProxyHTTP(ctx, request(1), nil); err != nil {
		t.Fatal(err)
	}
	awaitS2TestValue(t, entered, "worker entry")
	first := readTestHTTPResponse(t, link, suite)
	if first.Status != http.StatusOK || !strings.Contains(string(first.Body), `"ETIMEDOUT"`) {
		t.Fatalf("timed-out listing = %+v body=%s", first, first.Body)
	}
	if len(connection.fsSlots.slots) != 1 || len(connection.srv.fsSlots.slots) != 1 {
		t.Fatal("blocked ReadDir did not retain filesystem capacity")
	}
	if err := connection.startProxyHTTP(ctx, request(2), nil); err != nil {
		t.Fatal(err)
	}
	busy := readTestHTTPResponse(t, link, suite)
	if busy.Status != http.StatusOK || !strings.Contains(string(busy.Body), `"EBUSY"`) || calls.Load() != 1 {
		t.Fatalf("busy listing=%+v body=%s calls=%d", busy, busy.Body, calls.Load())
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for (len(connection.fsSlots.slots) != 0 || len(connection.srv.fsSlots.slots) != 0) && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(connection.fsSlots.slots) != 0 || len(connection.srv.fsSlots.slots) != 0 {
		t.Fatal("completed ReadDir did not restore filesystem capacity")
	}
	if err := connection.startProxyHTTP(ctx, request(3), nil); err != nil {
		t.Fatal(err)
	}
	awaitS2TestValue(t, entered, "worker entry")
	third := readTestHTTPResponse(t, link, suite)
	if third.Status != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("listing after release=%+v calls=%d", third, calls.Load())
	}
}

func TestFSHeldSlotWarningFiresOnceAndStopsAfterCompletion(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.fsListTimeout = time.Second
	deps.fsWarningAfter = 10 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	deps.fsReadDir = func(string) ([]os.DirEntry, error) {
		close(entered)
		<-release
		return nil, nil
	}
	handler := &warningCaptureHandler{messages: make(chan string, 2)}
	logger := slog.New(handler)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = fsListWithDependencies(
			context.Background(), "path="+url.QueryEscape(t.TempDir()), deps, nil, logger,
		)
	}()
	awaitS2TestValue(t, entered, "worker entry")
	select {
	case message := <-handler.messages:
		if message != "filesystem listing still holds bridge capacity" {
			t.Fatalf("warning message = %q", message)
		}
	case <-time.After(time.Second):
		t.Fatal("held filesystem slot emitted no warning")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("filesystem listing did not finish")
	}
	select {
	case message := <-handler.messages:
		t.Fatalf("filesystem worker emitted an extra warning after completion: %q", message)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestBestEffortGatewayLimitCloseSkipsBusySendGate(t *testing.T) {
	deps := productionBridgeDependencies()
	deps.closeWriteTimeout = time.Second
	connection, link, _ := newProxyTestConnection(t, deps, nil)
	connection.sendMu.Lock()
	done := make(chan struct{})
	go func() {
		connection.bestEffortGatewayLimitClose()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		connection.sendMu.Unlock()
		t.Fatal("busy send gate blocked the oversize close path")
	}
	connection.sendMu.Unlock()
	if len(link.writes) != 0 {
		t.Fatal("busy send gate emitted a close frame")
	}
}

func TestBestEffortGatewayLimitCloseWritesReasonWhenGateIsFree(t *testing.T) {
	deps := productionBridgeDependencies()
	connection, link, suite := newProxyTestConnection(t, deps, nil)
	connection.bestEffortGatewayLimitClose()
	plain := readTestPlain(t, link, suite)
	var closeMessage protocol.CtlMessage
	if err := json.Unmarshal(plain, &closeMessage); err != nil {
		t.Fatal(err)
	}
	if closeMessage.Ch != protocol.ChCtl || closeMessage.Op != protocol.CtlClose ||
		closeMessage.Reason != "gateway frame exceeds bridge limit" {
		t.Fatalf("gateway limit close = %+v", closeMessage)
	}
	if len(link.writes) != 0 {
		t.Fatal("gateway limit close emitted extra frames")
	}
}

func newOversizeGatewayServer(t *testing.T, trigger <-chan struct{}, accepted chan<- struct{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewaySocket, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept gateway websocket: %v", err)
			return
		}
		defer gatewaySocket.Close(websocket.StatusNormalClosure, "")
		close(accepted)
		<-trigger
		writeCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := gatewaySocket.Write(
			writeCtx, websocket.MessageText, bytes.Repeat([]byte("x"), protocol.MaxPlaintext),
		); err != nil && r.Context().Err() == nil {
			t.Errorf("write oversized gateway frame: %v", err)
		}
	}))
}

func TestTunnelOversizeGatewayFrameWritesCloseBeforeFailing(t *testing.T) {
	trigger := make(chan struct{})
	accepted := make(chan struct{})
	gatewayServer := newOversizeGatewayServer(t, trigger, accepted)
	defer gatewayServer.Close()
	deps := productionBridgeDependencies()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	connection, link, suite := newProxyTestConnectionForServer(t, server)
	tunnelResult := make(chan error, 1)
	go func() { tunnelResult <- connection.tunnel(context.Background()) }()
	awaitS2TestValue(t, accepted, "gateway acceptance")

	var gatewayState protocol.CtlMessage
	if err := json.Unmarshal(readTestPlain(t, link, suite), &gatewayState); err != nil {
		t.Fatal(err)
	}
	if gatewayState.Op != protocol.CtlGateway {
		t.Fatalf("initial control message = %+v", gatewayState)
	}
	close(trigger)
	plain := readTestPlain(t, link, suite)
	var closeMessage protocol.CtlMessage
	if err := json.Unmarshal(plain, &closeMessage); err != nil {
		t.Fatalf("decode limit close %q: %v", plain, err)
	}
	if closeMessage.Op != protocol.CtlClose || closeMessage.Reason != "gateway frame exceeds bridge limit" {
		t.Fatalf("gateway limit close = %+v", closeMessage)
	}
	select {
	case err := <-tunnelResult:
		if !errors.Is(err, errPlaintextLimit) {
			t.Fatalf("tunnel result = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversized gateway frame did not end tunnel")
	}
	if len(link.writes) != 0 {
		t.Fatalf("oversized gateway wrapper emitted %d unexpected frames", len(link.writes))
	}
}

func TestTunnelOversizeGatewayFrameSkipsCloseWhenSendGateBusy(t *testing.T) {
	trigger := make(chan struct{})
	accepted := make(chan struct{})
	gatewayServer := newOversizeGatewayServer(t, trigger, accepted)
	defer gatewayServer.Close()
	deps := productionBridgeDependencies()
	server := newProxyTestServerAtGateway(t, deps, gatewayServer)
	connection, link, suite := newProxyTestConnectionForServer(t, server)
	tunnelResult := make(chan error, 1)
	go func() { tunnelResult <- connection.tunnel(context.Background()) }()
	awaitS2TestValue(t, accepted, "gateway acceptance")
	_ = readTestPlain(t, link, suite) // initial gateway-ready control message
	connection.sendMu.Lock()
	close(trigger)
	select {
	case err := <-tunnelResult:
		if !errors.Is(err, errPlaintextLimit) {
			connection.sendMu.Unlock()
			t.Fatalf("tunnel result = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		connection.sendMu.Unlock()
		t.Fatal("busy send gate delayed oversized-frame tunnel failure")
	}
	connection.sendMu.Unlock()
	if len(link.writes) != 0 {
		t.Fatal("busy send gate emitted a gateway-limit close")
	}
}

// awaitS2TestValue bounds test observations independently of the deliberately
// non-cooperative worker gates used to prove retained ownership.
func awaitS2TestValue[T any](t *testing.T, ch <-chan T, label string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return receiveTestValue(t, ctx, ch, label)
}
