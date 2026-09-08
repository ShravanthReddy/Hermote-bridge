package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

const gatewayResponseLimitDetail = "gateway response exceeds the bridge limit"

var errPlaintextLimit = errors.New("bridge plaintext exceeds protocol limit")

type plaintextLimitError struct {
	size int
}

func (e plaintextLimitError) Error() string {
	return fmt.Sprintf("%s: %d > %d", errPlaintextLimit, e.size, protocol.MaxPlaintext)
}

func (e plaintextLimitError) Unwrap() error { return errPlaintextLimit }

func marshalHTTPResponse(response protocol.HTTPResponse) ([]byte, error) {
	raw, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if len(raw) > protocol.MaxPlaintext {
		return nil, plaintextLimitError{size: len(raw)}
	}
	return raw, nil
}

func responseBody(errorName, detail string) json.RawMessage {
	body, _ := json.Marshal(struct {
		Error  string `json:"error"`
		Detail string `json:"detail,omitempty"`
	}{Error: errorName, Detail: detail})
	return body
}

func prepareHTTPResponse(id uint64, status int, body json.RawMessage) ([]byte, error) {
	raw, err := marshalHTTPResponse(protocol.HTTPResponse{
		Ch: protocol.ChHTTP, ID: id, Status: status, Body: body,
	})
	if !errors.Is(err, errPlaintextLimit) {
		return raw, err
	}
	return marshalHTTPResponse(protocol.HTTPResponse{
		Ch: protocol.ChHTTP, ID: id, Status: http.StatusBadGateway,
		Body: responseBody("gateway response too large", gatewayResponseLimitDetail),
	})
}

func (c *conn) writeHTTPResponse(ctx context.Context, raw []byte, fail func(error)) error {
	if err := c.send(ctx, raw); err != nil {
		wrapped := fmt.Errorf("http response write: %w", err)
		if fail != nil {
			fail(wrapped)
		}
		return wrapped
	}
	return nil
}

func (c *conn) replyHTTP(
	ctx context.Context, id uint64, status int, body json.RawMessage, fail func(error),
) error {
	raw, err := prepareHTTPResponse(id, status, body)
	if err != nil {
		return err
	}
	return c.writeHTTPResponse(ctx, raw, fail)
}

// startProxyHTTP performs validation and capacity admission on the connection
// reader. Only an admitted request creates a worker.
func (c *conn) startProxyHTTP(ctx context.Context, req protocol.HTTPRequest, fail func(error)) error {
	path, err := canonicalizePath(req.Path)
	if err != nil {
		return c.replyHTTP(
			ctx, req.ID, http.StatusBadRequest,
			responseBody("malformed bridge request", "invalid path"), fail,
		)
	}
	if err := validateRawQuery(req.Query); err != nil {
		return c.replyHTTP(
			ctx, req.ID, http.StatusBadRequest,
			responseBody("malformed bridge request", "invalid query"), fail,
		)
	}
	if !routeAllowed(req.Method, path.Path) {
		return c.replyHTTP(ctx, req.ID, http.StatusForbidden, json.RawMessage(RefusedRouteError), fail)
	}
	lease, result := acquirePools(ctx, c.srv.deps.httpAdmissionWait, c.httpSlots, c.srv.httpSlots)
	switch result {
	case admissionCanceled:
		return ctx.Err()
	case admissionBusy:
		return c.replyHTTP(
			ctx, req.ID, http.StatusTooManyRequests,
			responseBody("bridge busy", "too many bridge HTTP requests"), fail,
		)
	default:
		go c.proxyHTTP(ctx, req, path, lease, fail)
		return nil
	}
}

// proxyHTTP performs one admitted REST call against the loopback gateway with
// the session token. Capacity covers the complete response write to the phone.
func (c *conn) proxyHTTP(
	ctx context.Context, req protocol.HTTPRequest, path canonicalPath, lease *workLease, fail func(error),
) {
	defer lease.release()
	if req.Method == http.MethodGet && path.Path == "/api/fs/list" {
		c.proxyFSList(ctx, req, lease, fail)
		return
	}
	base, ok := c.srv.Gateway.BaseURL()
	if !ok {
		c.finishHTTP(
			ctx, req.ID, http.StatusServiceUnavailable,
			responseBody("gateway not ready", "gateway not ready"), lease, fail,
		)
		return
	}
	target, baseURL, err := gatewayTarget(base, path, req.Query)
	if err != nil {
		c.finishHTTP(
			ctx, req.ID, http.StatusBadGateway,
			responseBody("gateway request failed", "invalid gateway target"), lease, fail,
		)
		return
	}
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, req.Method, target.String(), body)
	if err != nil {
		c.finishHTTP(
			ctx, req.ID, http.StatusBadRequest,
			responseBody("malformed bridge request", "invalid request"), lease, fail,
		)
		return
	}
	// Preserve the exact admitted spelling rather than letting String invent a
	// different one after request construction.
	hreq.URL.Path = path.Path
	hreq.URL.RawPath = path.RawPath
	hreq.URL.RawQuery = req.Query
	hreq.Header.Set("X-Hermes-Session-Token", c.srv.Gateway.Token())
	if body != nil {
		hreq.Header.Set("Content-Type", "application/json")
	}
	client := *c.srv.httpClient
	client.CheckRedirect = redirectPolicy(baseURL, c.srv.Gateway.Token())
	resp, err := client.Do(hreq)
	if err != nil {
		// net/http returns a non-nil response with an error only when
		// CheckRedirect rejects a redirect, and it closes that body before
		// returning it. Do not close that response a second time.
		c.finishHTTPError(ctx, req.ID, err, lease, fail)
		return
	}
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, protocol.MaxPlaintext+1))
	_ = resp.Body.Close()
	if readErr != nil {
		c.finishHTTPError(ctx, req.ID, readErr, lease, fail)
		return
	}
	if len(data) > protocol.MaxPlaintext {
		c.finishHTTP(
			ctx, req.ID, http.StatusBadGateway,
			responseBody("gateway response too large", gatewayResponseLimitDetail), lease, fail,
		)
		return
	}
	if !json.Valid(data) {
		data, _ = json.Marshal(string(data))
	}
	c.finishHTTP(ctx, req.ID, resp.StatusCode, data, lease, fail)
}

func (c *conn) proxyFSList(
	ctx context.Context, req protocol.HTTPRequest, httpLease *workLease, fail func(error),
) {
	fsLease, result := acquirePools(ctx, 0, c.fsSlots, c.srv.fsSlots)
	if result == admissionCanceled {
		httpLease.release()
		return
	}
	if result == admissionBusy {
		status, body := fsListingResponse(http.StatusOK, fsListing{
			Entries: []fsEntry{}, Error: "EBUSY", Detail: "too many filesystem listings",
		})
		c.finishHTTP(ctx, req.ID, status, body, httpLease, fail)
		return
	}
	status, body, err := fsListWithDependencies(ctx, req.Query, c.srv.deps, fsLease, c.srv.Logger)
	if err != nil {
		httpLease.release()
		return
	}
	c.finishHTTP(ctx, req.ID, status, body, httpLease, fail)
}

func (c *conn) finishHTTP(
	ctx context.Context, id uint64, status int, body json.RawMessage, lease *workLease, fail func(error),
) {
	defer lease.release()
	raw, err := prepareHTTPResponse(id, status, body)
	if err != nil {
		if fail != nil {
			fail(fmt.Errorf("prepare http response: %w", err))
		}
		return
	}
	_ = c.writeHTTPResponse(ctx, raw, fail)
}

func (c *conn) finishHTTPError(
	ctx context.Context, id uint64, err error, lease *workLease, fail func(error),
) {
	if ctx.Err() != nil {
		lease.release()
		return
	}
	if isTimeout(err) {
		c.finishHTTP(
			ctx, id, http.StatusGatewayTimeout,
			responseBody("gateway timeout", "gateway request timed out"), lease, fail,
		)
		return
	}
	c.finishHTTP(
		ctx, id, http.StatusBadGateway,
		responseBody("gateway request failed", "gateway request failed"), lease, fail,
	)
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func gatewayTarget(base string, path canonicalPath, query string) (*url.URL, *url.URL, error) {
	baseURL, err := url.Parse(base)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, nil, errors.New("invalid gateway base URL")
	}
	target := *baseURL
	target.Path = path.Path
	target.RawPath = path.RawPath
	target.RawQuery = query
	target.Fragment = ""
	return &target, baseURL, nil
}

func redirectPolicy(base *url.URL, token string) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		req.Header.Del("X-Hermes-Session-Token")
		if len(via) >= 10 {
			return errors.New("bridge redirect limit exceeded")
		}
		if !sameOrigin(base, req.URL) {
			return errors.New("bridge redirect origin refused")
		}
		path, err := canonicalizePath(req.URL.EscapedPath())
		if err != nil || validateRawQuery(req.URL.RawQuery) != nil || !routeAllowed(req.Method, path.Path) {
			return errors.New("bridge redirect route refused")
		}
		req.URL.Path = path.Path
		req.URL.RawPath = path.RawPath
		req.Header.Set("X-Hermes-Session-Token", token)
		return nil
	}
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) &&
		strings.EqualFold(left.Hostname(), right.Hostname()) && effectivePort(left) == effectivePort(right)
}

func effectivePort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	return "80"
}
