package update

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// tlsSource is a GitHub source pointed at a local TLS server that answers
// the latest-release path with handler.
func tlsSource(t *testing.T, handler http.HandlerFunc) (*GitHub, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	source := NewGitHub()
	source.client = newGitHubClient(server.Client().Transport)
	source.latestURL = server.URL + latestReleasePath
	return source, server
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// redirect asks the client's policy whether a download of first may follow
// a redirect to target.
func redirect(t *testing.T, first, target string) error {
	t.Helper()
	via := []*http.Request{{Method: http.MethodGet, URL: mustURL(t, first)}}
	return checkRedirect(&http.Request{Method: http.MethodGet, URL: mustURL(t, target)}, via)
}

const downloadURL = "https://github.com/ShravanthReddy/Hermote-bridge/releases/download/v0.16.0/hermote-bridge_0.16.0_darwin_universal.tar.gz"

func TestAPIRedirectRefused(t *testing.T) {
	var redirected bool
	source, server := tlsSource(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			redirected = true
			_, _ = w.Write([]byte(`{"tag_name":"v9.9.9"}`))
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	})
	_ = server
	_, err := source.Latest(context.Background())
	var sourceErr *SourceError
	if !errors.As(err, &sourceErr) || redirected {
		t.Fatalf("an API redirect was followed: %v (redirected=%v)", err, redirected)
	}
}

func TestDownloadRedirectToOtherRepositoryRefused(t *testing.T) {
	if err := redirect(t, downloadURL, "https://release-assets.githubusercontent.com/github-production-release-asset/1/abc?sig=x"); err != nil {
		t.Fatalf("the release asset host was refused: %v", err)
	}
	if err := redirect(t, downloadURL, "https://objects.githubusercontent.com/github-production-release-asset-2e65be/1"); err != nil {
		t.Fatalf("the object host was refused: %v", err)
	}
	if err := redirect(t, downloadURL, "https://github.com/ShravanthReddy/Hermote-bridge/releases/download/v0.16.0/checksums.txt"); err != nil {
		t.Fatalf("a download path in this repository was refused: %v", err)
	}
	for _, target := range []string{
		"https://github.com/someone/Hermote-bridge/releases/download/v0.16.0/hermote-bridge.tar.gz",
		"https://github.com/ShravanthReddy/Hermote-bridge/archive/refs/tags/v0.16.0.tar.gz",
		"https://github.com.evil.example/ShravanthReddy/Hermote-bridge/releases/download/v0.16.0/x",
		"https://evil.example/github-production-release-asset/1",
	} {
		if err := redirect(t, downloadURL, target); err == nil {
			t.Fatalf("redirect to %s was allowed", target)
		}
	}
	via := []*http.Request{{Method: http.MethodGet, URL: mustURL(t, downloadURL)}}
	for len(via) < maxDownloadRedirects {
		via = append(via, &http.Request{Method: http.MethodGet, URL: mustURL(t, downloadURL)})
	}
	if err := checkRedirect(&http.Request{URL: mustURL(t, downloadURL)}, via); err != nil {
		t.Fatalf("redirect %d of %d was refused: %v", len(via), maxDownloadRedirects, err)
	}
	via = append(via, &http.Request{Method: http.MethodGet, URL: mustURL(t, downloadURL)})
	if err := checkRedirect(&http.Request{URL: mustURL(t, downloadURL)}, via); err == nil {
		t.Fatalf("redirect %d was allowed", len(via))
	}
}

func TestRedirectWithUserinfoOrPortRefused(t *testing.T) {
	for _, target := range []string{
		"https://user:pass@release-assets.githubusercontent.com/asset",
		"https://user@objects.githubusercontent.com/asset",
		"https://release-assets.githubusercontent.com:8443/asset",
		"https://github.com:444/ShravanthReddy/Hermote-bridge/releases/download/v0.16.0/x",
	} {
		if err := redirect(t, downloadURL, target); err == nil {
			t.Fatalf("redirect to %s was allowed", target)
		}
	}
}

func TestPlainHTTPRedirectRefused(t *testing.T) {
	for _, target := range []string{
		"http://release-assets.githubusercontent.com/asset",
		"http://github.com/ShravanthReddy/Hermote-bridge/releases/download/v0.16.0/x",
	} {
		if err := redirect(t, downloadURL, target); err == nil {
			t.Fatalf("redirect to %s was allowed", target)
		}
	}
}

func TestOversizedResponseRejected(t *testing.T) {
	source, _ := tlsSource(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"tag_name":"v0.16.0","body":"` + strings.Repeat("a", maxAPIResponseBytes) + `"}`))
	})
	_, err := source.Latest(context.Background())
	var sourceErr *SourceError
	if !errors.As(err, &sourceErr) || sourceErr.Kind != ErrorMalformed {
		t.Fatalf("a response over %d bytes gave %v", maxAPIResponseBytes, err)
	}
}
