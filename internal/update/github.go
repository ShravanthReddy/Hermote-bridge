package update

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// D4's request forms. The bridge asks for the latest release and, from
// Phase 2, downloads assets of this repository; nothing else is requested.
const (
	repository           = "ShravanthReddy/Hermote-bridge"
	latestReleasePath    = "/repos/" + repository + "/releases/latest"
	latestReleaseURL     = "https://api.github.com" + latestReleasePath
	releaseDownloadPath  = "/" + repository + "/releases/download/"
	releaseTagURLPrefix  = "https://github.com/" + repository + "/releases/tag/"
	maxAPIResponseBytes  = 1 << 20
	maxDownloadRedirects = 3
)

// downloadHosts may serve a release asset after a redirect; the signed
// digest authenticates the bytes, so any path is accepted there (D4).
var downloadHosts = map[string]bool{
	"release-assets.githubusercontent.com": true,
	"objects.githubusercontent.com":        true,
}

var (
	errAPIRedirect      = errors.New("api.github.com redirected the request")
	errTooManyRedirects = errors.New("release download redirected too many times")
	errRedirectRefused  = errors.New("release download redirected outside GitHub's release hosts")
)

// GitHub is the release source: the repository's latest release through
// GitHub's unauthenticated REST API.
type GitHub struct {
	client    *http.Client
	latestURL string
}

// NewGitHub returns the production source.
func NewGitHub() *GitHub {
	return &GitHub{client: newGitHubClient(nil), latestURL: latestReleaseURL}
}

func newGitHubClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport, CheckRedirect: checkRedirect}
}

// checkRedirect enforces D4: an API request is never redirected; a release
// download follows at most three redirects, each over HTTPS on the default
// port without userinfo, to this repository's download path on github.com or
// to GitHub's asset hosts.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	if strings.HasPrefix(via[0].URL.Path, "/repos/") {
		return errAPIRedirect
	}
	// via holds the original request and every redirect already followed.
	if len(via) > maxDownloadRedirects {
		return errTooManyRedirects
	}
	target := req.URL
	if target.Scheme != "https" || target.User != nil || (target.Port() != "" && target.Port() != "443") {
		return errRedirectRefused
	}
	host := strings.ToLower(target.Hostname())
	if downloadHosts[host] {
		return nil
	}
	if host == "github.com" && strings.HasPrefix(target.Path, releaseDownloadPath) {
		return nil
	}
	return errRedirectRefused
}

// Latest asks for the latest release. ctx bounds the whole exchange,
// including the body, which is read to at most 1 MiB.
func (g *GitHub) Latest(ctx context.Context) (Release, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.latestURL, nil)
	if err != nil {
		return Release{}, &SourceError{Kind: ErrorMalformed, Message: "api.github.com: " + err.Error()}
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "hermote-bridge")
	resp, err := g.client.Do(req)
	if err != nil {
		return Release{}, transportError(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseBytes+1))
	if err != nil {
		return Release{}, transportError(err)
	}
	if err := statusError(resp); err != nil {
		return Release{}, err
	}
	if len(body) > maxAPIResponseBytes {
		return Release{}, &SourceError{Kind: ErrorMalformed, Message: "api.github.com sent more than 1 MiB for the latest release."}
	}
	var wire struct {
		TagName     string    `json:"tag_name"`
		PublishedAt time.Time `json:"published_at"`
		Body        string    `json:"body"`
	}
	if err := json.Unmarshal(body, &wire); err != nil || wire.TagName == "" {
		return Release{}, &SourceError{Kind: ErrorMalformed, Message: "api.github.com sent a latest release the bridge could not read."}
	}
	return Release{Tag: wire.TagName, PublishedAt: wire.PublishedAt, Body: wire.Body}, nil
}

// statusError words a non-200 answer as the desktop does
// (apps/desktop/electron/update-api-check.ts:153-181). The bridge never
// sends a GitHub token, so a spent limit is always the anonymous one.
func statusError(resp *http.Response) error {
	status := resp.StatusCode
	if status == http.StatusOK {
		return nil
	}
	if (status == http.StatusForbidden || status == http.StatusTooManyRequests) &&
		resp.Header.Get("X-RateLimit-Remaining") == "0" {
		var resetAt time.Time
		if seconds, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			resetAt = time.Unix(seconds, 0).UTC()
		}
		return &SourceError{
			Kind: ErrorRateLimited, ResetAt: resetAt,
			Message: fmt.Sprintf("GitHub API rate limit reached (HTTP %d): anonymous requests are limited to 60 per hour "+
				"per network address, shared with everyone behind the same connection.", status),
		}
	}
	if status >= 500 {
		return &SourceError{Kind: ErrorNetwork, Message: fmt.Sprintf(
			"GitHub is having trouble (HTTP %d from api.github.com) — check githubstatus.com and try again later.", status)}
	}
	return &SourceError{Kind: ErrorNetwork, Message: fmt.Sprintf("api.github.com answered HTTP %d.", status)}
}

// transportError words a failed exchange as the desktop does
// (update-api-check.ts:179-196), with this spec's 20-second bound.
func transportError(err error) error {
	var dnsErr *net.DNSError
	var netErr net.Error
	var certErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	switch {
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()):
		return &SourceError{Kind: ErrorTimeout, Message: fmt.Sprintf(
			"api.github.com did not answer within %d seconds.", int(apiRequestTimeout/time.Second))}
	case errors.Is(err, errAPIRedirect):
		return &SourceError{Kind: ErrorMalformed, Message: "api.github.com redirected the request; the bridge does not follow API redirects."}
	case errors.As(err, &dnsErr):
		return &SourceError{Kind: ErrorNetwork, Message: "DNS lookup for api.github.com failed — check your connection or proxy."}
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return &SourceError{Kind: ErrorNetwork, Message: fmt.Sprintf(
			"Connection to api.github.com failed (%s) — a firewall or proxy may be blocking it.", errnoName(err))}
	case errors.As(err, &certErr), errors.As(err, &unknownAuthority):
		return &SourceError{Kind: ErrorNetwork, Message: "TLS handshake with api.github.com failed — a proxy may be intercepting HTTPS."}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return &SourceError{Kind: ErrorNetwork, Message: "api.github.com: " + err.Error()}
}

func errnoName(err error) string {
	for _, known := range []struct {
		errno syscall.Errno
		name  string
	}{
		{syscall.ECONNREFUSED, "ECONNREFUSED"}, {syscall.ECONNRESET, "ECONNRESET"},
		{syscall.EHOSTUNREACH, "EHOSTUNREACH"}, {syscall.ENETUNREACH, "ENETUNREACH"},
	} {
		if errors.Is(err, known.errno) {
			return known.name
		}
	}
	return "connection error"
}
