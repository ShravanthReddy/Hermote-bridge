package bridge

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

// RefusedRouteError is the JSON body the bridge returns (HTTP 403) for a REST
// route outside the allow-list. HermesKit matches it verbatim to tell the user
// their hermes-remote is older than the app (docs/REMOTE-ACCESS.md §7).
const RefusedRouteError = `{"error":"path not allowed through the bridge"}`

const MalformedRequestError = `{"error":"malformed bridge request"}`

var (
	errInvalidPath  = errors.New("invalid bridge path")
	errInvalidQuery = errors.New("invalid bridge query")
)

type canonicalPath struct {
	Path    string
	RawPath string
}

// route is one allowed REST route: an HTTP method and a path prefix. A prefix
// ending in "/" also matches the bare path without the slash, so "/api/sessions/"
// covers both the collection and its items.
type route struct {
	method string
	prefix string
}

// allowedRoutes is the dashboard REST surface the phone may reach through the
// bridge. It mirrors what the desktop app calls; the bridge is deliberately not
// a general proxy. Routes are grouped by the app area that owns them so a new
// feature adds its lines in one place. Everything else is refused, and so is
// any path containing "..".
var allowedRoutes = []route{
	// Health, status, gateway lifecycle.
	{http.MethodGet, "/api/status"},
	{http.MethodPost, "/api/gateway/restart"},
	{http.MethodGet, "/api/hermes/update/check"},
	{http.MethodPost, "/api/hermes/update"},
	{http.MethodGet, "/api/logs"},

	// Sessions directory and media. GET /api/sessions/<id>/messages reads a
	// stored transcript without resuming it (export, the Artifacts index).
	{http.MethodGet, "/api/sessions"},
	{http.MethodGet, "/api/sessions/"},
	{http.MethodGet, "/api/sessions/search"},
	{http.MethodPatch, "/api/sessions/"},
	{http.MethodDelete, "/api/sessions/"},
	{http.MethodGet, "/api/media"},

	// Configuration (schema-driven settings) and environment.
	{http.MethodGet, "/api/config"},
	{http.MethodPut, "/api/config"},
	{http.MethodGet, "/api/config/schema"},
	{http.MethodGet, "/api/config/defaults"},
	{http.MethodGet, "/api/env"},
	{http.MethodPut, "/api/env"},
	{http.MethodDelete, "/api/env"},
	{http.MethodPost, "/api/env/reveal"},

	// Models and providers.
	{http.MethodGet, "/api/model/"},
	{http.MethodPost, "/api/model/set"},
	{http.MethodPut, "/api/model/auxiliary"},
	{http.MethodPut, "/api/model/moa"},
	{http.MethodGet, "/api/providers/"},
	{http.MethodPost, "/api/providers/"},
	{http.MethodPut, "/api/providers/"},
	{http.MethodDelete, "/api/providers/"},
	{http.MethodGet, "/api/local-models/"},
	{http.MethodPost, "/api/local-models/"},
	{http.MethodDelete, "/api/local-models/"},

	// Profiles.
	{http.MethodGet, "/api/profiles"},
	{http.MethodPost, "/api/profiles"},
	{http.MethodPut, "/api/profiles/"},
	{http.MethodDelete, "/api/profiles/"},

	// Capabilities: skills, tools, MCP.
	{http.MethodGet, "/api/skills"},
	{http.MethodPost, "/api/skills"},
	{http.MethodPost, "/api/skills/"},
	{http.MethodGet, "/api/skills/"},
	{http.MethodPut, "/api/skills/"},
	{http.MethodGet, "/api/tools/"},
	{http.MethodPut, "/api/tools/"},
	{http.MethodPost, "/api/tools/"},
	{http.MethodGet, "/api/mcp/"},
	{http.MethodPost, "/api/mcp/"},
	{http.MethodPut, "/api/mcp/"},
	{http.MethodDelete, "/api/mcp/"},

	// Workspace: files, git, projects.
	{http.MethodGet, "/api/fs/"},
	{http.MethodPost, "/api/fs/"},
	{http.MethodGet, "/api/git/"},
	{http.MethodPost, "/api/git/"},

	// Knowledge: memory, learning graph, curator.
	{http.MethodGet, "/api/memory"},
	{http.MethodPost, "/api/memory/"},
	{http.MethodGet, "/api/memory/"},
	// Memory providers (plan 10 / WP8): a provider's settings form is saved
	// with PUT; POST runs its package install on the Mac.
	{http.MethodPut, "/api/memory/providers/"},
	{http.MethodPost, "/api/memory/providers/"},
	{http.MethodGet, "/api/learning/"},
	{http.MethodPost, "/api/learning/"},
	// Starmap node mutations are exact routes. Do not widen PUT or DELETE to
	// the learning prefix: the phone cannot mutate another learning endpoint.
	{http.MethodPut, "/api/learning/node"},
	{http.MethodDelete, "/api/learning/node"},
	{http.MethodGet, "/api/curator"},
	{http.MethodPost, "/api/curator/"},
	{http.MethodPut, "/api/curator/"},

	// Operations and analytics (plan 10 / WP4): spawned actions are tailed
	// through /api/actions/<name>/status; a finished backup is fetched from
	// /api/ops/backup/download.
	{http.MethodPost, "/api/ops/"},
	{http.MethodGet, "/api/ops/"},
	{http.MethodGet, "/api/actions/"},
	{http.MethodGet, "/api/analytics/"},

	// Messaging platforms, pairing, webhooks, scheduled jobs.
	{http.MethodGet, "/api/messaging/"},
	{http.MethodPut, "/api/messaging/"},
	{http.MethodPost, "/api/messaging/"},
	// Cancelling a Telegram / WhatsApp guided setup.
	{http.MethodDelete, "/api/messaging/"},
	{http.MethodGet, "/api/pairing"},
	{http.MethodPost, "/api/pairing/"},
	{http.MethodGet, "/api/webhooks"},
	{http.MethodPost, "/api/webhooks"},
	{http.MethodPost, "/api/webhooks/"},
	{http.MethodPut, "/api/webhooks/"},
	{http.MethodDelete, "/api/webhooks/"},
	{http.MethodGet, "/api/cron/"},
	{http.MethodPost, "/api/cron/"},

	// Voice.
	{http.MethodGet, "/api/audio/"},
	{http.MethodPost, "/api/audio/"},

	// Plugins.
	{http.MethodGet, "/api/plugins/"},
	{http.MethodPost, "/api/plugins/"},
}

// routeAllowed reports whether an already-canonical method+path may be
// proxied to the gateway.
func routeAllowed(method, path string) bool {
	for _, r := range allowedRoutes {
		if r.method != method {
			continue
		}
		if path == r.prefix || path == strings.TrimSuffix(r.prefix, "/") {
			return true
		}
		if strings.HasSuffix(r.prefix, "/") && strings.HasPrefix(path, r.prefix) {
			return true
		}
	}
	return false
}

// canonicalizePath admits one origin-form path spelling and returns its single
// decoded representation. Ambiguous encodings are rejected instead of being
// normalized differently by the route table, filesystem intercept, or proxy.
func canonicalizePath(raw string) (canonicalPath, error) {
	if raw == "" || raw[0] != '/' || strings.ContainsAny(raw, "?#\\\x00") || strings.Contains(raw, "//") {
		return canonicalPath{}, errInvalidPath
	}
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b != '%' {
			if !isPathByte(b) {
				return canonicalPath{}, errInvalidPath
			}
			continue
		}
		if i+2 >= len(raw) || !isHex(raw[i+1]) || !isHex(raw[i+2]) {
			return canonicalPath{}, errInvalidPath
		}
		decoded := unhex(raw[i+1])<<4 | unhex(raw[i+2])
		if decoded == '/' || decoded == '\\' || decoded == '%' || isUnreserved(decoded) {
			return canonicalPath{}, errInvalidPath
		}
		i += 2
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil || !utf8.ValidString(decoded) || strings.ContainsRune(decoded, '\\') || hasControlByte(decoded) {
		return canonicalPath{}, errInvalidPath
	}
	for _, segment := range strings.Split(decoded, "/") {
		if segment == "." || segment == ".." {
			return canonicalPath{}, errInvalidPath
		}
	}
	return canonicalPath{Path: decoded, RawPath: raw}, nil
}

func isPathByte(b byte) bool {
	return isUnreserved(b) || b == '/' || strings.ContainsRune("!$&'()*+,;=:@", rune(b))
}

func hasControlByte(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] == 0x7f {
			return true
		}
	}
	return false
}

// validateRawQuery accepts the RFC 3986 query production used by
// URLComponents and preserves it verbatim for the gateway request.
func validateRawQuery(raw string) error {
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b >= utf8.RuneSelf || b < 0x21 || b == 0x7f || b == '#' {
			return errInvalidQuery
		}
		if b == '%' {
			if i+2 >= len(raw) || !isHex(raw[i+1]) || !isHex(raw[i+2]) {
				return errInvalidQuery
			}
			i += 2
			continue
		}
		if !isQueryByte(b) {
			return errInvalidQuery
		}
	}
	return nil
}

func isQueryByte(b byte) bool {
	return isUnreserved(b) || strings.ContainsRune("!$&'()*+,;=:@/?", rune(b))
}

func isUnreserved(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
		b == '-' || b == '.' || b == '_' || b == '~'
}

func isHex(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

func unhex(b byte) byte {
	switch {
	case b >= '0' && b <= '9':
		return b - '0'
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	default:
		return b - 'A' + 10
	}
}
