package bridge

import (
	"net/http"
	"strings"
)

// RefusedRouteError is the JSON body the bridge returns (HTTP 403) for a REST
// route outside the allow-list. HermesKit matches it verbatim to tell the user
// their hermes-remote is older than the app (docs/REMOTE-ACCESS.md §7).
const RefusedRouteError = `{"error":"path not allowed through the bridge"}`

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
	{http.MethodGet, "/api/learning/"},
	{http.MethodPost, "/api/learning/"},
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

// routeAllowed reports whether the phone may proxy method+path to the gateway.
func routeAllowed(method, path string) bool {
	if strings.Contains(path, "..") {
		return false
	}
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
