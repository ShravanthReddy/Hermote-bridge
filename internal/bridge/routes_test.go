package bridge

import (
	"net/http"
	"testing"
)

func TestRouteAllowed(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{"exact route", http.MethodGet, "/api/status", true},
		{"collection with slash prefix", http.MethodPatch, "/api/sessions/abc123", true},
		{"bare collection for slash prefix", http.MethodGet, "/api/profiles", true},
		{"config schema is readable", http.MethodGet, "/api/config/schema", true},
		{"config is writable", http.MethodPut, "/api/config", true},
		{"nested workspace route", http.MethodPost, "/api/git/review/stage", true},
		{"curator pause is a PUT", http.MethodPut, "/api/curator/paused", true},
		{"stored transcript is readable", http.MethodGet, "/api/sessions/20260901_abc/messages", true},
		{"skill toggle is a PUT", http.MethodPut, "/api/skills/toggle", true},
		{"skill edit is a PUT", http.MethodPut, "/api/skills/content", true},
		{"spawned action tail", http.MethodGet, "/api/actions/doctor/status", true},
		{"backup download", http.MethodGet, "/api/ops/backup/download", true},
		{"onboarding cancel is a DELETE", http.MethodDelete, "/api/messaging/whatsapp/onboarding/p1", true},
		{"webhook delete", http.MethodDelete, "/api/webhooks/deploys", true},
		{"memory provider form is writable", http.MethodPut, "/api/memory/providers/honcho/config", true},
		{"memory provider setup is a POST", http.MethodPost, "/api/memory/providers/honcho/setup", true},
		{"memory files are not writable", http.MethodPut, "/api/memory", false},
		{"method matters", http.MethodDelete, "/api/status", false},
		{"unknown route", http.MethodGet, "/api/secret-dump", false},
		{"traversal is refused", http.MethodGet, "/api/fs/../etc/passwd", false},
		{"prefix must be a path segment", http.MethodGet, "/api/statusx", false},
		{"websocket endpoint is not proxied", http.MethodGet, "/api/ws", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			canonical, err := canonicalizePath(tc.path)
			got := err == nil && routeAllowed(tc.method, canonical.Path)
			if got != tc.want {
				t.Fatalf("routeAllowed(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestCanonicalizePathRejectsAmbiguousSpellings(t *testing.T) {
	cases := []string{
		"", "api/status", "https://127.0.0.1/api/status", "/api/status?x=1", "/api/status#fragment",
		"/api//status", "/api/./status", "/api/fs/../status", "/api/fs/%2e%2e/status",
		"/api/fs/%2E/status", "/api/fs/%2fetc", "/api/fs/%2Fetc", "/api/fs/%5cetc",
		"/api/fs/%5Cetc", "/api/fs/%25etc", "/api/fs/%252e%252e", "/api/fs/%6cist",
		"/api/fs/%6List", "/api/fs/%00list", "/api/fs/%", "/api/fs/%0", "/api/fs/%GG", "/api/fs\\list",
		"/api/fs/\x00list", "/api/fs/\nlist",
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			if got, err := canonicalizePath(raw); err == nil {
				t.Fatalf("canonicalizePath(%q) = %+v, want error", raw, got)
			}
		})
	}

	got, err := canonicalizePath("/api/profiles/%E2%9C%93")
	if err != nil || got.Path != "/api/profiles/✓" || got.RawPath != "/api/profiles/%E2%9C%93" {
		t.Fatalf("encoded UTF-8 path = %+v, %v", got, err)
	}
}

func TestCanonicalRouteBoundariesAndInterpolatedCallsites(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodGet, "/api/fs", true},
		{http.MethodGet, "/api/fs/list", true},
		{http.MethodGet, "/api/fsx", false},
		{http.MethodPatch, "/api/sessions/session-1", true},
		{http.MethodGet, "/api/sessions/session-1/messages", true},
		{http.MethodDelete, "/api/profiles/research", true},
		{http.MethodPost, "/api/providers/oauth/openai/start", true},
		{http.MethodGet, "/api/providers/oauth/openai/poll/oauth-session", true},
		{http.MethodDelete, "/api/providers/oauth/sessions/oauth-session", true},
		{http.MethodPost, "/api/providers/custom-endpoints/local/activate", true},
		{http.MethodGet, "/api/local-models/jobs/job-1", true},
		{http.MethodDelete, "/api/local-models/models/qwen3-8b-q4", true},
		{http.MethodPut, "/api/mcp/servers/image_gen/enabled", true},
		{http.MethodGet, "/api/memory/providers/honcho/config", true},
		{http.MethodPost, "/api/memory/providers/honcho/setup", true},
		{http.MethodPut, "/api/messaging/platforms/telegram", true},
		{http.MethodGet, "/api/messaging/telegram/onboarding/pair-1", true},
		{http.MethodDelete, "/api/webhooks/deploy", true},
		{http.MethodPut, "/api/tools/toolsets/browser/config", true},
		{http.MethodGet, "/api/actions/doctor/status", true},
	}
	for _, tc := range cases {
		canonical, err := canonicalizePath(tc.path)
		if err != nil {
			t.Fatalf("canonicalizePath(%q): %v", tc.path, err)
		}
		if got := routeAllowed(tc.method, canonical.Path); got != tc.want {
			t.Errorf("routeAllowed(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestValidateRawQuery(t *testing.T) {
	accepted := []string{
		"", "limit=20&offset=0", "name=hello+world", "name=%E2%9C%93", "x=a/b?c:d@e",
		"symbols=!$&'()*+,;=:@/?", "encoded=%23%20%25",
	}
	for _, raw := range accepted {
		if err := validateRawQuery(raw); err != nil {
			t.Errorf("validateRawQuery(%q): %v", raw, err)
		}
	}
	rejected := []string{"raw=✓", "bad=%", "bad=%0", "bad=%GG", "x=#fragment", "x=hello world", "x=\t", "x=\x00", "x=[a]"}
	for _, raw := range rejected {
		if err := validateRawQuery(raw); err == nil {
			t.Errorf("validateRawQuery(%q) succeeded", raw)
		}
	}
}

// Every entry in the table must be a well-formed API path; a typo here would
// silently open or close a route.
func TestAllowedRoutesAreWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range allowedRoutes {
		key := r.method + " " + r.prefix
		if seen[key] {
			t.Errorf("duplicate route %s", key)
		}
		seen[key] = true
		if len(r.prefix) < len("/api/") || r.prefix[:5] != "/api/" {
			t.Errorf("route %s is outside /api/", key)
		}
	}
}
