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
		{"method matters", http.MethodDelete, "/api/status", false},
		{"unknown route", http.MethodGet, "/api/secret-dump", false},
		{"traversal is refused", http.MethodGet, "/api/fs/../etc/passwd", false},
		{"prefix must be a path segment", http.MethodGet, "/api/statusx", false},
		{"websocket endpoint is not proxied", http.MethodGet, "/api/ws", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := routeAllowed(tc.method, tc.path); got != tc.want {
				t.Fatalf("routeAllowed(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
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
