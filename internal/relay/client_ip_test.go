package relay

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

// requestWithAddr builds a request with the given direct peer address; each
// xff value becomes one X-Forwarded-For header line.
func requestWithAddr(t *testing.T, remoteAddr string, xff ...string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/phone", nil)
	r.RemoteAddr = remoteAddr
	if len(xff) > 0 {
		r.Header["X-Forwarded-For"] = xff
	}
	return r
}

// The relay honours X-Forwarded-For only from a peer inside a trusted prefix —
// the loopback proxy by default, the configured CIDRs otherwise. Everywhere
// else the header is client-controlled and ignored.
func TestClientIP(t *testing.T) {
	loopback := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
	cases := []struct {
		name       string
		remoteAddr string
		xff        []string
		trusted    []netip.Prefix
		want       string
	}{
		{"non-loopback peer with a forged header uses the peer", "203.0.113.9:5000", []string{"1.2.3.4"}, loopback, "203.0.113.9"},
		{"loopback peer trusts the single forwarded entry", "127.0.0.1:5000", []string{"1.2.3.4"}, loopback, "1.2.3.4"},
		{"loopback peer uses the last entry", "127.0.0.1:5000", []string{"evil, 5.6.7.8"}, loopback, "5.6.7.8"},
		{"malformed last entry falls back to the peer", "127.0.0.1:5000", []string{"1.2.3.4, bogus"}, loopback, "127.0.0.1"},
		{"trailing empty entry is skipped", "127.0.0.1:5000", []string{"1.2.3.4, "}, loopback, "1.2.3.4"},
		{"IPv6 loopback peer is honoured", "[::1]:5000", []string{"2001:db8::42"}, loopback, "2001:db8::42"},
		{"IPv4-mapped loopback peer is honoured", "[::ffff:127.0.0.1]:5000", []string{"1.2.3.4"}, loopback, "1.2.3.4"},
		{"peer with no port is honoured", "127.0.0.1", []string{"1.2.3.4"}, loopback, "1.2.3.4"},
		{"zone-id loopback peer is honoured", "[::1%lo0]:5000", []string{"2001:db8::42"}, loopback, "2001:db8::42"},
		{"two header lines join before the last entry wins", "127.0.0.1:5000", []string{"evil", "9.9.9.9"}, loopback, "9.9.9.9"},
		{"the returned address is canonical", "127.0.0.1:5000", []string{" 2001:DB8::42 "}, loopback, "2001:db8::42"},
		{"no header uses the peer", "198.51.100.7:5000", nil, loopback, "198.51.100.7"},
		{"no header on loopback uses the peer", "127.0.0.1:5000", nil, loopback, "127.0.0.1"},
		{"trusted non-loopback proxy is honoured", "172.30.0.2:5000", []string{"9.9.9.9"}, []netip.Prefix{netip.MustParsePrefix("172.30.0.0/24")}, "9.9.9.9"},
		{"untrusted private peer uses the peer", "10.0.0.9:5000", []string{"9.9.9.9"}, []netip.Prefix{netip.MustParsePrefix("172.30.0.0/24")}, "10.0.0.9"},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			r := requestWithAddr(t, entry.remoteAddr, entry.xff...)
			if got := clientIP(r, entry.trusted); got != entry.want {
				t.Fatalf("clientIP = %q, want %q", got, entry.want)
			}
		})
	}
}

func TestParseTrustedProxies(t *testing.T) {
	got, err := ParseTrustedProxies("127.0.0.0/8, ::1/128")
	if err != nil || len(got) != 2 {
		t.Fatalf("ParseTrustedProxies = %v, %v; want two prefixes, nil error", got, err)
	}
	if got, err := ParseTrustedProxies(""); err != nil || got != nil {
		t.Fatalf("empty list = %v, %v; want nil, nil", got, err)
	}
	if _, err := ParseTrustedProxies("127.0.0.0/8,not-a-cidr"); err == nil {
		t.Fatal("a bad CIDR must fail fast at startup, got nil error")
	}
}
