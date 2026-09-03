package push

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// APNs hosts (HTTP/2; net/http negotiates it over TLS).
const (
	HostProduction = "https://api.push.apple.com"
	HostSandbox    = "https://api.sandbox.push.apple.com"
)

// ErrUnregistered means APNs no longer knows the token: the phone
// reinstalled or the token was bad. The registration should be dropped.
var ErrUnregistered = errors.New("device token is no longer registered")

// Alert is one notification. Category names match the app's registered
// UNNotificationCategory identifiers.
type Alert struct {
	Kind      Kind   `json:"kind"`
	Title     string `json:"-"`
	Body      string `json:"-"`
	Category  string `json:"-"`
	SessionID string `json:"session_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	// Sound file in the app's Library/Sounds, e.g. "completion-3.wav"; empty
	// for the default.
	Sound string `json:"-"`
	// Alerts with the same collapse id replace each other on the device.
	CollapseID string `json:"-"`
}

// Client sends alerts with a token-auth (ES256 JWT) APNs key.
type Client struct {
	cfg  Config
	key  *ecdsa.PrivateKey
	http *http.Client
	// Host overrides the APNs host (tests point it at a local server).
	Host string
	now  func() time.Time

	mu       sync.Mutex
	bearer   string
	bearerAt time.Time
}

// NewClient loads the .p8 key named by cfg.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("apns key: %w", err)
	}
	key, err := parseKey(raw)
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, key: key, http: &http.Client{Timeout: 20 * time.Second}, now: time.Now}, nil
}

func parseKey(raw []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("apns key: not a PEM file")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("apns key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("apns key: not an EC key")
	}
	return key, nil
}

// Send delivers one alert to a device token in the given environment.
func (c *Client) Send(ctx context.Context, deviceToken, environment string, a Alert) error {
	host := c.Host
	if host == "" {
		host = HostProduction
		if environment == "sandbox" {
			host = HostSandbox
		}
	}
	bearer, err := c.token()
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload(a))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, host+"/3/device/"+deviceToken, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "bearer "+bearer)
	req.Header.Set("apns-topic", c.cfg.BundleID)
	req.Header.Set("apns-push-type", "alert")
	req.Header.Set("apns-priority", "10")
	req.Header.Set("apns-expiration", fmt.Sprint(c.now().Add(time.Hour).Unix()))
	if a.CollapseID != "" {
		req.Header.Set("apns-collapse-id", a.CollapseID)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var failure struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(reply, &failure)
	if resp.StatusCode == http.StatusGone || failure.Reason == "BadDeviceToken" || failure.Reason == "Unregistered" {
		return ErrUnregistered
	}
	return fmt.Errorf("apns %d %s", resp.StatusCode, failure.Reason)
}

// payload is the APNs JSON: the `aps` dictionary plus a `hermes` dictionary
// the app reads to open the right chat or answer an approval.
func payload(a Alert) map[string]any {
	aps := map[string]any{
		"alert": map[string]string{"title": a.Title, "body": a.Body},
		"sound": "default",
	}
	if a.Sound != "" {
		aps["sound"] = a.Sound
	}
	if a.Category != "" {
		aps["category"] = a.Category
	}
	if a.SessionID != "" {
		aps["thread-id"] = a.SessionID
	}
	return map[string]any{"aps": aps, "hermes": a}
}

// token returns a bearer JWT, reused for 50 minutes (Apple accepts up to an
// hour and throttles keys refreshed more than once every 20 minutes).
func (c *Client) token() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.bearer != "" && now.Sub(c.bearerAt) < 50*time.Minute {
		return c.bearer, nil
	}
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": c.cfg.KeyID})
	claims, _ := json.Marshal(map[string]any{"iss": c.cfg.TeamID, "iat": now.Unix()})
	signing := b64(header) + "." + b64(claims)
	digest := sha256.Sum256([]byte(signing))
	signature, err := signES256(c.key, digest[:])
	if err != nil {
		return "", err
	}
	c.bearer = signing + "." + b64(signature)
	c.bearerAt = now
	return c.bearer, nil
}

// signES256 produces the JWS form (R ‖ S, 32 bytes each), not ASN.1.
func signES256(key *ecdsa.PrivateKey, digest []byte) ([]byte, error) {
	r, s, err := ecdsa.Sign(rand.Reader, key, digest)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 64)
	r.FillBytes(out[:32])
	s.FillBytes(out[32:])
	return out, nil
}

func b64(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}

// ParseBearer splits a token into its signed part and signature, for tests
// and diagnostics.
func ParseBearer(bearer string) (signing string, signature []byte, err error) {
	dot := strings.LastIndex(bearer, ".")
	if dot < 0 {
		return "", nil, errors.New("not a jwt")
	}
	signature, err = base64.RawURLEncoding.DecodeString(bearer[dot+1:])
	return bearer[:dot], signature, err
}

var _ crypto.Signer = (*ecdsa.PrivateKey)(nil)
