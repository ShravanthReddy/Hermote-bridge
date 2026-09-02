package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"
)

// Transport is how the phone reaches the bridge.
type Transport string

const (
	// TransportDirect: the phone connects straight to the bridge (over the tailnet).
	TransportDirect Transport = "direct"
	// TransportRelay: both sides connect to a relay that forwards ciphertext.
	TransportRelay Transport = "relay"
)

// PairCodeSize is the size of the one-time pairing code (128 bits).
const PairCodeSize = 16

// PairTTL is how long a printed QR code stays valid.
const PairTTL = 5 * time.Minute

// PairPayload is the content of the QR code / hermes://pair link.
type PairPayload struct {
	Version   int
	Transport Transport
	URL       string    // bridge WebSocket URL (direct) or relay phone endpoint (relay)
	SessionID string    // SessionIDFor(BridgeKey)
	BridgeKey []byte    // bridge Ed25519 public key, pinned by the phone
	Code      []byte    // one-time pairing code; proves the phone saw this QR
	Expires   time.Time // Code validity (unix seconds on the wire)
	Name      string    // human-readable Mac name shown after pairing
}

// NewPairCode draws a fresh one-time pairing code.
func NewPairCode(rng io.Reader) ([]byte, error) {
	if rng == nil {
		rng = rand.Reader
	}
	code := make([]byte, PairCodeSize)
	_, err := io.ReadFull(rng, code)
	return code, err
}

// String renders the hermes://pair?... URL. Query keys are deliberately short
// to keep the QR small (version 6–8 at medium error correction).
func (p PairPayload) String() string {
	q := url.Values{}
	q.Set("v", strconv.Itoa(p.Version))
	q.Set("t", string(p.Transport))
	q.Set("u", p.URL)
	q.Set("s", p.SessionID)
	q.Set("k", b64.EncodeToString(p.BridgeKey))
	q.Set("c", b64.EncodeToString(p.Code))
	q.Set("e", strconv.FormatInt(p.Expires.Unix(), 10))
	if p.Name != "" {
		q.Set("n", p.Name)
	}
	return "hermes://pair?" + q.Encode()
}

// ParsePair parses and validates a hermes://pair URL. It does not check
// expiry (callers decide how strict to be about clock skew).
func ParsePair(s string) (PairPayload, error) {
	u, err := url.Parse(s)
	if err != nil {
		return PairPayload{}, err
	}
	if u.Scheme != "hermes" || u.Host != "pair" {
		return PairPayload{}, errors.New("protocol: not a hermes://pair link")
	}
	q := u.Query()
	var p PairPayload
	if p.Version, err = strconv.Atoi(q.Get("v")); err != nil || p.Version != Version {
		return PairPayload{}, fmt.Errorf("protocol: unsupported pair version %q", q.Get("v"))
	}
	p.Transport = Transport(q.Get("t"))
	if p.Transport != TransportDirect && p.Transport != TransportRelay {
		return PairPayload{}, fmt.Errorf("protocol: unknown transport %q", q.Get("t"))
	}
	p.URL = q.Get("u")
	if wu, err := url.Parse(p.URL); err != nil || (wu.Scheme != "wss" && wu.Scheme != "ws") || wu.Host == "" {
		return PairPayload{}, errors.New("protocol: pair URL must be ws(s)://host")
	}
	if p.BridgeKey, err = b64.DecodeString(q.Get("k")); err != nil || len(p.BridgeKey) != ed25519.PublicKeySize {
		return PairPayload{}, errors.New("protocol: bad bridge key")
	}
	p.SessionID = q.Get("s")
	if p.SessionID != SessionIDFor(p.BridgeKey) {
		return PairPayload{}, errors.New("protocol: session id does not match bridge key")
	}
	if p.Code, err = b64.DecodeString(q.Get("c")); err != nil || len(p.Code) != PairCodeSize {
		return PairPayload{}, errors.New("protocol: bad pairing code")
	}
	exp, err := strconv.ParseInt(q.Get("e"), 10, 64)
	if err != nil {
		return PairPayload{}, errors.New("protocol: bad expiry")
	}
	p.Expires = time.Unix(exp, 0).UTC()
	p.Name = q.Get("n")
	return p, nil
}
