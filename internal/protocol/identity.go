package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
)

// Version is the protocol version carried in the pair payload and bound into
// every handshake transcript.
const Version = 1

const (
	labelTranscript = "hermes-remote v1 transcript"
	labelKeys       = "hermes-remote v1 keys"
	labelSigBridge  = "hermes-remote v1 bridge"
	labelSigPhone   = "hermes-remote v1 phone"
	labelPairProof  = "hermes-remote v1 pair"
)

// SessionIDLength is the number of base64url characters in a session id.
const SessionIDLength = 22

var b64 = base64.RawURLEncoding

// Identity is a long-lived Ed25519 signing key. It only ever signs handshake
// transcripts; it never encrypts.
type Identity struct {
	priv ed25519.PrivateKey
}

// NewIdentity generates a fresh identity from rng (crypto/rand when nil).
func NewIdentity(rng io.Reader) (*Identity, error) {
	if rng == nil {
		rng = rand.Reader
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(rng, seed); err != nil {
		return nil, err
	}
	return IdentityFromSeed(seed)
}

// IdentityFromSeed rebuilds an identity from its 32-byte seed (the persisted form).
func IdentityFromSeed(seed []byte) (*Identity, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, errors.New("protocol: identity seed must be 32 bytes")
	}
	return &Identity{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// Seed returns the 32-byte seed to persist (file mode 0600 / Keychain).
func (i *Identity) Seed() []byte { return i.priv.Seed() }

// Public returns the 32-byte Ed25519 public key.
func (i *Identity) Public() []byte {
	return []byte(i.priv.Public().(ed25519.PublicKey))
}

// SessionID returns the session id derived from this identity's public key.
func (i *Identity) SessionID() string { return SessionIDFor(i.Public()) }

// SessionIDFor derives a session id: base64url(SHA-256(pub))[:22]. It is
// stable per Mac, lets a phone find its Mac on a relay, and reveals nothing
// about the key beyond a hash.
func SessionIDFor(pub []byte) string {
	sum := sha256.Sum256(pub)
	return b64.EncodeToString(sum[:])[:SessionIDLength]
}

func (i *Identity) sign(label string, transcript []byte) []byte {
	msg := make([]byte, 0, len(label)+len(transcript))
	msg = append(msg, label...)
	msg = append(msg, transcript...)
	return ed25519.Sign(i.priv, msg)
}

func verify(pub []byte, label string, transcript, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize {
		return false
	}
	msg := make([]byte, 0, len(label)+len(transcript))
	msg = append(msg, label...)
	msg = append(msg, transcript...)
	return ed25519.Verify(ed25519.PublicKey(pub), msg, sig)
}
