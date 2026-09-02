package protocol

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// NonceSize is the size of each side's handshake nonce.
const NonceSize = 32

// Hello is the phone's opening message (plaintext JSON text frame).
type Hello struct {
	Version   int    `json:"v"`
	SessionID string `json:"s"`
	PhoneID   Bytes  `json:"pid"` // phone Ed25519 public key
	PhoneEph  Bytes  `json:"pe"`  // phone X25519 ephemeral public key
	NonceP    Bytes  `json:"np"`
}

// Accept is the bridge's reply (plaintext JSON text frame).
type Accept struct {
	BridgeEph Bytes `json:"be"` // bridge X25519 ephemeral public key
	NonceB    Bytes `json:"nb"`
	Sig       Bytes `json:"sig"` // Ed25519(bridgeID, labelSigBridge ‖ transcript)
}

// Confirm is the phone's final message. It travels inside the first
// phone→bridge envelope, so the relay never sees the pairing proof.
type Confirm struct {
	Ch    string `json:"ch"`              // always "confirm"
	Sig   Bytes  `json:"sig"`             // Ed25519(phoneID, labelSigPhone ‖ transcript)
	Proof Bytes  `json:"proof,omitempty"` // HMAC-SHA256(code, labelPairProof ‖ transcript); unknown phones only
}

// ErrUntrustedPhone is returned when an unknown phone confirms without a valid
// pairing proof.
var ErrUntrustedPhone = errors.New("protocol: phone is not trusted and presented no valid pairing proof")

// ErrBadSignature is returned when a handshake signature does not verify.
var ErrBadSignature = errors.New("protocol: handshake signature invalid")

func lp(b []byte) []byte {
	out := make([]byte, 2+len(b))
	binary.BigEndian.PutUint16(out, uint16(len(b)))
	copy(out[2:], b)
	return out
}

// Transcript computes the handshake transcript hash (see package doc).
func Transcript(h Hello, bridgeID []byte, a Accept) []byte {
	sum := sha256.New()
	sum.Write([]byte(labelTranscript))
	sum.Write(lp([]byte{byte(h.Version)}))
	sum.Write(lp([]byte(h.SessionID)))
	sum.Write(lp(h.PhoneID))
	sum.Write(lp(h.PhoneEph))
	sum.Write(lp(h.NonceP))
	sum.Write(lp(bridgeID))
	sum.Write(lp(a.BridgeEph))
	sum.Write(lp(a.NonceB))
	return sum.Sum(nil)
}

// pairProof binds the one-time code to this exact handshake.
func pairProof(code, transcript []byte) []byte {
	mac := hmac.New(sha256.New, code)
	mac.Write([]byte(labelPairProof))
	mac.Write(transcript)
	return mac.Sum(nil)
}

// newEphemeral draws the 32 X25519 private-key bytes directly from rng.
// (ecdh.GenerateKey may consume an extra random byte — randutil.MaybeReadByte —
// which would make the golden vectors non-deterministic.)
func newEphemeral(rng io.Reader) (*ecdh.PrivateKey, error) {
	if rng == nil {
		rng = rand.Reader
	}
	seed := make([]byte, 32)
	if _, err := io.ReadFull(rng, seed); err != nil {
		return nil, err
	}
	return ecdh.X25519().NewPrivateKey(seed)
}

func newNonce(rng io.Reader) ([]byte, error) {
	if rng == nil {
		rng = rand.Reader
	}
	n := make([]byte, NonceSize)
	_, err := io.ReadFull(rng, n)
	return n, err
}

// ── Phone side ──────────────────────────────────────────────────────────────
// Implemented here so the Go tests generate the golden vectors the Swift
// client is checked against; the iOS app has its own CryptoKit implementation.

// PhoneState holds the phone's handshake secrets between Hello and Confirm.
type PhoneState struct {
	id    *Identity
	eph   *ecdh.PrivateKey
	hello Hello
}

// PhoneHello builds the opening message for the given session.
func PhoneHello(phoneID *Identity, sessionID string, rng io.Reader) (Hello, *PhoneState, error) {
	eph, err := newEphemeral(rng)
	if err != nil {
		return Hello{}, nil, err
	}
	nonce, err := newNonce(rng)
	if err != nil {
		return Hello{}, nil, err
	}
	h := Hello{
		Version:   Version,
		SessionID: sessionID,
		PhoneID:   phoneID.Public(),
		PhoneEph:  eph.PublicKey().Bytes(),
		NonceP:    nonce,
	}
	return h, &PhoneState{id: phoneID, eph: eph, hello: h}, nil
}

// Finish verifies the bridge's Accept against the pinned bridge key, derives
// the session keys and produces the encrypted Confirm frame. code may be nil
// for an already-trusted phone.
func (p *PhoneState) Finish(a Accept, bridgeID []byte, code []byte) (confirmFrame []byte, suite *Suite, err error) {
	transcript := Transcript(p.hello, bridgeID, a)
	if !verify(bridgeID, labelSigBridge, transcript, a.Sig) {
		return nil, nil, ErrBadSignature
	}
	bridgeEph, err := ecdh.X25519().NewPublicKey(a.BridgeEph)
	if err != nil {
		return nil, nil, err
	}
	shared, err := p.eph.ECDH(bridgeEph)
	if err != nil {
		return nil, nil, err
	}
	suite, err = deriveSuite(shared, transcript, false)
	if err != nil {
		return nil, nil, err
	}
	c := Confirm{Ch: "confirm", Sig: p.id.sign(labelSigPhone, transcript)}
	if code != nil {
		c.Proof = pairProof(code, transcript)
	}
	plain, err := json.Marshal(c)
	if err != nil {
		return nil, nil, err
	}
	confirmFrame, err = suite.Seal(plain)
	if err != nil {
		return nil, nil, err
	}
	return confirmFrame, suite, nil
}

// ── Bridge side ─────────────────────────────────────────────────────────────

// Pending holds the bridge's handshake secrets between Accept and Confirm.
type Pending struct {
	hello      Hello
	transcript []byte
	suite      *Suite
}

// PhoneID returns the public key the phone claimed in Hello.
func (p *Pending) PhoneID() []byte { return p.hello.PhoneID }

// BridgeAccept validates Hello, draws the bridge's ephemeral key and nonce,
// signs the transcript and derives the keys. The caller sends Accept and then
// feeds the phone's first envelope to Pending.Finish.
func BridgeAccept(bridgeID *Identity, h Hello, rng io.Reader) (Accept, *Pending, error) {
	if h.Version != Version {
		return Accept{}, nil, errors.New("protocol: unsupported hello version")
	}
	if h.SessionID != bridgeID.SessionID() {
		return Accept{}, nil, errors.New("protocol: hello is for a different session")
	}
	if len(h.PhoneID) != ed25519.PublicKeySize || len(h.NonceP) != NonceSize {
		return Accept{}, nil, errors.New("protocol: malformed hello")
	}
	phoneEph, err := ecdh.X25519().NewPublicKey(h.PhoneEph)
	if err != nil {
		return Accept{}, nil, err
	}
	eph, err := newEphemeral(rng)
	if err != nil {
		return Accept{}, nil, err
	}
	nonce, err := newNonce(rng)
	if err != nil {
		return Accept{}, nil, err
	}
	a := Accept{BridgeEph: eph.PublicKey().Bytes(), NonceB: nonce}
	transcript := Transcript(h, bridgeID.Public(), a)
	a.Sig = bridgeID.sign(labelSigBridge, transcript)
	shared, err := eph.ECDH(phoneEph)
	if err != nil {
		return Accept{}, nil, err
	}
	suite, err := deriveSuite(shared, transcript, true)
	if err != nil {
		return Accept{}, nil, err
	}
	return a, &Pending{hello: h, transcript: transcript, suite: suite}, nil
}

// Finish opens the phone's Confirm envelope and decides admission:
// trusted(phoneID) → admitted; otherwise the proof must match one of the
// outstanding pairing codes (validCode is called with each candidate's
// expected proof and returns true if it matches — the caller owns expiry and
// single-use bookkeeping). On success it returns the ready Suite.
func (p *Pending) Finish(confirmFrame []byte, trusted func(phoneID []byte) bool, codes [][]byte) (suite *Suite, usedCode []byte, err error) {
	plain, err := p.suite.Open(confirmFrame)
	if err != nil {
		return nil, nil, err
	}
	var c Confirm
	if err := json.Unmarshal(plain, &c); err != nil || c.Ch != "confirm" {
		return nil, nil, errors.New("protocol: first frame is not a confirm")
	}
	if !verify(p.hello.PhoneID, labelSigPhone, p.transcript, c.Sig) {
		return nil, nil, ErrBadSignature
	}
	if trusted != nil && trusted(p.hello.PhoneID) {
		return p.suite, nil, nil
	}
	if len(c.Proof) == sha256.Size {
		for _, code := range codes {
			if hmac.Equal(pairProof(code, p.transcript), c.Proof) {
				return p.suite, code, nil
			}
		}
	}
	return nil, nil, ErrUntrustedPhone
}
