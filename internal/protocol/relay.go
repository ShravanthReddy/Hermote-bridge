package protocol

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
)

// Relay transport (docs/REMOTE-ACCESS.md §6). The relay never sees plaintext:
// it matches one bridge with N phones by session id and shuttles opaque
// WebSocket messages between them. Everything below is the *framing* the
// bridge and relay agree on; the end-to-end protocol inside is unchanged.
//
// Bridge ⇄ relay (one WebSocket, multiplexed):
//
//	relay → bridge  text  {"t":"challenge","nonce":<b64url 32 bytes>}
//	bridge → relay  text  {"t":"attach","s":<session id>,"k":<bridge pub>,"sig":<Ed25519(labelRelayAttach ‖ nonce)>}
//	relay → bridge  text  {"t":"attached"}            (or a close with RelayCloseBadAttach)
//	then binary frames only:  channel(2, big-endian) ‖ kind(1) ‖ payload
//	  channel 0 is control (JSON payload): {"t":"open","c":N} / {"t":"close","c":N,"reason":…}
//	  channels ≥1 carry one phone each; kind 0 = the phone's text frame, 1 = binary frame.
//
// Phone ⇄ relay: plain WebSocket to /v1/phone?s=<session id>; frames pass
// through untouched. No bridge → close 4404; bridge full → close 4429.

const (
	labelRelayAttach = "hermes-remote v1 relay attach"

	// RelayControlChannel carries open/close messages between relay and bridge.
	RelayControlChannel uint16 = 0
	// RelayKindText / RelayKindBinary tag the phone's original message type.
	RelayKindText   byte = 0
	RelayKindBinary byte = 1
	// RelayHeaderSize is channel(2) + kind(1).
	RelayHeaderSize = 3

	// RelayMaxPhones caps concurrent phones per bridge session.
	RelayMaxPhones = 4

	// WebSocket close codes the relay uses toward phones and bridges.
	RelayCloseNoBridge  = 4404
	RelayCloseFull      = 4429
	RelayCloseBadAttach = 4401
	RelayCloseReplaced  = 4409
)

// RelayChallenge is the relay's first message to an attaching bridge.
type RelayChallenge struct {
	T     string `json:"t"` // "challenge"
	Nonce Bytes  `json:"nonce"`
}

// RelayAttach proves the bridge owns the session id it claims.
type RelayAttach struct {
	T   string `json:"t"` // "attach"
	S   string `json:"s"`
	K   Bytes  `json:"k"`
	Sig Bytes  `json:"sig"`
}

// RelayControl is a channel-0 message.
type RelayControl struct {
	T      string `json:"t"` // "open" | "close"
	C      uint16 `json:"c"`
	Reason string `json:"reason,omitempty"`
}

// SignRelayAttach signs the relay's nonce with the bridge identity.
func (i *Identity) SignRelayAttach(nonce []byte) []byte {
	return i.sign(labelRelayAttach, nonce)
}

// VerifyRelayAttach checks an attach message against the challenge nonce.
func VerifyRelayAttach(a RelayAttach, nonce []byte) error {
	if a.T != "attach" || len(a.K) != ed25519.PublicKeySize || len(nonce) != NonceSize {
		return errors.New("relay: malformed attach")
	}
	if SessionIDFor(a.K) != a.S {
		return errors.New("relay: session id does not match key")
	}
	if !verify(a.K, labelRelayAttach, nonce, a.Sig) {
		return errors.New("relay: attach signature invalid")
	}
	return nil
}

// RelayFrame builds a multiplexed frame.
func RelayFrame(channel uint16, kind byte, payload []byte) []byte {
	out := make([]byte, RelayHeaderSize+len(payload))
	binary.BigEndian.PutUint16(out, channel)
	out[2] = kind
	copy(out[RelayHeaderSize:], payload)
	return out
}

// ParseRelayFrame splits a multiplexed frame.
func ParseRelayFrame(frame []byte) (channel uint16, kind byte, payload []byte, err error) {
	if len(frame) < RelayHeaderSize {
		return 0, 0, nil, errors.New("relay: short frame")
	}
	return binary.BigEndian.Uint16(frame), frame[2], frame[RelayHeaderSize:], nil
}
