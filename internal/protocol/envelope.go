package protocol

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
)

// FrameVersion is the first byte of every envelope.
const FrameVersion byte = 0x01

const (
	keySize     = 32
	counterSize = 8
	headerSize  = 1 + counterSize
	gcmNonce    = 12
	gcmTag      = 16
)

// MaxPlaintext bounds a single envelope (the gateway's own WebSocket cap);
// larger payloads are chunked at the message layer.
const MaxPlaintext = 16 << 20

var (
	// ErrCounter is returned when a frame's counter is not the expected next value.
	ErrCounter = errors.New("protocol: envelope counter out of sequence")
	// ErrFrame is returned for malformed or unauthenticated frames.
	ErrFrame = errors.New("protocol: envelope malformed or authentication failed")
)

type cipherState struct {
	aead    cipher.AEAD
	counter uint64 // last counter used (send) or accepted (recv)
}

// Suite holds one connection's directional keys and counters. It is safe for
// concurrent use; each direction is independently serialised.
type Suite struct {
	sendMu sync.Mutex
	recvMu sync.Mutex
	send   cipherState
	recv   cipherState
}

// deriveSuite derives k_p2b ‖ k_b2p and assigns directions by role.
func deriveSuite(shared, transcript []byte, isBridge bool) (*Suite, error) {
	keys, err := hkdf.Key(sha256.New, shared, transcript, labelKeys, 2*keySize)
	if err != nil {
		return nil, err
	}
	p2b, err := newAEAD(keys[:keySize])
	if err != nil {
		return nil, err
	}
	b2p, err := newAEAD(keys[keySize:])
	if err != nil {
		return nil, err
	}
	s := &Suite{}
	if isBridge {
		s.send.aead, s.recv.aead = b2p, p2b
	} else {
		s.send.aead, s.recv.aead = p2b, b2p
	}
	return s, nil
}

// SuiteFromKeys builds a suite from explicit directional keys (test vectors).
func SuiteFromKeys(sendKey, recvKey []byte) (*Suite, error) {
	sk, err := newAEAD(sendKey)
	if err != nil {
		return nil, err
	}
	rk, err := newAEAD(recvKey)
	if err != nil {
		return nil, err
	}
	return &Suite{send: cipherState{aead: sk}, recv: cipherState{aead: rk}}, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != keySize {
		return nil, errors.New("protocol: key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func header(counter uint64) []byte {
	h := make([]byte, headerSize)
	h[0] = FrameVersion
	binary.BigEndian.PutUint64(h[1:], counter)
	return h
}

func nonceFor(counter uint64) []byte {
	n := make([]byte, gcmNonce)
	binary.BigEndian.PutUint64(n[4:], counter)
	return n
}

// Seal encrypts plaintext as the next outbound frame.
func (s *Suite) Seal(plaintext []byte) ([]byte, error) {
	if len(plaintext) > MaxPlaintext {
		return nil, errors.New("protocol: plaintext exceeds MaxPlaintext")
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.send.counter == ^uint64(0) {
		return nil, errors.New("protocol: send counter exhausted")
	}
	s.send.counter++
	hdr := header(s.send.counter)
	out := make([]byte, headerSize, headerSize+len(plaintext)+gcmTag)
	copy(out, hdr)
	return s.send.aead.Seal(out, nonceFor(s.send.counter), plaintext, hdr), nil
}

// Open authenticates and decrypts an inbound frame, enforcing the counter.
func (s *Suite) Open(frame []byte) ([]byte, error) {
	if len(frame) < headerSize+gcmTag || frame[0] != FrameVersion {
		return nil, ErrFrame
	}
	counter := binary.BigEndian.Uint64(frame[1:headerSize])
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	if counter != s.recv.counter+1 {
		return nil, ErrCounter
	}
	plain, err := s.recv.aead.Open(nil, nonceFor(counter), frame[headerSize:], frame[:headerSize])
	if err != nil {
		return nil, ErrFrame
	}
	s.recv.counter = counter
	return plain, nil
}
