package protocol

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
)

// Test-only helpers exposing the raw derivation steps for the vector file.

func ecdhPublic(b []byte) (*ecdh.PublicKey, error) { return ecdh.X25519().NewPublicKey(b) }

func hkdfKeys(shared, transcript []byte) ([]byte, error) {
	return hkdf.Key(sha256.New, shared, transcript, labelKeys, 2*keySize)
}
