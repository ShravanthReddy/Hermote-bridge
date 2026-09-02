// Package protocol implements the hermes-remote wire protocol shared by the
// bridge, the relay and (as golden test vectors) the iOS client:
//
//   - identities: Ed25519 key pairs for the bridge and each phone, and the
//     session id derived from the bridge's public key;
//   - the hermes://pair QR payload;
//   - the handshake: X25519 ephemeral agreement signed by both identities,
//     HKDF-SHA256 key derivation, pairing-code proof for unknown phones;
//   - the envelope: AES-256-GCM frames with strictly sequential counters;
//   - the plaintext channel messages (ws / http / ctl / chunk).
//
// Everything here is standard-library cryptography. The design is described in
// docs/REMOTE-ACCESS.md §3–§4; the byte layouts below are the normative source
// and are asserted by Fixtures/e2ee-vectors.json on both platforms.
//
// # Transcript
//
// transcript = SHA-256( "hermes-remote v1 transcript"
//
//	‖ lp(version) ‖ lp(sessionID) ‖ lp(phoneID) ‖ lp(phoneEph) ‖ lp(nonceP)
//	‖ lp(bridgeID) ‖ lp(bridgeEph) ‖ lp(nonceB) )
//
// where lp(x) is a 2-byte big-endian length followed by x, and version is one
// byte. Signatures are over label ‖ transcript with per-role labels so a
// bridge signature can never be replayed as a phone signature.
//
// # Keys
//
// shared = X25519(phoneEph, bridgeEph)
// keys   = HKDF-SHA256(ikm=shared, salt=transcript, info="hermes-remote v1 keys", 64 bytes)
// k_p2b  = keys[0:32] (phone → bridge), k_b2p = keys[32:64] (bridge → phone)
//
// # Envelope
//
// frame = 0x01 ‖ counter(8, big-endian) ‖ AES-256-GCM(key_dir, nonce = 0x00000000 ‖ counter,
// aad = 0x01 ‖ counter, plaintext) with the 16-byte tag appended (Go and CryptoKit
// both produce ciphertext ‖ tag). Counters start at 1 and must be exactly the
// previous counter + 1; anything else closes the connection.
package protocol
