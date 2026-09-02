package protocol

import "encoding/json"

// Bytes marshals as unpadded base64url in JSON. Every binary field on the wire
// (keys, nonces, signatures, proofs) uses it so Swift's Data(base64URLEncoded:)
// round-trips without padding fixes.
type Bytes []byte

// MarshalJSON implements json.Marshaler.
func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(b64.EncodeToString(b))
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	raw, err := b64.DecodeString(s)
	if err != nil {
		return err
	}
	*b = raw
	return nil
}
