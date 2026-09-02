package protocol

// DeviceID is the stable textual id of a phone: base64url of its Ed25519
// public key. Used in devices.json and in CLI output.
func DeviceID(phonePub []byte) string { return b64.EncodeToString(phonePub) }

// DecodeDeviceID reverses DeviceID.
func DecodeDeviceID(id string) ([]byte, error) { return b64.DecodeString(id) }
