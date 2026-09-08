package bridge

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

func decodeBlobBase(raw []byte) (map[string]json.RawMessage, string, string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, "", "", errors.New("malformed blob message")
	}
	ch, err := boundedJSONString(fields["ch"], 16)
	if err != nil || ch != protocol.ChBlob {
		return fields, "", "", errors.New("invalid blob channel")
	}
	id, idErr := boundedJSONString(fields["id"], 64)
	if idErr != nil {
		return fields, "", "", errors.New("invalid blob id")
	}
	op, opErr := boundedJSONString(fields["op"], 16)
	if opErr != nil {
		return fields, id, "", errors.New("invalid blob operation")
	}
	return fields, id, op, nil
}

func exactBlobFields(fields map[string]json.RawMessage, allowed ...string) error {
	if len(fields) != len(allowed) {
		return errors.New("blob operation has missing or unexpected fields")
	}
	set := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		set[field] = struct{}{}
	}
	for field := range fields {
		if _, ok := set[field]; !ok {
			return errors.New("blob operation has an unexpected field")
		}
	}
	return nil
}

func boundedJSONString(raw json.RawMessage, limit int) (string, error) {
	if len(raw) == 0 || len(raw) > limit*6+2 {
		return "", errors.New("invalid bounded string")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !utf8.ValidString(value) || len(value) > limit {
		return "", errors.New("invalid bounded string")
	}
	return value, nil
}

func strictNonnegativeInt(raw json.RawMessage) (int64, error) {
	text := string(raw)
	if text == "" || (len(text) > 1 && text[0] == '0') {
		return 0, errors.New("invalid integer")
	}
	for _, c := range text {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid integer")
		}
	}
	return strconv.ParseInt(text, 10, 64)
}

func encodedChunkLimit(rawBytes int) int { return (rawBytes*8 + 5) / 6 }

func validAttachmentName(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 255 &&
		!strings.ContainsAny(name, `/\`) && !containsControl(name)
}

func validOriginalAttachmentName(name string) bool {
	return name != "" && name != "." && name != ".." && len(name) <= 255 &&
		!strings.ContainsAny(name, `/\`) && !strings.ContainsRune(name, '\x00')
}

func containsControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func isLowerHexDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isCanonicalUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
	}
	return true
}

func randomUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	text := hex.EncodeToString(value[:])
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:], nil
}

func boundedMessage(message string) string {
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, message)
	if len(message) <= 512 {
		return message
	}
	message = message[:512]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

// parseFileToken accepts exactly one Python-compatible @file reference and
// returns its unquoted value. The gateway's ref_path, rather than its absolute
// storage path, is the authoritative value inside ref_text.
func parseFileToken(value string) (string, bool) {
	if !strings.HasPrefix(value, "@file:") || containsControl(value) {
		return "", false
	}
	ref := strings.TrimPrefix(value, "@file:")
	if ref == "" {
		return "", false
	}
	first := ref[0]
	if first == '`' || first == '"' || first == '\'' {
		closing := strings.IndexByte(ref[1:], first) + 1
		// The ordered Python/Swift regex chooses a nonempty quoted match
		// when possible. An early closing delimiter would truncate the token.
		if closing > 1 {
			if closing != len(ref)-1 {
				return "", false
			}
			return ref[1:closing], true
		}
		// No closing delimiter (or an empty quoted alternative) falls back
		// to the same bare alternative used by the actual scanners.
	}
	// Python and the desktop/Swift scanner use a bare \S+ alternative.
	// The gateway falls back to it when a path contains all three quote
	// delimiters. Receipt validation still requires this exact parsed value
	// to equal the authoritative ref_path.
	for _, r := range ref {
		if unicode.IsSpace(r) {
			return "", false
		}
	}
	return ref, true
}
