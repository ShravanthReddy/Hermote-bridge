package bridge

import (
	"crypto/subtle"
	"sync"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

// pairings tracks outstanding one-time pairing codes.
type pairings struct {
	mu    sync.Mutex
	codes map[string]time.Time // code → expiry
}

func newPairings() *pairings { return &pairings{codes: map[string]time.Time{}} }

// Issue creates a code valid for ttl.
func (p *pairings) Issue(ttl time.Duration) ([]byte, time.Time, error) {
	code, err := protocol.NewPairCode(nil)
	if err != nil {
		return nil, time.Time{}, err
	}
	// Keep the monotonic component internally so wall-clock adjustments cannot
	// extend a code. Only the caller-facing timestamp is converted to UTC.
	exp := time.Now().Add(ttl)
	p.mu.Lock()
	p.codes[string(code)] = exp
	p.mu.Unlock()
	return code, exp.UTC(), nil
}

// Outstanding returns the unexpired codes (and prunes expired ones).
func (p *pairings) Outstanding() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var out [][]byte
	for c, exp := range p.codes {
		if !now.Before(exp) {
			delete(p.codes, c)
			continue
		}
		out = append(out, []byte(c))
	}
	return out
}

// Consume atomically verifies that code still exists and has not expired, then
// retires it. Exactly one concurrent caller can succeed.
func (p *pairings) Consume(code []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	consumed := false
	for c, exp := range p.codes {
		if !now.Before(exp) {
			delete(p.codes, c)
			continue
		}
		if subtle.ConstantTimeCompare([]byte(c), code) == 1 {
			delete(p.codes, c)
			consumed = true
		}
	}
	return consumed
}

// Pending reports how many codes are outstanding.
func (p *pairings) Pending() int { return len(p.Outstanding()) }
