package bridge

import (
	"crypto/subtle"
	"sync"
	"time"

	"github.com/shravanthreddy/hermes-ios/remote/internal/protocol"
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
	exp := time.Now().Add(ttl).UTC()
	p.mu.Lock()
	p.codes[string(code)] = exp
	p.mu.Unlock()
	return code, exp, nil
}

// Outstanding returns the unexpired codes (and prunes expired ones).
func (p *pairings) Outstanding() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var out [][]byte
	for c, exp := range p.codes {
		if now.After(exp) {
			delete(p.codes, c)
			continue
		}
		out = append(out, []byte(c))
	}
	return out
}

// Consume retires a code after a successful pairing (single use).
func (p *pairings) Consume(code []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.codes {
		if subtle.ConstantTimeCompare([]byte(c), code) == 1 {
			delete(p.codes, c)
		}
	}
}

// Pending reports how many codes are outstanding.
func (p *pairings) Pending() int { return len(p.Outstanding()) }
