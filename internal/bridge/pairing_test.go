package bridge

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

func TestPairingConsumeIsAtomicAndSingleUse(t *testing.T) {
	p := newPairings()
	code, expiry, err := p.Issue(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if expiry.Location() != time.UTC {
		t.Fatalf("caller-facing expiry location = %v, want UTC", expiry.Location())
	}
	const contenders = 32
	var successes atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if p.Consume(code) {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful consumes = %d, want 1", got)
	}
	if p.Consume(code) {
		t.Fatal("already-consumed code succeeded")
	}
	if p.Consume([]byte("absent")) {
		t.Fatal("absent code succeeded")
	}
}

func TestPairingConsumeRechecksExpiry(t *testing.T) {
	p := newPairings()
	code, err := protocol.NewPairCode(nil)
	if err != nil {
		t.Fatal(err)
	}
	p.codes[string(code)] = time.Now().Add(-time.Nanosecond)
	if p.Consume(code) {
		t.Fatal("expired code succeeded")
	}
	if p.Pending() != 0 {
		t.Fatal("expired code was not pruned")
	}
}
