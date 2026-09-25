package bridge

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"
)

// fsFlights coalesces listings of one folder. A read macOS does not finish
// (a Desktop or Documents folder waiting on iCloud or a privacy prompt) cannot
// be cancelled, so it keeps its capacity slot until the kernel lets go. Without
// coalescing, every tap on that folder took another slot, until listings of
// healthy folders answered EBUSY. A request for a folder already being read
// waits on that read instead and holds no slot.
type fsFlights struct {
	mu       sync.Mutex
	inflight map[string]*fsFlight
}

// fsFlight is one read in progress and, once it settles, its answer.
type fsFlight struct {
	done   chan struct{}
	status int
	body   []byte
}

func newFSFlights() *fsFlights {
	return &fsFlights{inflight: map[string]*fsFlight{}}
}

// listFolder answers one listing. The first request for a folder calls start,
// which must call settle exactly once when the underlying read ends, however
// long after its own answer that is; later requests for the same folder wait
// on that read, up to timeout, without calling start.
func (f *fsFlights) listFolder(
	ctx context.Context, rawQuery string, timeout time.Duration,
	start func(settle func(status int, body []byte)) (int, []byte, error),
) (int, []byte, error) {
	key := fsFlightKey(rawQuery)
	if key == "" {
		return start(func(int, []byte) {})
	}
	flight, leader := f.join(key)
	if !leader {
		return flight.wait(ctx, timeout)
	}
	var once sync.Once
	return start(func(status int, body []byte) {
		once.Do(func() { f.finish(key, flight, status, body) })
	})
}

// join returns the read of key already in flight, or registers a new one that
// the caller now leads.
func (f *fsFlights) join(key string) (*fsFlight, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if flight, ok := f.inflight[key]; ok {
		return flight, false
	}
	flight := &fsFlight{done: make(chan struct{})}
	f.inflight[key] = flight
	return flight, true
}

// finish records the read's answer, wakes its waiters and forgets the folder,
// so the next request reads it afresh.
func (f *fsFlights) finish(key string, flight *fsFlight, status int, body []byte) {
	f.mu.Lock()
	if f.inflight[key] == flight {
		delete(f.inflight, key)
	}
	f.mu.Unlock()
	flight.status, flight.body = status, body
	close(flight.done)
}

// wait answers with the read's own result, or ETIMEDOUT when the read is still
// stuck after timeout, as the leading request's listing does.
func (flight *fsFlight) wait(ctx context.Context, timeout time.Duration) (int, []byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-flight.done:
		return flight.status, flight.body, nil
	case <-timer.C:
		status, body := fsListingResponse(200, fsListing{Entries: []fsEntry{}, Error: "ETIMEDOUT"})
		return status, body, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// fsFlightKey is the requested path as the phone sent it; spellings that
// resolve to one folder read separately, which only costs a slot. Resolving
// here would touch the stuck folder before the deadline applies.
func fsFlightKey(rawQuery string) string {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(values.Get("path"))
}
