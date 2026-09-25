package bridge

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// folderLister owns every phone's folder listings: admission against the
// connection's and the process's slots, one shared read per folder, and the
// read's settlement. A macOS folder read can block indefinitely, so a phone
// gives back its slot at the deadline while the process slot remains held.
type folderLister struct {
	deps         bridgeDependencies
	logger       *slog.Logger
	processSlots *workPool
	mu           sync.Mutex
	reads        map[string]*folderRead
	// abandoned is a stale read a retry replaced, kept until macOS returns, so an identical requested path holds at most two process slots.
	abandoned map[string]*folderRead
}

// folderRead is one read in progress and, once it settles, its answer.
type folderRead struct {
	started time.Time
	done    chan struct{}
	joins   int // requests that joined this read; guarded by folderLister.mu; tests wait on it
	status  int
	body    []byte
}

func newFolderLister(deps bridgeDependencies, logger *slog.Logger, processSlots *workPool) *folderLister {
	if logger == nil {
		logger = slog.Default()
	}
	return &folderLister{
		deps: deps, logger: logger, processSlots: processSlots,
		reads: map[string]*folderRead{}, abandoned: map[string]*folderRead{},
	}
}

// list answers one listing for a connection whose listing slots are connSlots.
func (l *folderLister) list(ctx context.Context, rawQuery string, connSlots *workPool) (int, []byte, error) {
	key := fsFlightKey(rawQuery)
	if key == "" {
		lease, result := acquirePools(ctx, 0, connSlots, l.processSlots)
		if result != admissionGranted {
			return l.admissionAnswer(ctx, result)
		}
		status, body, timedOut, err := fsListSettling(ctx, rawQuery, l.deps, lease, l.logger, func(int, []byte) {})
		if timedOut {
			lease.releasePool(connSlots)
		}
		return status, body, err
	}

	l.mu.Lock()
	var stale *folderRead
	if read := l.reads[key]; read != nil {
		if time.Since(read.started) < l.deps.fsStaleAfter {
			read.joins++
			remaining := l.deps.fsListTimeout - time.Since(read.started)
			l.mu.Unlock()
			return read.wait(ctx, remaining)
		}
		if l.abandoned[key] != nil {
			l.mu.Unlock()
			status, body := fsListingResponse(http.StatusOK, fsListing{Entries: []fsEntry{}, Error: "ETIMEDOUT"})
			return status, body, nil
		}
		stale = read
	}

	lease, result := acquirePools(ctx, 0, connSlots, l.processSlots)
	if result != admissionGranted {
		l.mu.Unlock()
		return l.admissionAnswer(ctx, result)
	}
	read := &folderRead{started: time.Now(), done: make(chan struct{})}
	if stale != nil {
		l.abandoned[key] = stale
	}
	l.reads[key] = read
	l.mu.Unlock()

	status, body, timedOut, err := fsListSettling(
		ctx, rawQuery, l.deps, lease, l.logger,
		func(status int, body []byte) { l.finish(key, read, status, body) },
	)
	if timedOut {
		lease.releasePool(connSlots)
	}
	return status, body, err
}

func (l *folderLister) admissionAnswer(ctx context.Context, result admissionResult) (int, []byte, error) {
	switch result {
	case admissionCanceled:
		err := ctx.Err()
		if err == nil {
			// contextExpired in limits.go detects elapsed deadlines before ctx.Err is set.
			err = context.DeadlineExceeded
		}
		return 0, nil, err
	case admissionBusy:
		status, body := fsListingResponse(http.StatusOK, fsListing{
			Entries: []fsEntry{}, Error: "EBUSY", Detail: "too many filesystem listings",
		})
		return status, body, nil
	default:
		panic("bridge: invalid filesystem admission result")
	}
}

func (l *folderLister) finish(key string, read *folderRead, status int, body []byte) {
	l.mu.Lock()
	if l.reads[key] == read {
		delete(l.reads, key)
	}
	if l.abandoned[key] == read {
		delete(l.abandoned, key)
	}
	l.mu.Unlock()
	read.status, read.body = status, body
	close(read.done)
	if time.Since(read.started) > l.deps.fsListTimeout {
		l.logger.Info("filesystem listing finished after its deadline", "held", time.Since(read.started))
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

func (read *folderRead) wait(ctx context.Context, remaining time.Duration) (int, []byte, error) {
	if remaining <= 0 {
		status, body := fsListingResponse(http.StatusOK, fsListing{Entries: []fsEntry{}, Error: "ETIMEDOUT"})
		return status, body, nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-read.done:
		return read.status, read.body, nil
	case <-timer.C:
		status, body := fsListingResponse(http.StatusOK, fsListing{Entries: []fsEntry{}, Error: "ETIMEDOUT"})
		return status, body, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}
