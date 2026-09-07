package bridge

import (
	"context"
	"os"
	"sync"
	"time"
)

const (
	defaultHTTPPerConnection = 8
	defaultHTTPProcessWide   = 32
	defaultFSPerConnection   = 2
	defaultFSProcessWide     = 8
	defaultHTTPAdmissionWait = 2 * time.Second
	defaultFSListTimeout     = 10 * time.Second
	defaultFSWarningAfter    = 2 * time.Minute
	defaultCloseWriteTimeout = 2 * time.Second
	defaultBlobMaxFileBytes  = int64(256 << 20)
	defaultBlobReservedBytes = int64(512 << 20)
	defaultBlobChunkBytes    = 512 << 10
	defaultBlobProcessWide   = 2
	defaultBlobIdleTimeout   = 120 * time.Second
	defaultBlobCommitTimeout = 120 * time.Second
)

type readDirectory func(string) ([]os.DirEntry, error)
type resolveFilesystemPath func(string) (string, error)

// bridgeDependencies are immutable after Server construction. Tests inject
// smaller pools and short durations without exposing production configuration.
type bridgeDependencies struct {
	httpPerConnection int
	httpProcessWide   int
	fsPerConnection   int
	fsProcessWide     int
	httpAdmissionWait time.Duration
	closeWriteTimeout time.Duration
	fsResolvePath     resolveFilesystemPath
	fsReadDir         readDirectory
	fsListTimeout     time.Duration
	fsWarningAfter    time.Duration
	blobSpoolRoot     string
	blobMaxFileBytes  int64
	blobReservedBytes int64
	blobChunkBytes    int
	blobProcessWide   int
	blobIdleTimeout   time.Duration
	blobCommitTimeout time.Duration
}

func productionBridgeDependencies() bridgeDependencies {
	return bridgeDependencies{
		httpPerConnection: defaultHTTPPerConnection,
		httpProcessWide:   defaultHTTPProcessWide,
		fsPerConnection:   defaultFSPerConnection,
		fsProcessWide:     defaultFSProcessWide,
		httpAdmissionWait: defaultHTTPAdmissionWait,
		closeWriteTimeout: defaultCloseWriteTimeout,
		fsResolvePath:     fsResolvePath,
		fsReadDir:         os.ReadDir,
		fsListTimeout:     defaultFSListTimeout,
		fsWarningAfter:    defaultFSWarningAfter,
		blobMaxFileBytes:  defaultBlobMaxFileBytes,
		blobReservedBytes: defaultBlobReservedBytes,
		blobChunkBytes:    defaultBlobChunkBytes,
		blobProcessWide:   defaultBlobProcessWide,
		blobIdleTimeout:   defaultBlobIdleTimeout,
		blobCommitTimeout: defaultBlobCommitTimeout,
	}
}

type workPool struct {
	slots chan struct{}
}

func newWorkPool(limit int) *workPool {
	if limit <= 0 {
		panic("bridge: work-pool limit must be positive")
	}
	return &workPool{slots: make(chan struct{}, limit)}
}

func (p *workPool) tryAcquire() bool {
	select {
	case p.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *workPool) acquire(ctx context.Context) bool {
	select {
	case p.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *workPool) release() {
	<-p.slots
}

type workLease struct {
	once  sync.Once
	pools []*workPool
}

func (l *workLease) release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		for i := len(l.pools) - 1; i >= 0; i-- {
			l.pools[i].release()
		}
	})
}

type admissionResult int

const (
	admissionGranted admissionResult = iota
	admissionBusy
	admissionCanceled
)

// acquirePools tries every pool in fixed order. A positive wait budget starts
// only when the first pool is unavailable, and that one deadline is shared by
// all remaining pools. Partial acquisition is always rolled back.
func acquirePools(parent context.Context, wait time.Duration, pools ...*workPool) (*workLease, admissionResult) {
	lease := &workLease{pools: make([]*workPool, 0, len(pools))}
	var waitCtx context.Context
	if checked, result := validateAdmission(parent, waitCtx, lease); result != admissionGranted {
		return checked, result
	}
	for _, pool := range pools {
		if waitCtx != nil {
			if checked, result := validateAdmission(parent, waitCtx, lease); result != admissionGranted {
				return checked, result
			}
		}
		if pool.tryAcquire() {
			lease.pools = append(lease.pools, pool)
			if checked, result := validateAdmission(parent, waitCtx, lease); result != admissionGranted {
				return checked, result
			}
			continue
		}
		if wait <= 0 {
			lease.release()
			if contextExpired(parent, time.Now()) {
				return nil, admissionCanceled
			}
			return nil, admissionBusy
		}
		if waitCtx == nil {
			var cancel context.CancelFunc
			waitCtx, cancel = context.WithTimeout(parent, wait)
			defer cancel()
		}
		if !pool.acquire(waitCtx) {
			lease.release()
			if contextExpired(parent, time.Now()) {
				return nil, admissionCanceled
			}
			return nil, admissionBusy
		}
		lease.pools = append(lease.pools, pool)
		if checked, result := validateAdmission(parent, waitCtx, lease); result != admissionGranted {
			return checked, result
		}
	}
	return validateAdmission(parent, waitCtx, lease)
}

// validateAdmission is the single success boundary for pool acquisition. It
// checks actual deadlines as well as Err because a context timer can be
// delivered after its deadline, and always gives parent cancellation
// precedence over admission timeout.
func validateAdmission(
	parent context.Context, waitCtx context.Context, lease *workLease,
) (*workLease, admissionResult) {
	now := time.Now()
	if contextExpired(parent, now) {
		lease.release()
		return nil, admissionCanceled
	}
	if waitCtx != nil && contextExpired(waitCtx, now) {
		lease.release()
		return nil, admissionBusy
	}
	return lease, admissionGranted
}

func contextExpired(ctx context.Context, now time.Time) bool {
	if ctx == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !deadline.After(now)
}
