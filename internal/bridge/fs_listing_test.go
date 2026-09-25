package bridge

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type folderListResult struct {
	status int
	body   []byte
	err    error
}

func folderListingDeps(timeout time.Duration, readDir readDirectory) bridgeDependencies {
	deps := productionBridgeDependencies()
	deps.fsResolvePath = func(path string) (string, error) { return path, nil }
	deps.fsReadDir = readDir
	deps.fsListTimeout = timeout
	deps.fsWarningAfter = 0
	return deps
}

func closeFolderRead(release chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(release) }) }
}

func listFolder(l *folderLister, ctx context.Context, query string, connSlots *workPool) folderListResult {
	status, body, err := l.list(ctx, query, connSlots)
	return folderListResult{status: status, body: body, err: err}
}

func startFolderList(
	l *folderLister, ctx context.Context, query string, connSlots *workPool,
) <-chan folderListResult {
	result := make(chan folderListResult, 1)
	go func() {
		status, body, err := l.list(ctx, query, connSlots)
		result <- folderListResult{status: status, body: body, err: err}
	}()
	return result
}

func awaitFolderList(t *testing.T, result <-chan folderListResult) folderListResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(time.Second):
		t.Fatal("folder listing did not return")
		return folderListResult{}
	}
}

func waitForFolderWaiters(t *testing.T, l *folderLister, key string, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		read := l.reads[key]
		ready := read != nil && read.waiters == count
		l.mu.Unlock()
		if ready {
			return
		}
		runtimeGosched()
	}
	t.Fatal("folder listing waiter count did not reach the expected value")
}

func waitForNoFolderReads(t *testing.T, l *folderLister) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		empty := len(l.reads) == 0
		l.mu.Unlock()
		if empty {
			return
		}
		runtimeGosched()
	}
	t.Fatal("finished folder read remained registered")
}

func runtimeGosched() {
	runtime.Gosched()
}

func folderQuery(path string) string {
	return "path=" + url.QueryEscape(path)
}

func TestFolderListingSharesAStuckRead(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var reads atomic.Int32
	deps := folderListingDeps(50*time.Millisecond, func(string) ([]os.DirEntry, error) {
		reads.Add(1)
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(8))
	connSlots := newWorkPool(2)
	query := folderQuery(t.TempDir())
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	firstResult := startFolderList(l, context.Background(), query, connSlots)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not start")
	}
	first := awaitFolderList(t, firstResult)
	if first.err != nil || decodeListing(t, first.body).Error != "ETIMEDOUT" {
		t.Fatalf("first result = status %d body %s err %v", first.status, first.body, first.err)
	}
	started := time.Now()
	second := startFolderList(l, context.Background(), query, connSlots)
	got := awaitFolderList(t, second)
	if got.err != nil || decodeListing(t, got.body).Error != "ETIMEDOUT" {
		t.Fatalf("second result = status %d body %s err %v", got.status, got.body, got.err)
	}
	if time.Since(started) >= 20*time.Millisecond {
		t.Fatalf("second timed-out listing took %s", time.Since(started))
	}
	if reads.Load() != 1 {
		t.Fatalf("filesystem reads = %d, want 1", reads.Load())
	}
}

func TestFolderListingHandsTheReadsAnswerToWaiters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var reads atomic.Int32
	deps := folderListingDeps(time.Second, func(string) ([]os.DirEntry, error) {
		reads.Add(1)
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(8))
	connSlots := newWorkPool(2)
	query := folderQuery(t.TempDir())
	key := fsFlightKey(query)
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	leader := startFolderList(l, context.Background(), query, connSlots)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not start")
	}
	waiter := startFolderList(l, context.Background(), query, connSlots)
	waitForFolderWaiters(t, l, key, 1)
	releaseRead()

	for i, result := range []folderListResult{awaitFolderList(t, leader), awaitFolderList(t, waiter)} {
		if result.err != nil || decodeListing(t, result.body).Error != "" {
			t.Fatalf("request %d result = status %d body %s err %v", i, result.status, result.body, result.err)
		}
	}
	if reads.Load() != 1 {
		t.Fatalf("filesystem reads = %d, want 1", reads.Load())
	}
}

func TestFolderListingReadsAfreshOnceTheReadEnds(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	var reads atomic.Int32
	deps := folderListingDeps(30*time.Millisecond, func(string) ([]os.DirEntry, error) {
		if reads.Add(1) == 1 {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(8))
	connSlots := newWorkPool(2)
	query := folderQuery(t.TempDir())
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	first := startFolderList(l, context.Background(), query, connSlots)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not start")
	}
	if got := decodeListing(t, awaitFolderList(t, first).body).Error; got != "ETIMEDOUT" {
		t.Fatalf("first listing error = %q, want ETIMEDOUT", got)
	}
	releaseRead()
	waitForNoFolderReads(t, l)
	second := listFolder(l, context.Background(), query, connSlots)
	if second.err != nil || decodeListing(t, second.body).Error != "" {
		t.Fatalf("listing after read ended = status %d body %s err %v", second.status, second.body, second.err)
	}
	if reads.Load() != 2 {
		t.Fatalf("filesystem reads = %d, want 2", reads.Load())
	}
}

func TestFolderListingKeepsFoldersApart(t *testing.T) {
	stuckPath := t.TempDir()
	release := make(chan struct{})
	var entered sync.Once
	stuck := make(chan struct{})
	deps := folderListingDeps(40*time.Millisecond, func(path string) ([]os.DirEntry, error) {
		if path == stuckPath {
			entered.Do(func() { close(stuck) })
			<-release
		}
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(8))
	connSlots := newWorkPool(2)
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	first := startFolderList(l, context.Background(), folderQuery(stuckPath), connSlots)
	select {
	case <-stuck:
	case <-time.After(time.Second):
		t.Fatal("stuck filesystem read did not start")
	}
	if got := decodeListing(t, awaitFolderList(t, first).body).Error; got != "ETIMEDOUT" {
		t.Fatalf("stuck folder error = %q", got)
	}
	healthy := listFolder(l, context.Background(), folderQuery(t.TempDir()), connSlots)
	if healthy.err != nil || decodeListing(t, healthy.body).Error != "" {
		t.Fatalf("healthy folder result = status %d body %s err %v", healthy.status, healthy.body, healthy.err)
	}
}

func TestFolderListingBusyAdmissionSharesNothing(t *testing.T) {
	var reads atomic.Int32
	deps := folderListingDeps(time.Second, func(string) ([]os.DirEntry, error) {
		reads.Add(1)
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(2))
	connSlots := newWorkPool(1)
	if !connSlots.tryAcquire() {
		t.Fatal("could not pre-fill connection pool")
	}
	query := folderQuery(t.TempDir())

	busy := listFolder(l, context.Background(), query, connSlots)
	if busy.err != nil || decodeListing(t, busy.body).Error != "EBUSY" {
		t.Fatalf("busy result = status %d body %s err %v", busy.status, busy.body, busy.err)
	}
	l.mu.Lock()
	registered := len(l.reads)
	l.mu.Unlock()
	if registered != 0 {
		t.Fatalf("busy admission registered %d reads", registered)
	}
	connSlots.release()
	result := listFolder(l, context.Background(), query, connSlots)
	if result.err != nil || decodeListing(t, result.body).Error != "" {
		t.Fatalf("listing after admission = status %d body %s err %v", result.status, result.body, result.err)
	}
	if reads.Load() != 1 {
		t.Fatalf("filesystem reads = %d, want 1", reads.Load())
	}
}

func TestFolderListingCancelledWaiterLeavesTheRead(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	deps := folderListingDeps(time.Second, func(string) ([]os.DirEntry, error) {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(4))
	connSlots := newWorkPool(2)
	query := folderQuery(t.TempDir())
	key := fsFlightKey(query)
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	leader := startFolderList(l, context.Background(), query, connSlots)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiter := startFolderList(l, ctx, query, connSlots)
	waitForFolderWaiters(t, l, key, 1)
	cancel()
	if got := awaitFolderList(t, waiter); got.err != context.Canceled {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", got.err)
	}
	l.mu.Lock()
	stillRegistered := l.reads[key] != nil
	l.mu.Unlock()
	if !stillRegistered {
		t.Fatal("canceling a waiter removed the shared read")
	}
	releaseRead()
	if got := awaitFolderList(t, leader); got.err != nil {
		t.Fatalf("leader error = %v", got.err)
	}
	waitForNoFolderReads(t, l)
}

func TestFolderListingReplacesAStaleRead(t *testing.T) {
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	entered := make(chan int, 2)
	var reads atomic.Int32
	deps := folderListingDeps(10*time.Millisecond, func(string) ([]os.DirEntry, error) {
		n := int(reads.Add(1))
		entered <- n
		if n == 1 {
			<-firstRelease
		} else if n == 2 {
			<-secondRelease
		}
		return nil, nil
	})
	deps.fsStaleAfter = 30 * time.Millisecond
	l := newFolderLister(deps, slog.Default(), newWorkPool(4))
	connSlots := newWorkPool(2)
	query := folderQuery(t.TempDir())
	key := fsFlightKey(query)
	releaseFirstRead := closeFolderRead(firstRelease)
	releaseSecondRead := closeFolderRead(secondRelease)
	defer releaseFirstRead()
	defer releaseSecondRead()

	first := startFolderList(l, context.Background(), query, connSlots)
	if got := awaitFolderList(t, first); got.err != nil || decodeListing(t, got.body).Error != "ETIMEDOUT" {
		t.Fatalf("first listing = status %d body %s err %v", got.status, got.body, got.err)
	}
	l.mu.Lock()
	firstRead := l.reads[key]
	l.mu.Unlock()
	if firstRead == nil {
		t.Fatal("timed-out read was not registered")
	}
	started := time.Now()
	timer := time.NewTimer(40 * time.Millisecond)
	<-timer.C
	if time.Since(started) < 40*time.Millisecond {
		t.Fatal("stale interval did not elapse")
	}
	second := startFolderList(l, context.Background(), query, connSlots)
	if n := awaitFolderSignal(t, entered); n != 1 {
		t.Fatalf("first read signal = %d", n)
	}
	if n := awaitFolderSignal(t, entered); n != 2 {
		t.Fatalf("second read signal = %d", n)
	}
	if got := awaitFolderList(t, second); got.err != nil || decodeListing(t, got.body).Error != "ETIMEDOUT" {
		t.Fatalf("replacement listing = status %d body %s err %v", got.status, got.body, got.err)
	}
	l.mu.Lock()
	secondRead := l.reads[key]
	l.mu.Unlock()
	if secondRead == nil || secondRead == firstRead {
		t.Fatal("stale read was not replaced")
	}

	releaseFirstRead()
	select {
	case <-firstRead.done:
	case <-time.After(time.Second):
		t.Fatal("old filesystem read did not settle")
	}
	l.mu.Lock()
	current := l.reads[key]
	l.mu.Unlock()
	if current != secondRead {
		t.Fatal("old read removed the replacement registration")
	}
	releaseSecondRead()
	waitForNoFolderReads(t, l)
}

func awaitFolderSignal(t *testing.T, signals <-chan int) int {
	t.Helper()
	select {
	case got := <-signals:
		return got
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not signal")
		return 0
	}
}

func TestFolderListingTimedOutReadReturnsTheConnectionSlot(t *testing.T) {
	stuckPath := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	deps := folderListingDeps(30*time.Millisecond, func(path string) ([]os.DirEntry, error) {
		if path == stuckPath {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return nil, nil
	})
	l := newFolderLister(deps, slog.Default(), newWorkPool(2))
	connSlots := newWorkPool(1)
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	first := startFolderList(l, context.Background(), folderQuery(stuckPath), connSlots)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stuck filesystem read did not start")
	}
	if got := awaitFolderList(t, first); got.err != nil || decodeListing(t, got.body).Error != "ETIMEDOUT" {
		t.Fatalf("timed-out listing = status %d body %s err %v", got.status, got.body, got.err)
	}
	if len(connSlots.slots) != 0 || len(l.processSlots.slots) != 1 {
		t.Fatalf("slots after timeout: connection=%d process=%d", len(connSlots.slots), len(l.processSlots.slots))
	}
	healthy := listFolder(l, context.Background(), folderQuery(t.TempDir()), connSlots)
	if healthy.err != nil || decodeListing(t, healthy.body).Error != "" {
		t.Fatalf("healthy listing = status %d body %s err %v", healthy.status, healthy.body, healthy.err)
	}
}

type folderLogRecord struct {
	message string
	attrs   []slog.Attr
}

type folderLogHandler struct {
	records chan folderLogRecord
}

func (h *folderLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *folderLogHandler) Handle(_ context.Context, record slog.Record) error {
	captured := folderLogRecord{message: record.Message}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs = append(captured.attrs, attr)
		return true
	})
	h.records <- captured
	return nil
}
func (h *folderLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *folderLogHandler) WithGroup(string) slog.Handler      { return h }

func TestFolderListingLogsAnOverdueRead(t *testing.T) {
	path := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	deps := folderListingDeps(30*time.Millisecond, func(string) ([]os.DirEntry, error) {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil, nil
	})
	handler := &folderLogHandler{records: make(chan folderLogRecord, 2)}
	l := newFolderLister(deps, slog.New(handler), newWorkPool(2))
	releaseRead := closeFolderRead(release)
	defer releaseRead()

	result := startFolderList(l, context.Background(), folderQuery(path), newWorkPool(1))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("filesystem read did not start")
	}
	if got := awaitFolderList(t, result); got.err != nil || decodeListing(t, got.body).Error != "ETIMEDOUT" {
		t.Fatalf("timed-out listing = status %d body %s err %v", got.status, got.body, got.err)
	}
	releaseRead()
	var record folderLogRecord
	select {
	case record = <-handler.records:
	case <-time.After(time.Second):
		t.Fatal("overdue filesystem read did not log its completion")
	}
	if record.message != "filesystem listing finished after its deadline" {
		t.Fatalf("log message = %q", record.message)
	}
	if len(record.attrs) != 1 || record.attrs[0].Key != "held" {
		t.Fatalf("log attributes = %+v, want only held", record.attrs)
	}
	if strings.Contains(record.attrs[0].Value.String(), path) {
		t.Fatalf("log attribute contains folder path: %+v", record.attrs[0])
	}
	select {
	case extra := <-handler.records:
		t.Fatalf("overdue read logged more than once: %+v", extra)
	default:
	}
}
