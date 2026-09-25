package bridge

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flightTestDeps reads a folder through readDir, giving up after timeout.
func flightTestDeps(timeout time.Duration, readDir readDirectory) bridgeDependencies {
	deps := productionBridgeDependencies()
	deps.fsReadDir = readDir
	deps.fsListTimeout = timeout
	deps.fsWarningAfter = 0
	return deps
}

// listThrough answers one listing via flights, counting the reads it starts.
func listThrough(
	t *testing.T, flights *fsFlights, deps bridgeDependencies, query string, reads *atomic.Int32,
) fsListing {
	t.Helper()
	_, body, err := flights.listFolder(context.Background(), query, deps.fsListTimeout,
		func(settle func(int, []byte)) (int, []byte, error) {
			reads.Add(1)
			return fsListSettling(context.Background(), query, deps, nil, slog.Default(), settle)
		})
	if err != nil {
		t.Fatalf("listing failed: %v", err)
	}
	return decodeListing(t, body)
}

func waitForNoFlights(t *testing.T, flights *fsFlights) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		flights.mu.Lock()
		empty := len(flights.inflight) == 0
		flights.mu.Unlock()
		if empty {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the finished read was never forgotten")
}

func TestFSFlightsWaitOnAStuckReadInsteadOfStartingAnother(t *testing.T) {
	release := make(chan struct{})
	deps := flightTestDeps(50*time.Millisecond, func(string) ([]os.DirEntry, error) {
		<-release
		return nil, nil
	})
	flights := newFSFlights()
	query := "path=" + url.QueryEscape(t.TempDir())
	var reads atomic.Int32

	if got := listThrough(t, flights, deps, query, &reads).Error; got != "ETIMEDOUT" {
		t.Fatalf("first listing error = %q, want ETIMEDOUT", got)
	}
	started := time.Now()
	if got := listThrough(t, flights, deps, query, &reads).Error; got != "ETIMEDOUT" {
		t.Fatalf("second listing error = %q, want ETIMEDOUT", got)
	}
	if reads.Load() != 1 {
		t.Fatalf("a second read started on the stuck folder (%d reads)", reads.Load())
	}
	if time.Since(started) > 150*time.Millisecond {
		t.Fatal("the waiting listing did not give up at the deadline")
	}

	// Once macOS lets go the folder is read afresh.
	close(release)
	waitForNoFlights(t, flights)
	if got := listThrough(t, flights, deps, query, &reads).Error; got != "" {
		t.Fatalf("listing after the read finished error = %q", got)
	}
	if reads.Load() != 2 {
		t.Fatalf("reads = %d, want a fresh read after the stuck one finished", reads.Load())
	}
}

func TestFSFlightsHandTheReadsAnswerToWaiters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	deps := flightTestDeps(time.Second, func(string) ([]os.DirEntry, error) {
		close(entered)
		<-release
		return nil, nil
	})
	flights := newFSFlights()
	query := "path=" + url.QueryEscape(t.TempDir())
	var reads atomic.Int32

	var wg sync.WaitGroup
	results := make([]fsListing, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0] = listThrough(t, flights, deps, query, &reads)
	}()
	<-entered
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[1] = listThrough(t, flights, deps, query, &reads)
	}()
	// Let the second request join before the read answers.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if reads.Load() != 1 {
		t.Fatalf("reads = %d, want the two requests to share one", reads.Load())
	}
	for i, listing := range results {
		if listing.Error != "" {
			t.Fatalf("request %d error = %q", i, listing.Error)
		}
	}
}

func TestFSFlightsKeepDifferentFoldersApart(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	stuck := t.TempDir()
	deps := flightTestDeps(50*time.Millisecond, func(dir string) ([]os.DirEntry, error) {
		if dir == stuck {
			<-release
		}
		return nil, nil
	})
	flights := newFSFlights()
	var reads atomic.Int32
	resolvedStuck, _ := fsResolvePath(stuck)
	stuck = resolvedStuck

	if got := listThrough(t, flights, deps, "path="+url.QueryEscape(stuck), &reads).Error; got != "ETIMEDOUT" {
		t.Fatalf("stuck folder error = %q", got)
	}
	if got := listThrough(t, flights, deps, "path="+url.QueryEscape(t.TempDir()), &reads).Error; got != "" {
		t.Fatalf("a healthy folder waited on the stuck one: error = %q", got)
	}
}
