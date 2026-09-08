package state

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

func testPhoneID(n byte) []byte { return bytes.Repeat([]byte{n}, 32) }

func openTestStore(t *testing.T, dir string, opts storeOptions) *Store {
	t.Helper()
	s, err := openAt(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Store: %v", err)
		}
	})
	return s
}

func TestConcurrentStoresPreserveDeviceTransactions(t *testing.T) {
	dir := t.TempDir()
	a := openTestStore(t, dir, storeOptions{})
	b := openTestStore(t, dir, storeOptions{})

	const count = 24
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 0 {
				store = b
			}
			errs <- store.AddTrusted(testPhoneID(byte(i)), fmt.Sprintf("phone-%d", i))
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	devices, err := a.Devices()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != count {
		t.Fatalf("got %d devices after concurrent transactions, want %d", len(devices), count)
	}
	duplicate := testPhoneID(1)
	var firstSeen time.Time
	for _, device := range devices {
		if device.ID == protocol.DeviceID(duplicate) {
			firstSeen = device.FirstSeen
			break
		}
	}
	if firstSeen.IsZero() {
		t.Fatal("duplicate-test device missing before update")
	}
	start := make(chan struct{})
	duplicateErrs := make(chan error, 2)
	var duplicateWG sync.WaitGroup
	for _, store := range []*Store{a, b} {
		duplicateWG.Add(1)
		go func(store *Store) {
			defer duplicateWG.Done()
			<-start
			if err := store.AddTrusted(duplicate, "updated"); err != nil {
				duplicateErrs <- err
			}
		}(store)
	}
	close(start)
	duplicateWG.Wait()
	select {
	case err := <-duplicateErrs:
		t.Fatal(err)
	default:
	}
	devices, err = b.Devices()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != count {
		t.Fatalf("duplicate trust created another record: %+v", devices)
	}
	for _, device := range devices {
		if device.ID == protocol.DeviceID(duplicate) && !device.FirstSeen.Equal(firstSeen) {
			t.Fatal("duplicate trust changed first_seen")
		}
	}

	raw, err := os.ReadFile(a.Path("devices.json"))
	if err != nil || !json.Valid(raw) {
		t.Fatalf("devices.json is not valid JSON: %v: %q", err, raw)
	}
	info, err := os.Stat(a.Path("devices.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("devices.json mode = %o, want 600", got)
	}
	leftovers, err := filepath.Glob(filepath.Join(dir, ".devices.json.tmp-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files left after writes: %v, err %v", leftovers, err)
	}
}

func TestTrustAndLastSeenCannotResurrectRevokedDevice(t *testing.T) {
	dir := t.TempDir()
	a := openTestStore(t, dir, storeOptions{})
	b := openTestStore(t, dir, storeOptions{})
	phone := testPhoneID(0xA1)
	id := protocol.DeviceID(phone)

	for i := 0; i < 20; i++ {
		if err := a.AddTrusted(phone, "before"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = b.IsTrusted(phone)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = a.Revoke(id)
		}()
		close(start)
		wg.Wait()
		devices, err := b.Devices()
		if err != nil {
			t.Fatal(err)
		}
		if len(devices) != 0 {
			t.Fatalf("last-seen race restored revoked device: %+v", devices)
		}

		if err := a.AddTrusted(phone, "before"); err != nil {
			t.Fatal(err)
		}
		start = make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = b.Trust(phone, "late name")
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = a.Revoke(id)
		}()
		close(start)
		wg.Wait()
		devices, err = a.Devices()
		if err != nil {
			t.Fatal(err)
		}
		if len(devices) != 0 {
			t.Fatalf("name-update race restored revoked device: %+v", devices)
		}
	}
	if err := b.Trust(phone, "cannot return"); err == nil {
		t.Fatal("updating an absent device should fail")
	}
}

func TestCorruptDevicesFailClosedAndRemainUntouched(t *testing.T) {
	s := openTestStore(t, t.TempDir(), storeOptions{})
	corrupt := []byte("{not-json\n")
	if err := os.WriteFile(s.Path("devices.json"), corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	phone := testPhoneID(0xB2)
	if s.IsTrusted(phone) {
		t.Fatal("corrupt trust state admitted a device")
	}
	if err := s.AddTrusted(phone, "new"); err == nil {
		t.Fatal("AddTrusted replaced corrupt state")
	}
	if err := s.Trust(phone, "rename"); err == nil {
		t.Fatal("Trust replaced corrupt state")
	}
	if _, err := s.Revoke("anything"); err == nil {
		t.Fatal("Revoke replaced corrupt state")
	}
	got, err := os.ReadFile(s.Path("devices.json"))
	if err != nil || !bytes.Equal(got, corrupt) {
		t.Fatalf("corrupt state changed: err=%v got=%q", err, got)
	}
}

func TestWriteFailuresFailClosedWithoutReplacingState(t *testing.T) {
	dir := t.TempDir()
	seed := openTestStore(t, dir, storeOptions{})
	phone := testPhoneID(0xC3)
	if err := seed.AddTrusted(phone, "seed"); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(seed.Path("devices.json"))
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected write failure")
	failing := openTestStore(t, dir, storeOptions{writeFile: func(string, []byte, os.FileMode) (bool, error) {
		return false, wantErr
	}})
	if failing.IsTrusted(phone) {
		t.Fatal("last-seen write failure must fail admission")
	}
	if err := failing.Trust(phone, "changed"); !errors.Is(err, wantErr) || errors.Is(err, ErrMutationCommitted) {
		t.Fatalf("Trust error = %v, want injected failure", err)
	}
	if err := failing.AddTrusted(testPhoneID(0xC4), "new"); !errors.Is(err, wantErr) || errors.Is(err, ErrMutationCommitted) {
		t.Fatalf("AddTrusted error = %v, want injected failure", err)
	}
	if removed, err := failing.Revoke(protocol.DeviceID(phone)); !errors.Is(err, wantErr) || errors.Is(err, ErrMutationCommitted) || removed != (Device{}) {
		t.Fatalf("Revoke result = %+v, %v; want zero device and ordinary injected failure", removed, err)
	}
	got, err := os.ReadFile(seed.Path("devices.json"))
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("failed write changed devices.json: err=%v got=%q", err, got)
	}
	if err := failing.SaveConfig(Config{Name: "should fail"}); !errors.Is(err, wantErr) || errors.Is(err, ErrMutationCommitted) {
		t.Fatalf("SaveConfig error = %v, want injected failure", err)
	}
}

func TestPostRenameDirectorySyncFailureReportsCommittedMutation(t *testing.T) {
	wantErr := errors.New("injected directory sync failure")
	s := openTestStore(t, t.TempDir(), storeOptions{
		syncDirectory: func(string) error { return wantErr },
	})
	id, err := s.Identity()
	if id == nil || !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("Identity result = %v, %v; want identity and committed diagnostic", id, err)
	}
	reloadedID, err := s.Identity()
	if err != nil || !bytes.Equal(id.Public(), reloadedID.Public()) {
		t.Fatalf("committed identity was not stable: %v", err)
	}
	err = s.SaveConfig(Config{Name: "committed config"})
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("SaveConfig error = %v, want committed classification and sync cause", err)
	}
	config, err := s.Config()
	if err != nil || config.Name != "committed config" {
		t.Fatalf("committed config = %+v, %v", config, err)
	}
	phone := testPhoneID(0xC5)
	err = s.AddTrusted(phone, "committed")
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("AddTrusted error = %v, want committed classification and sync cause", err)
	}
	devices, err := s.Devices()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 || devices[0].ID != protocol.DeviceID(phone) || devices[0].Name != "committed" {
		t.Fatalf("committed AddTrusted record = %+v", devices)
	}
	if s.IsTrusted(phone) {
		t.Fatal("post-commit last-seen diagnostic authorized the phone")
	}
	err = s.Trust(phone, "committed rename")
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("Trust error = %v, want committed classification and sync cause", err)
	}
	devices, err = s.Devices()
	if err != nil || len(devices) != 1 || devices[0].Name != "committed rename" {
		t.Fatalf("committed Trust update = %+v, %v", devices, err)
	}
	removed, err := s.Revoke(protocol.DeviceID(phone))
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("Revoke error = %v, want committed classification and sync cause", err)
	}
	if removed.ID != protocol.DeviceID(phone) {
		t.Fatalf("committed Revoke returned %+v", removed)
	}
	devices, readErr := s.Devices()
	if readErr != nil || len(devices) != 0 {
		t.Fatalf("devices after committed Revoke = %+v, %v", devices, readErr)
	}
}

func TestUnlockFailureClassifiesOnlyCommittedMutationAndReleasesFlock(t *testing.T) {
	dir := t.TempDir()
	wantErr := errors.New("injected post-unlock diagnostic")
	failing := openTestStore(t, dir, storeOptions{
		releaseLock: func(file *os.File) error {
			if err := releaseFileLock(file); err != nil {
				return err
			}
			return wantErr
		},
	})
	observer := openTestStore(t, dir, storeOptions{})
	phone := testPhoneID(0xC6)
	err := failing.AddTrusted(phone, "unlock")
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, wantErr) {
		t.Fatalf("mutation error = %v, want committed classification and unlock cause", err)
	}
	devices, err := observer.Devices()
	if err != nil || len(devices) != 1 || devices[0].ID != protocol.DeviceID(phone) {
		t.Fatalf("separate Store after injected unlock = %+v, %v", devices, err)
	}
	devices, err = failing.Devices()
	if !errors.Is(err, wantErr) || errors.Is(err, ErrMutationCommitted) {
		t.Fatalf("read-only unlock error = %v, want ordinary unlock cause", err)
	}
	if len(devices) != 1 {
		t.Fatalf("read-only result lost before unlock diagnostic: %+v", devices)
	}
}

func TestCommittedMutationJoinsDurabilityAndUnlockDiagnostics(t *testing.T) {
	dir := t.TempDir()
	syncErr := errors.New("injected durability diagnostic")
	unlockErr := errors.New("injected unlock diagnostic")
	s := openTestStore(t, dir, storeOptions{
		syncDirectory: func(string) error { return syncErr },
		releaseLock: func(file *os.File) error {
			if err := releaseFileLock(file); err != nil {
				return err
			}
			return unlockErr
		},
	})
	phone := testPhoneID(0xC7)
	err := s.AddTrusted(phone, "joined")
	if !errors.Is(err, ErrMutationCommitted) || !errors.Is(err, syncErr) || !errors.Is(err, unlockErr) {
		t.Fatalf("joined post-commit diagnostics = %v", err)
	}
	observer := openTestStore(t, dir, storeOptions{})
	devices, readErr := observer.Devices()
	if readErr != nil || len(devices) != 1 || devices[0].ID != protocol.DeviceID(phone) {
		t.Fatalf("committed record after joined diagnostics = %+v, %v", devices, readErr)
	}
}

func TestSameStoreGateWaitUsesBoundedDeadline(t *testing.T) {
	s := openTestStore(t, t.TempDir(), storeOptions{lockTimeout: 40 * time.Millisecond})
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	// Registered after openTestStore's Close cleanup so LIFO cleanup unblocks
	// the active transaction before Close tries to acquire the gate.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	blockingResult := make(chan error, 1)
	go func() {
		blockingResult <- s.locked(func() (bool, error) {
			close(entered)
			<-release
			return false, nil
		})
	}()
	waitSignal(t, entered, time.Second, "blocking transaction")
	if _, err := s.Devices(); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("same-Store gate error = %v, want ErrLockTimeout", err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := waitError(t, blockingResult, time.Second, "blocking transaction result"); err != nil {
		t.Fatal(err)
	}
}

func TestGateAndFlockShareOneAbsoluteDeadline(t *testing.T) {
	const (
		budget = 200 * time.Millisecond
		held   = 50 * time.Millisecond
	)
	enteredGate := make(chan time.Time, 1)
	acquired := make(chan time.Time, 1)
	acquiredAt := make(chan time.Time, 1)
	var beforeOnce sync.Once
	s := openTestStore(t, t.TempDir(), storeOptions{
		lockTimeout: budget,
		beforeGate: func(deadline time.Time) {
			beforeOnce.Do(func() { enteredGate <- deadline })
		},
		acquireLock: func(file *os.File, deadline time.Time) error {
			acquired <- deadline
			acquiredAt <- time.Now()
			return acquireFileLock(file, deadline)
		},
	})
	<-s.gate
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { releaseGate(s.gate) }) })
	result := make(chan error, 1)
	go func() {
		_, err := s.Devices()
		result <- err
	}()
	startDeadline := waitTime(t, enteredGate, time.Second, "pre-gate deadline")
	time.Sleep(held)
	releaseOnce.Do(func() { releaseGate(s.gate) })
	if err := waitError(t, result, time.Second, "composed-deadline operation"); err != nil {
		t.Fatal(err)
	}
	flockDeadline := waitTime(t, acquired, time.Second, "flock deadline")
	invoked := waitTime(t, acquiredAt, time.Second, "flock invocation")
	if !flockDeadline.Equal(startDeadline) {
		t.Fatalf("flock deadline %v differs from pre-gate deadline %v", flockDeadline, startDeadline)
	}
	if remaining := flockDeadline.Sub(invoked); remaining > budget-held {
		t.Fatalf("flock received restarted budget %s; want at most %s", remaining, budget-held)
	}
}

func TestExpiredDeadlineStillAttemptsFreeFlockOnce(t *testing.T) {
	s := openTestStore(t, t.TempDir(), storeOptions{})
	if err := acquireFileLock(s.lockFile, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("immediate flock attempt with expired deadline: %v", err)
	}
	if err := releaseFileLock(s.lockFile); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCloseIsIdempotentAndOperationsFailClosed(t *testing.T) {
	s := openTestStore(t, t.TempDir(), storeOptions{})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := s.Devices(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("post-close operation error = %v, want os.ErrClosed", err)
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitError(t *testing.T, ch <-chan error, timeout time.Duration, label string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
		return nil
	}
}

func waitTime(t *testing.T, ch <-chan time.Time, timeout time.Duration, label string) time.Time {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
		return time.Time{}
	}
}

func TestIdentityCreationIsStableAcrossStores(t *testing.T) {
	dir := t.TempDir()
	a := openTestStore(t, dir, storeOptions{})
	b := openTestStore(t, dir, storeOptions{})
	type result struct {
		id  *protocol.Identity
		err error
	}
	results := make(chan result, 2)
	go func() { id, err := a.Identity(); results <- result{id, err} }()
	go func() { id, err := b.Identity(); results <- result{id, err} }()
	x, y := <-results, <-results
	if x.err != nil || y.err != nil {
		t.Fatalf("identity errors: %v, %v", x.err, y.err)
	}
	if !bytes.Equal(x.id.Public(), y.id.Public()) {
		t.Fatal("concurrent stores created different identities")
	}
	info, err := os.Stat(a.Path("identity.key"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode: info=%v err=%v", info, err)
	}
}

func TestFileLockContentionIsBoundedAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	seed := openTestStore(t, dir, storeOptions{})
	if _, err := seed.Devices(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestStateLockHelperProcess$")
	cmd.Env = append(os.Environ(), "HERMES_STATE_LOCK_HELPER="+filepath.Join(dir, ".state.lock"))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var waitOnce sync.Once
	var waitErr error
	wait := func() { waitOnce.Do(func() { waitErr = cmd.Wait() }) }
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		wait()
	})
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "locked\n" {
		t.Fatalf("lock helper readiness = %q, %v", line, err)
	}

	contender := openTestStore(t, dir, storeOptions{lockTimeout: 40 * time.Millisecond})
	started := time.Now()
	_, err = contender.Devices()
	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("contended operation error = %v, want ErrLockTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded lock attempt took %s", elapsed)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	wait()
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if _, err := contender.Devices(); err != nil {
		t.Fatalf("operation did not recover after lock release: %v", err)
	}
}

func TestStateLockHelperProcess(t *testing.T) {
	lockPath := os.Getenv("HERMES_STATE_LOCK_HELPER")
	if lockPath == "" {
		return
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := acquireFileLock(f, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	fmt.Println("locked")
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
	if err := releaseFileLock(f); err != nil {
		t.Fatal(err)
	}
}
