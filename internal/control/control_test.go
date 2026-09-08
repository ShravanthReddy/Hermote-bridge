package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/state"
)

type statusBackend struct{ launchID string }

func (b statusBackend) Status(context.Context) Status {
	return Status{LaunchID: b.launchID, Gateway: "ready"}
}

func (statusBackend) Pair(context.Context) (PairResult, error) {
	return PairResult{}, errors.New("not implemented")
}

func (statusBackend) Revoke(context.Context, string) (state.Device, error) {
	return state.Device{}, errors.New("not implemented")
}

func waitForLaunchID(t *testing.T, socketPath, launchID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		status, err := NewClient(socketPath).Status(ctx)
		cancel()
		if err == nil && status.LaunchID == launchID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("launch %q never owned %s", launchID, socketPath)
}

func TestOldServerShutdownDoesNotRemoveReplacementSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "hermote-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "control.sock")
	oldCtx, stopOld := context.WithCancel(context.Background())
	oldDone := make(chan error, 1)
	go func() { oldDone <- Serve(oldCtx, socketPath, statusBackend{launchID: "old"}) }()
	waitForLaunchID(t, socketPath, "old")

	newCtx, stopNew := context.WithCancel(context.Background())
	newDone := make(chan error, 1)
	go func() { newDone <- Serve(newCtx, socketPath, statusBackend{launchID: "new"}) }()
	waitForLaunchID(t, socketPath, "new")

	stopOld()
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socketPath); err != nil {
		t.Fatalf("old server removed replacement socket: %v", err)
	}
	waitForLaunchID(t, socketPath, "new")

	stopNew()
	if err := <-newDone; err != nil {
		t.Fatal(err)
	}
}
