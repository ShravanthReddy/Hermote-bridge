package bridge

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func decodeListing(t *testing.T, body []byte) fsListing {
	t.Helper()
	var listing fsListing
	if err := json.Unmarshal(body, &listing); err != nil {
		t.Fatalf("bad listing body %s: %v", body, err)
	}
	return listing
}

func TestFSListSortsFoldersFirstAndHidesBuildDirs(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"zeta", "Alpha", "node_modules", ".git"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []string{"b.txt", "a.txt"} {
		if err := os.WriteFile(filepath.Join(root, file), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	status, body := fsList(context.Background(), "path="+url.QueryEscape(root))
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	listing := decodeListing(t, body)
	if listing.Error != "" {
		t.Fatalf("unexpected error %q", listing.Error)
	}
	var names []string
	for _, e := range listing.Entries {
		names = append(names, e.Name)
	}
	want := []string{"Alpha", "zeta", "a.txt", "b.txt"}
	if len(names) != len(want) {
		t.Fatalf("entries = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("entries = %v, want %v", names, want)
		}
	}
	if !listing.Entries[0].IsDirectory || listing.Entries[2].IsDirectory {
		t.Fatal("isDirectory flags wrong")
	}
	resolvedRoot, _ := filepath.EvalSymlinks(root) // the gateway resolves symlinks too
	if listing.Entries[0].Path != filepath.Join(resolvedRoot, "Alpha") {
		t.Fatalf("path = %q", listing.Entries[0].Path)
	}
}

func TestFSListReportsMissingAndNonDirectoryLikeTheGateway(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "f.txt")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, body := fsList(context.Background(), "path="+url.QueryEscape(filepath.Join(root, "missing")))
	if got := decodeListing(t, body).Error; got != "ENOENT" {
		t.Fatalf("missing dir error = %q", got)
	}
	_, body = fsList(context.Background(), "path="+url.QueryEscape(file))
	if got := decodeListing(t, body).Error; got != "ENOTDIR" {
		t.Fatalf("file error = %q", got)
	}
	if status, _ := fsList(context.Background(), ""); status != 400 {
		t.Fatalf("empty path status = %d", status)
	}
}

func TestFSListGivesUpOnAFolderThatNeverAnswers(t *testing.T) {
	previous := fsReadDir
	fsReadDir = func(string) ([]os.DirEntry, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	}
	defer func() { fsReadDir = previous }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	status, body := fsList(ctx, "path="+url.QueryEscape(t.TempDir()))
	if status != 200 || decodeListing(t, body).Error != "ETIMEDOUT" {
		t.Fatalf("status %d body %s", status, body)
	}
	if time.Since(started) > 150*time.Millisecond {
		t.Fatal("the listing waited for the slow read instead of giving up")
	}
}

func TestFSResolvePathExpandsHomeAndFileURLs(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home")
	}
	if got, _ := fsResolvePath("~/x"); got != filepath.Join(home, "x") {
		t.Fatalf("~ expansion = %q", got)
	}
	if got, _ := fsResolvePath("file:///tmp/y"); got != "/tmp/y" && got != "/private/tmp/y" {
		t.Fatalf("file URL = %q", got)
	}
	if _, err := fsResolvePath("file://evil.example/etc"); err == nil {
		t.Fatal("a remote file URL must be refused")
	}
}
