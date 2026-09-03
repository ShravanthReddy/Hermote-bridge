package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Directory listings are answered by the daemon, not proxied to the gateway.
// The gateway's `/api/fs/list` reads the directory synchronously on its event
// loop, and on a Mac with Desktop & Documents in iCloud a dataless folder
// blocks that read for 15 s or more — every other request froze with it and
// the watchdog then ended the child (2026-09-03). Here each listing runs on
// its own goroutine with a deadline: a slow folder fails alone, in time.
//
// The response mirrors the gateway's exactly ({entries:[{name,path,
// isDirectory}], error?}) so the phone needs no change.

const fsListTimeout = 10 * time.Second

// fsHiddenNames mirrors the gateway's `_FS_READDIR_HIDDEN`.
var fsHiddenNames = map[string]bool{
	".git": true, ".hg": true, ".svn": true, ".cache": true, ".next": true, ".turbo": true, ".venv": true,
	"__pycache__": true, "build": true, "dist": true, "node_modules": true, "target": true, "venv": true,
}

// fsReadDir is swapped by tests to simulate a folder that never answers.
var fsReadDir = os.ReadDir

type fsEntry struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	IsDirectory bool   `json:"isDirectory"`
}

type fsListing struct {
	Entries []fsEntry `json:"entries"`
	Error   string    `json:"error,omitempty"`
}

// fsList answers GET /api/fs/list?path=… Status 400 for an unusable path,
// otherwise 200 with the gateway's shape (errors ride inside the body).
func fsList(ctx context.Context, rawQuery string) (int, []byte) {
	values, _ := url.ParseQuery(rawQuery)
	target, err := fsResolvePath(values.Get("path"))
	if err != nil {
		body, _ := json.Marshal(map[string]string{"detail": err.Error()})
		return 400, body
	}
	type result struct {
		entries []os.DirEntry
		err     error
	}
	done := make(chan result, 1)
	go func() {
		entries, err := fsReadDir(target)
		done <- result{entries, err}
	}()
	ctx, cancel := context.WithTimeout(ctx, fsListTimeout)
	defer cancel()
	var listing fsListing
	listing.Entries = []fsEntry{}
	select {
	case <-ctx.Done():
		listing.Error = "ETIMEDOUT"
	case r := <-done:
		switch {
		case r.err == nil:
			for _, entry := range r.entries {
				if fsHiddenNames[entry.Name()] {
					continue
				}
				listing.Entries = append(listing.Entries, fsEntry{
					Name:        entry.Name(),
					Path:        filepath.Join(target, entry.Name()),
					IsDirectory: entry.IsDir(),
				})
			}
			sort.Slice(listing.Entries, func(i, j int) bool {
				a, b := listing.Entries[i], listing.Entries[j]
				if a.IsDirectory != b.IsDirectory {
					return a.IsDirectory
				}
				if la, lb := strings.ToLower(a.Name), strings.ToLower(b.Name); la != lb {
					return la < lb
				}
				return a.Name < b.Name
			})
		case errors.Is(r.err, os.ErrNotExist):
			listing.Error = "ENOENT"
		case errors.Is(r.err, os.ErrPermission):
			listing.Error = "EACCES"
		case isNotDir(r.err):
			listing.Error = "ENOTDIR"
		default:
			listing.Error = "read-error"
		}
	}
	body, _ := json.Marshal(listing)
	return 200, body
}

// fsResolvePath mirrors the gateway's `_fs_path`: `~` expands, `file:` URLs
// are accepted, relative paths hang off the daemon's cwd, symlinks resolve
// as far as the path exists.
func fsResolvePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("Path is required")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("Invalid path")
	}
	if strings.HasPrefix(strings.ToLower(raw), "file:") {
		parsed, err := url.Parse(raw)
		if err != nil || (parsed.Host != "" && parsed.Host != "localhost") {
			return "", errors.New("Invalid path")
		}
		raw = parsed.Path
	}
	if raw == "~" || strings.HasPrefix(raw, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			raw = home + raw[1:]
		} else if u, err := user.Current(); err == nil {
			raw = u.HomeDir + raw[1:]
		}
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", errors.New("Invalid path")
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, nil
	}
	return filepath.Clean(abs), nil
}

func isNotDir(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return strings.Contains(strings.ToLower(pathErr.Err.Error()), "not a directory")
	}
	return false
}
