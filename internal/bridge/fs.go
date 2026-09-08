package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Directory listings run in the daemon instead of the gateway: a dataless
// iCloud folder can block the gateway event loop and freeze other requests.
// Each listing runs in its own goroutine with a deadline, so a slow folder
// fails alone.
//
// The response mirrors the gateway's exactly ({entries:[{name,path,
// isDirectory}], error?}) so the phone needs no change.

// fsHiddenNames mirrors the gateway's `_FS_READDIR_HIDDEN`.
var fsHiddenNames = map[string]bool{
	".git": true, ".hg": true, ".svn": true, ".cache": true, ".next": true, ".turbo": true, ".venv": true,
	"__pycache__": true, "build": true, "dist": true, "node_modules": true, "target": true, "venv": true,
}

type fsEntry struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	IsDirectory bool   `json:"isDirectory"`
}

type fsListing struct {
	Entries []fsEntry `json:"entries"`
	Error   string    `json:"error,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

// fsList answers GET /api/fs/list?path=… Status 400 for an unusable path,
// otherwise 200 with the gateway's shape (errors ride inside the body).
func fsList(ctx context.Context, rawQuery string) (int, []byte) {
	status, body, _ := fsListWithDependencies(
		ctx, rawQuery, productionBridgeDependencies(), nil, slog.Default(),
	)
	return status, body
}

func fsListWithDependencies(
	ctx context.Context, rawQuery string, deps bridgeDependencies, lease *workLease, logger *slog.Logger,
) (int, []byte, error) {
	values, queryErr := url.ParseQuery(rawQuery)
	if queryErr != nil {
		// Query parsing is the only synchronous exit after FS admission. No
		// worker owns the lease on this path, so release it explicitly.
		if lease != nil {
			lease.release()
		}
		status, body := fsListingResponse(400, fsListing{Entries: []fsEntry{}, Detail: "Invalid path"})
		return status, body, nil
	}
	listCtx, cancel := context.WithTimeout(ctx, deps.fsListTimeout)
	defer cancel()
	type result struct {
		status int
		body   []byte
	}
	done := make(chan result, 1)
	owned := make(chan struct{})
	if deps.fsWarningAfter > 0 {
		if logger == nil {
			logger = slog.Default()
		}
		go func() {
			timer := time.NewTimer(deps.fsWarningAfter)
			defer timer.Stop()
			select {
			case <-timer.C:
				logger.Warn("filesystem listing still holds bridge capacity")
			case <-owned:
			}
		}()
	}
	go func() {
		var completed result
		defer func() { done <- completed }()
		defer close(owned)
		if lease != nil {
			defer lease.release()
		}

		target, err := deps.fsResolvePath(values.Get("path"))
		if err != nil {
			completed.status, completed.body = fsListingResponse(
				400, fsListing{Entries: []fsEntry{}, Detail: err.Error()},
			)
			return
		}
		entries, readErr := deps.fsReadDir(target)
		listing := fsListing{Entries: []fsEntry{}}
		switch {
		case readErr == nil:
			for _, entry := range entries {
				name := entry.Name()
				if fsHiddenNames[name] {
					continue
				}
				listing.Entries = append(listing.Entries, fsEntry{
					Name: name, Path: filepath.Join(target, name), IsDirectory: entry.IsDir(),
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
		case errors.Is(readErr, os.ErrNotExist):
			listing.Error = "ENOENT"
		case errors.Is(readErr, os.ErrPermission):
			listing.Error = "EACCES"
		case isNotDir(readErr):
			listing.Error = "ENOTDIR"
		default:
			listing.Error = "read-error"
		}
		completed.status, completed.body = fsListingResponse(200, listing)
	}()
	select {
	case <-listCtx.Done():
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		status, body := fsListingResponse(200, fsListing{Entries: []fsEntry{}, Error: "ETIMEDOUT"})
		return status, body, nil
	case r := <-done:
		return r.status, r.body, nil
	}
}

func fsListingResponse(status int, listing fsListing) (int, []byte) {
	body, _ := json.Marshal(listing)
	return status, body
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
