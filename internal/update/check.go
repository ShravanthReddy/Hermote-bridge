package update

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Bounds of the update check (§4, B1.2).
const (
	// apiRequestTimeout bounds one API exchange, body included.
	apiRequestTimeout = 20 * time.Second
	// checkCacheLifetime is how long a successful check is reused, as the
	// desktop does (apps/desktop/src/store/updates.ts:1017).
	checkCacheLifetime = 24 * time.Hour
	// minCheckInterval spaces requests: at most 30 an hour, under GitHub's
	// anonymous 60.
	minCheckInterval = 2 * time.Minute
	// maxRateLimitWait caps how long a spent rate limit stops requests.
	maxRateLimitWait = time.Hour
)

// releaseTag accepts only vX.Y.Z tags.
var releaseTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// CheckError is a failed check as the phone shows it (§3, error).
type CheckError struct {
	Kind    ErrorKind `json:"kind"`
	Message string    `json:"message"`
}

// LatestRelease is the newest release a successful check saw.
type LatestRelease struct {
	// Version is Tag without its "v".
	Version     string
	Tag         string
	PublishedAt time.Time
	Notes       []string
	More        int
}

// Result is the checker's state after a check: the last good release (nil
// until a check has succeeded once), when it was checked, and the cause of
// the latest failure, if the latest attempt failed.
type Result struct {
	Current   string
	Latest    *LatestRelease
	CheckedAt time.Time
	Err       *CheckError
}

// ReleaseInfo is the release block of a check reply. URL is built locally
// from the tag, never taken from the source (D4).
type ReleaseInfo struct {
	Tag         string    `json:"tag"`
	URL         string    `json:"url"`
	PublishedAt time.Time `json:"published_at"`
	Notes       []string  `json:"notes"`
	More        int       `json:"more"`
}

// CheckResponse is the reply to GET /bridge/v1/update/check (§3). current,
// verified, can_apply and update_command are always present; latest,
// update_available, checked_at and release stay null until a check has
// succeeded once.
type CheckResponse struct {
	Current         string       `json:"current"`
	Latest          *string      `json:"latest"`
	UpdateAvailable *bool        `json:"update_available"`
	CheckedAt       *time.Time   `json:"checked_at"`
	Verified        bool         `json:"verified"`
	Release         *ReleaseInfo `json:"release"`
	CanApply        bool         `json:"can_apply"`
	Unavailable     *Reason      `json:"unavailable"`
	UpdateCommand   string       `json:"update_command"`
	Error           *CheckError  `json:"error"`
}

// Checker is the bridge's one cached release check per Mac (D3). Concurrent
// callers join a single request, which runs on its own context so a caller
// that leaves does not cancel it for the others; the cache has that one
// request as its only writer.
type Checker struct {
	source  Source
	current string
	now     func() time.Time
	timeout time.Duration

	mu           sync.Mutex
	latest       *LatestRelease
	checkedAt    time.Time
	lastErr      *CheckError
	lastRequest  time.Time
	blockedUntil time.Time
	flight       chan struct{}
}

// NewChecker checks source for releases newer than current (main.version).
func NewChecker(source Source, current string) *Checker {
	return &Checker{source: source, current: current, now: time.Now, timeout: apiRequestTimeout}
}

// Check returns the release check. A successful result is reused for a day
// unless force is set; any request, forced or not, waits two minutes after
// the previous one, and none is made while GitHub's rate limit is spent.
// Otherwise it asks the source, joining a request already running. When ctx
// ends first, Check returns what is cached without waiting.
func (c *Checker) Check(ctx context.Context, force bool) Result {
	c.mu.Lock()
	flight := c.flight
	if flight == nil {
		if !c.mayRequest(force) {
			result := c.resultLocked()
			c.mu.Unlock()
			return result
		}
		flight = make(chan struct{})
		c.flight = flight
		c.lastRequest = c.now()
		go c.request(flight)
	}
	c.mu.Unlock()
	select {
	case <-flight:
	case <-ctx.Done():
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resultLocked()
}

func (c *Checker) mayRequest(force bool) bool {
	now := c.now()
	switch {
	case now.Before(c.blockedUntil):
		return false
	case !c.lastRequest.IsZero() && now.Sub(c.lastRequest) < minCheckInterval:
		return false
	case !force && c.latest != nil && now.Sub(c.checkedAt) < checkCacheLifetime:
		return false
	}
	return true
}

func (c *Checker) request(flight chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	release, err := c.source.Latest(ctx)
	cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	defer close(flight)
	c.flight = nil
	if err == nil {
		err = c.accept(release)
	}
	if err == nil {
		c.lastErr = nil
		return
	}
	var sourceErr *SourceError
	if !errors.As(err, &sourceErr) {
		sourceErr = &SourceError{Kind: ErrorNetwork, Message: "api.github.com: " + err.Error()}
	}
	c.lastErr = &CheckError{Kind: sourceErr.Kind, Message: sourceErr.Message}
	if sourceErr.Kind == ErrorRateLimited {
		now := c.now()
		c.blockedUntil = now.Add(maxRateLimitWait)
		if !sourceErr.ResetAt.IsZero() && sourceErr.ResetAt.Before(c.blockedUntil) {
			c.blockedUntil = sourceErr.ResetAt
		}
	}
}

// accept records a successful answer; a tag that is not vX.Y.Z is malformed.
func (c *Checker) accept(release Release) error {
	if !releaseTag.MatchString(release.Tag) {
		return &SourceError{Kind: ErrorMalformed, Message: "The latest release's tag " + strconv.Quote(release.Tag) + " is not a version."}
	}
	notes, more := releaseNotes(release.Body)
	c.latest = &LatestRelease{
		Version: release.Tag[1:], Tag: release.Tag, PublishedAt: release.PublishedAt.UTC(),
		Notes: notes, More: more,
	}
	c.checkedAt = c.now()
	return nil
}

func (c *Checker) resultLocked() Result {
	return Result{Current: c.current, Latest: c.latest, CheckedAt: c.checkedAt, Err: c.lastErr}
}

// Response is the check reply for this install. A latest release is an
// update only when it is newer than the running version; a running version
// that is not X.Y.Z (a development build) is never offered one.
func (r Result) Response(install Install) CheckResponse {
	canApply, unavailable := install.Availability()
	response := CheckResponse{
		Current: r.Current, CanApply: canApply, UpdateCommand: install.UpdateCommand, Error: r.Err,
	}
	if unavailable != "" {
		response.Unavailable = &unavailable
	}
	if r.Latest == nil {
		return response
	}
	latest := r.Latest.Version
	available := isNewer(r.Latest.Tag, "v"+r.Current)
	checkedAt := r.CheckedAt.UTC().Truncate(time.Second)
	response.Latest, response.UpdateAvailable, response.CheckedAt = &latest, &available, &checkedAt
	response.Release = &ReleaseInfo{
		Tag: r.Latest.Tag, URL: releaseTagURLPrefix + r.Latest.Tag,
		PublishedAt: r.Latest.PublishedAt.Truncate(time.Second), Notes: r.Latest.Notes, More: r.Latest.More,
	}
	return response
}

// isNewer compares two vX.Y.Z tags; false when either is not one.
func isNewer(candidate, running string) bool {
	a, b := releaseTag.FindStringSubmatch(candidate), releaseTag.FindStringSubmatch(running)
	if a == nil || b == nil {
		return false
	}
	for i := 1; i <= 3; i++ {
		x, _ := strconv.Atoi(a[i])
		y, _ := strconv.Atoi(b[i])
		if x != y {
			return x > y
		}
	}
	return false
}
