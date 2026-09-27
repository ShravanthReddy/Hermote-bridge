package update

import (
	"context"
	"time"
)

// Release is one published release as the source reports it. Only the tag,
// its publication time and the changelog body are read; any link the source
// offers is ignored and built locally (D4).
type Release struct {
	Tag         string
	PublishedAt time.Time
	Body        string
}

// Source is where the bridge learns about releases (D3). Phase 1 only asks
// for the latest release; the phone never supplies a URL, host or path.
type Source interface {
	Latest(ctx context.Context) (Release, error)
}

// ErrorKind classifies a failed check for the phone (§3, error.kind).
type ErrorKind string

const (
	ErrorNetwork     ErrorKind = "network"
	ErrorTimeout     ErrorKind = "timeout"
	ErrorRateLimited ErrorKind = "rate_limited"
	ErrorMalformed   ErrorKind = "malformed"
)

// SourceError is a failed release lookup with the kind and sentence the
// phone shows.
type SourceError struct {
	Kind    ErrorKind
	Message string
	// ResetAt is GitHub's X-RateLimit-Reset for ErrorRateLimited; zero when
	// GitHub did not say.
	ResetAt time.Time
}

func (e *SourceError) Error() string { return e.Message }
