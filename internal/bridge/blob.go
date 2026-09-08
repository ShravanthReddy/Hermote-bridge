package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
)

const (
	blobProtocolVersion = 1
	blobPayloadName     = "payload"
	blobResultMaxBytes  = 60 << 10
	blobWireMaxBytes    = 64 << 10
	blobStringMaxBytes  = 4 << 10

	blobCodeInvalidRequest = -32070
	blobCodeBusy           = -32071
	blobCodeQuota          = -32072
	blobCodeInvalidState   = -32073
	blobCodeOffset         = -32074
	blobCodeSize           = -32075
	blobCodeDigest         = -32076
	blobCodeTimeout        = -32077
	blobCodeGateway        = -32078
	blobCodeGatewayUpdate  = -32079
	blobCodeIO             = -32080
)

type blobLocalError struct {
	code    int
	kind    string
	message string
}

func (e blobLocalError) Error() string { return e.message }

func (e blobLocalError) wire() *protocol.BlobError {
	return &protocol.BlobError{Code: e.code, Kind: e.kind, Message: boundedMessage(e.message)}
}

func invalidBlob(message string) blobLocalError {
	return blobLocalError{blobCodeInvalidRequest, "invalid_request", message}
}

type blobManager struct {
	root       string
	maxBytes   int64
	maxUploads int
	// Package-private seam for deterministic allocation-failure tests.
	openPayload func(string) (*os.File, error)

	mu              sync.Mutex
	active          int
	reserved        int64
	peakActive      int
	peakReserved    int64
	auxDispatches   uint64
	lastAuxWrites   int64
	lastAuxBytes    int64
	lastAuxMaxWrite int
	lastCommitNanos int64
	cleanupFailures uint64
	initErr         error
}

// blobUsageSnapshot is a package-private, numeric-only observation seam for
// deterministic lifecycle tests and the disposable real-gateway fixture.
// It deliberately contains no names, paths, session IDs, or payload data.
type blobUsageSnapshot struct {
	ActiveUploads       int
	ReservedBytes       int64
	PeakActiveUploads   int
	PeakReservedBytes   int64
	AuxiliaryDispatches uint64
	LastAuxWriteCalls   int64
	LastAuxBytes        int64
	LastAuxMaxWrite     int
	LastCommitNanos     int64
	CleanupFailures     uint64
	Unavailable         int
}

func newBlobManager(root string, deps bridgeDependencies) *blobManager {
	m := &blobManager{
		root:        root,
		maxBytes:    deps.blobReservedBytes,
		maxUploads:  deps.blobProcessWide,
		openPayload: openBlobPayload,
	}
	if root == "" {
		m.initErr = errors.New("attachment spool root is unavailable")
		return m
	}
	if err := m.prepareRoot(); err != nil {
		m.initErr = err
	}
	return m
}

func (m *blobManager) prepareRoot() error {
	if err := os.MkdirAll(m.root, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(m.root)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("attachment spool root is not a directory")
	}
	if err := os.Chmod(m.root, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isCanonicalUUID(entry.Name()) {
			continue
		}
		if err := cleanupOwnedUpload(m.root, entry.Name()); err != nil {
			m.cleanupFailures++
			return fmt.Errorf("could not clean stale attachment spool: %w", err)
		}
	}
	return nil
}

func (m *blobManager) reserve(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initErr != nil {
		return m.initErr
	}
	if m.active >= m.maxUploads {
		return blobLocalError{blobCodeBusy, "process_busy", "too many attachment uploads are active"}
	}
	if size > m.maxBytes-m.reserved {
		return blobLocalError{blobCodeQuota, "spool_quota_exceeded", "attachment spool quota exceeded"}
	}
	m.active++
	m.reserved += size
	if m.active > m.peakActive {
		m.peakActive = m.active
	}
	if m.reserved > m.peakReserved {
		m.peakReserved = m.reserved
	}
	return nil
}

func (m *blobManager) settleCleanup(size int64, cleanupErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cleanupErr != nil {
		m.cleanupFailures++
		if m.initErr == nil {
			m.initErr = fmt.Errorf("attachment spool cleanup failed: %w", cleanupErr)
		}
		return
	}
	m.active--
	m.reserved -= size
}

func (m *blobManager) available() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.initErr == nil
}

func (m *blobManager) recordAuxiliaryDispatch() {
	m.mu.Lock()
	m.auxDispatches++
	m.mu.Unlock()
}

func (m *blobManager) recordAuxiliaryWrite(stats blobWriteStats) {
	m.mu.Lock()
	m.lastAuxWrites = stats.writeCalls
	m.lastAuxBytes = stats.writtenBytes
	m.lastAuxMaxWrite = stats.maxWriteBytes
	m.mu.Unlock()
}

func (m *blobManager) recordCommitDuration(elapsed time.Duration) {
	m.mu.Lock()
	m.lastCommitNanos = elapsed.Nanoseconds()
	m.mu.Unlock()
}

func (m *blobManager) snapshot() blobUsageSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	unavailable := 0
	if m.initErr != nil {
		unavailable = 1
	}
	return blobUsageSnapshot{
		ActiveUploads: m.active, ReservedBytes: m.reserved,
		PeakActiveUploads: m.peakActive, PeakReservedBytes: m.peakReserved,
		AuxiliaryDispatches: m.auxDispatches,
		LastAuxWriteCalls:   m.lastAuxWrites, LastAuxBytes: m.lastAuxBytes,
		LastAuxMaxWrite: m.lastAuxMaxWrite, LastCommitNanos: m.lastCommitNanos,
		CleanupFailures: m.cleanupFailures, Unavailable: unavailable,
	}
}

func (m *blobManager) createPayload() (dirID, dir, path string, file *os.File, err error) {
	for range 16 {
		dirID, err = randomUUID()
		if err != nil {
			return "", "", "", nil, err
		}
		dir = filepath.Join(m.root, dirID)
		err = os.Mkdir(dir, 0o700)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", "", "", nil, err
		}
		path = filepath.Join(dir, blobPayloadName)
		file, err = m.openPayload(path)
		if err == nil {
			return dirID, dir, path, file, nil
		}
		if file != nil {
			_ = file.Close()
		}
		cleanupErr := cleanupOwnedUpload(m.root, dirID)
		return "", "", "", nil, blobAllocationError{cause: err, cleanupErr: cleanupErr}
	}
	return "", "", "", nil, errors.New("could not allocate an attachment spool directory")
}

// A failed allocation can still own a directory or partial payload. Carry the
// cleanup result to the reservation owner rather than releasing it blindly.
type blobAllocationError struct {
	cause      error
	cleanupErr error
}

func (e blobAllocationError) Error() string { return e.cause.Error() }
func (e blobAllocationError) Unwrap() error { return e.cause }

func openBlobPayload(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return file, err
	}
	return file, file.Chmod(0o600)
}

func cleanupOwnedUpload(root, dirID string) error {
	if !isCanonicalUUID(dirID) {
		return errors.New("invalid owned upload directory")
	}
	dir := filepath.Join(root, dirID)
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("owned upload path is not a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != blobPayloadName {
			return errors.New("owned upload directory contains an unknown entry")
		}
	}
	payload := filepath.Join(dir, blobPayloadName)
	if payloadInfo, payloadErr := os.Lstat(payload); payloadErr == nil {
		if payloadInfo.Mode()&os.ModeSymlink != 0 || !payloadInfo.Mode().IsRegular() {
			return errors.New("owned upload payload is not a regular file")
		}
		if err := os.Remove(payload); err != nil {
			return err
		}
	} else if !errors.Is(payloadErr, os.ErrNotExist) {
		return payloadErr
	}
	return os.Remove(dir)
}

type blobPhase uint8

const (
	blobReceiving blobPhase = iota
	blobCommitting
)

type blobUpload struct {
	id        string
	sessionID string
	name      string
	size      int64
	digest    string
	dirID     string
	path      string
	file      *os.File
	hasher    hash.Hash
	offset    int64
	phase     blobPhase
	finished  bool

	parent        context.Context
	ctx           context.Context
	cancel        context.CancelFunc
	activity      chan struct{}
	commitStarted chan struct{}
	finishOnce    sync.Once
	fail          func(error)
}

type blobConnection struct {
	conn *conn

	mu        sync.Mutex
	active    *blobUpload
	completed *protocol.BlobMessage
	workers   sync.WaitGroup
}

func newBlobConnection(c *conn) *blobConnection {
	return &blobConnection{conn: c}
}

func (b *blobConnection) shutdownAndWait() {
	b.mu.Lock()
	if b.active != nil {
		b.active.cancel()
	}
	b.mu.Unlock()
	b.workers.Wait()
}

func (b *blobConnection) readTimeout(fallback time.Duration) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil {
		return fallback
	}
	timeout := b.conn.srv.deps.blobIdleTimeout + 10*time.Second
	if b.active.phase == blobCommitting {
		timeout = b.conn.srv.deps.blobCommitTimeout + 10*time.Second
	}
	if timeout < fallback {
		return fallback
	}
	return timeout
}

func (b *blobConnection) handle(
	ctx context.Context, plain []byte, fail func(error),
) error {
	fields, id, op, err := decodeBlobBase(plain)
	if err != nil {
		if id == "" {
			return err
		}
		return b.replyError(ctx, id, invalidBlob(err.Error()))
	}
	if !isCanonicalUUID(id) {
		return b.replyError(ctx, id, invalidBlob("id must be a canonical lowercase UUID"))
	}
	if len(plain) > protocol.ChunkThreshold {
		return b.failMatchingUpload(ctx, id, blobLocalError{
			blobCodeSize, "chunk_too_large", "attachment blob frame exceeds the protocol limit",
		})
	}
	switch op {
	case "begin":
		return b.handleBegin(ctx, fields, id, fail)
	case "data":
		return b.handleData(ctx, fields, id)
	case "commit":
		return b.handleCommit(ctx, fields, id)
	case "abort":
		return b.handleAbort(ctx, fields, id)
	default:
		return b.replyError(ctx, id, invalidBlob("unknown blob operation"))
	}
}

func (b *blobConnection) handleBegin(
	ctx context.Context, fields map[string]json.RawMessage, id string, fail func(error),
) error {
	if err := exactBlobFields(fields, "ch", "op", "id", "session_id", "name", "size", "sha256"); err != nil {
		return b.replyError(ctx, id, invalidBlob(err.Error()))
	}
	sessionID, err := boundedJSONString(fields["session_id"], 1024)
	if err != nil || sessionID == "" || containsControl(sessionID) {
		return b.replyError(ctx, id, invalidBlob("invalid session_id"))
	}
	name, err := boundedJSONString(fields["name"], 255)
	if err != nil || !validOriginalAttachmentName(name) {
		return b.replyError(ctx, id, invalidBlob("invalid attachment name"))
	}
	size, err := strictNonnegativeInt(fields["size"])
	if err != nil || size > b.conn.srv.deps.blobMaxFileBytes {
		return b.replyError(ctx, id, blobLocalError{
			blobCodeSize, "file_size_invalid",
			fmt.Sprintf("attachment size must be between 0 and %d bytes", b.conn.srv.deps.blobMaxFileBytes),
		})
	}
	digest, err := boundedJSONString(fields["sha256"], 64)
	if err != nil || !isLowerHexDigest(digest) {
		return b.replyError(ctx, id, invalidBlob("sha256 must be 64 lowercase hexadecimal characters"))
	}

	b.mu.Lock()
	if b.active != nil {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{
			blobCodeBusy, "upload_busy", "this connection already has an active attachment upload",
		})
	}
	if err := b.conn.srv.blobs.reserve(size); err != nil {
		b.mu.Unlock()
		if local, ok := err.(blobLocalError); ok {
			return b.replyError(ctx, id, local)
		}
		return b.replyError(ctx, id, blobLocalError{blobCodeIO, "spool_unavailable", "attachment spool is unavailable"})
	}
	dirID, _, path, file, createErr := b.conn.srv.blobs.createPayload()
	if createErr != nil {
		var allocation blobAllocationError
		var cleanupErr error
		if errors.As(createErr, &allocation) {
			cleanupErr = allocation.cleanupErr
		}
		b.conn.srv.blobs.settleCleanup(size, cleanupErr)
		b.mu.Unlock()
		if cleanupErr != nil {
			return b.replyError(ctx, id, blobLocalError{blobCodeIO, "spool_cleanup_failed", "could not clean attachment spool data"})
		}
		return b.replyError(ctx, id, blobLocalError{blobCodeIO, "spool_io", "could not create attachment spool file"})
	}
	uploadCtx, cancel := context.WithCancel(ctx)
	upload := &blobUpload{
		id: id, sessionID: sessionID, name: name, size: size, digest: digest,
		dirID: dirID, path: path, file: file, hasher: sha256.New(),
		parent: ctx, ctx: uploadCtx, cancel: cancel, activity: make(chan struct{}, 1),
		commitStarted: make(chan struct{}), fail: fail,
	}
	b.active = upload
	b.workers.Add(1)
	go b.monitorUpload(upload)
	b.mu.Unlock()

	offset := int64(0)
	chunkBytes := b.conn.srv.deps.blobChunkBytes
	return b.conn.sendJSON(ctx, protocol.BlobMessage{
		Ch: protocol.ChBlob, Op: "ready", ID: id, Offset: &offset, ChunkBytes: &chunkBytes,
	})
}

func (b *blobConnection) monitorUpload(upload *blobUpload) {
	defer b.workers.Done()
	timer := time.NewTimer(b.conn.srv.deps.blobIdleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-upload.ctx.Done():
			if b.claimReceivingFinish(upload) {
				b.finish(upload, nil, false)
			}
			return
		case <-upload.commitStarted:
			return
		case <-upload.activity:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.conn.srv.deps.blobIdleTimeout)
		case <-timer.C:
			response := blobErrorMessage(upload.id, blobLocalError{
				blobCodeTimeout, "upload_idle_timeout", "attachment upload timed out waiting for data",
			})
			if b.claimReceivingFinish(upload) {
				b.finish(upload, &response, false)
			}
			return
		}
	}
}

// claimReceivingFinish prevents the idle/cancellation monitor from closing a
// spool file after commit has taken ownership of it. The transition shares the
// same lock as handleData and handleCommit.
func (b *blobConnection) claimReceivingFinish(upload *blobUpload) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != upload || upload.phase != blobReceiving || upload.finished {
		return false
	}
	upload.finished = true
	return true
}

func (b *blobConnection) handleData(ctx context.Context, fields map[string]json.RawMessage, id string) error {
	if err := exactBlobFields(fields, "ch", "op", "id", "offset", "d"); err != nil {
		return b.failMatchingUpload(ctx, id, invalidBlob(err.Error()))
	}
	offset, err := strictNonnegativeInt(fields["offset"])
	if err != nil {
		return b.failMatchingUpload(ctx, id, invalidBlob("offset must be a nonnegative integer"))
	}
	encodedLimit := encodedChunkLimit(b.conn.srv.deps.blobChunkBytes)
	encoded, err := boundedJSONString(fields["d"], encodedLimit+1)
	if err != nil || encoded == "" || strings.Contains(encoded, "=") {
		return b.failMatchingUpload(ctx, id, invalidBlob("data must be nonempty unpadded base64url"))
	}
	if len(encoded) > encodedLimit || base64.RawURLEncoding.DecodedLen(len(encoded)) > b.conn.srv.deps.blobChunkBytes {
		return b.failMatchingUpload(ctx, id, blobLocalError{
			blobCodeSize, "chunk_too_large", "attachment data chunk exceeds the negotiated limit",
		})
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(data) > b.conn.srv.deps.blobChunkBytes {
		return b.failMatchingUpload(ctx, id, invalidBlob("invalid base64url attachment data"))
	}

	b.mu.Lock()
	upload := b.active
	if upload == nil || upload.id != id {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "unknown_upload", "no matching attachment upload"})
	}
	if upload.finished || upload.ctx.Err() != nil {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "upload_finished", "attachment upload has finished"})
	}
	if upload.phase != blobReceiving {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "commit_in_progress", "attachment commit is already in progress"})
	}
	if offset != upload.offset {
		b.mu.Unlock()
		return b.failUpload(upload, blobLocalError{blobCodeOffset, "offset_mismatch", "attachment data offset is not contiguous"})
	}
	if int64(len(data)) > upload.size-upload.offset {
		b.mu.Unlock()
		return b.failUpload(upload, blobLocalError{blobCodeSize, "declared_size_exceeded", "attachment data exceeds the declared size"})
	}
	n, writeErr := upload.file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		b.mu.Unlock()
		return b.failUpload(upload, blobLocalError{blobCodeIO, "spool_io", "could not write attachment data"})
	}
	_, _ = upload.hasher.Write(data)
	upload.offset += int64(n)
	next := upload.offset
	select {
	case upload.activity <- struct{}{}:
	default:
	}
	b.mu.Unlock()
	return b.conn.sendJSON(ctx, protocol.BlobMessage{
		Ch: protocol.ChBlob, Op: "ack", ID: id, Offset: &next,
	})
}

func (b *blobConnection) handleCommit(
	ctx context.Context, fields map[string]json.RawMessage, id string,
) error {
	if err := exactBlobFields(fields, "ch", "op", "id"); err != nil {
		return b.failMatchingUpload(ctx, id, invalidBlob(err.Error()))
	}
	b.mu.Lock()
	upload := b.active
	if upload == nil {
		completed := cloneBlobMessage(b.completed)
		b.mu.Unlock()
		if completed != nil && completed.ID == id {
			return b.conn.sendJSON(ctx, *completed)
		}
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "unknown_upload", "no matching attachment upload"})
	}
	if upload.id != id {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "unknown_upload", "no matching attachment upload"})
	}
	if upload.finished {
		completed := cloneBlobMessage(b.completed)
		b.mu.Unlock()
		if completed != nil && completed.ID == id {
			return b.conn.sendJSON(ctx, *completed)
		}
		return nil
	}
	if upload.ctx.Err() != nil {
		b.mu.Unlock()
		return nil
	}
	if upload.phase == blobCommitting {
		completed := cloneBlobMessage(b.completed)
		b.mu.Unlock()
		if completed != nil && completed.ID == id {
			return b.conn.sendJSON(ctx, *completed)
		}
		return nil
	}
	upload.phase = blobCommitting
	close(upload.commitStarted)
	b.workers.Add(1)
	go b.commit(upload)
	b.mu.Unlock()
	return nil
}

func (b *blobConnection) commit(upload *blobUpload) {
	defer b.workers.Done()
	started := time.Now()
	defer func() { b.conn.srv.blobs.recordCommitDuration(time.Since(started)) }()
	commitCtx, cancel := context.WithTimeout(upload.ctx, b.conn.srv.deps.blobCommitTimeout)
	defer cancel()

	if upload.offset != upload.size {
		response := blobErrorMessage(upload.id, blobLocalError{
			blobCodeSize, "size_mismatch", "attachment byte count does not match the declared size",
		})
		b.finish(upload, &response, true)
		return
	}
	if got := hex.EncodeToString(upload.hasher.Sum(nil)); got != upload.digest {
		response := blobErrorMessage(upload.id, blobLocalError{
			blobCodeDigest, "hash_mismatch", "attachment sha256 does not match",
		})
		b.finish(upload, &response, true)
		return
	}
	if err := upload.file.Sync(); err != nil {
		response := blobErrorMessage(upload.id, blobLocalError{blobCodeIO, "spool_io", "could not sync attachment data"})
		b.finish(upload, &response, true)
		return
	}
	if err := upload.file.Close(); err != nil {
		response := blobErrorMessage(upload.id, blobLocalError{blobCodeIO, "spool_io", "could not close attachment data"})
		b.finish(upload, &response, true)
		return
	}
	upload.file = nil

	result, err := b.callFileAttach(commitCtx, upload)
	if err != nil {
		if upload.parent.Err() != nil || errors.Is(upload.ctx.Err(), context.Canceled) {
			b.finish(upload, nil, false)
			return
		}
		response := blobErrorMessage(upload.id, blobErrorForGateway(err))
		b.finish(upload, &response, true)
		return
	}
	response := protocol.BlobMessage{
		Ch: protocol.ChBlob, Op: "result", ID: upload.id, Result: result,
	}
	if raw, marshalErr := json.Marshal(response); marshalErr != nil || len(raw) > blobWireMaxBytes {
		response = blobErrorMessage(upload.id, blobLocalError{
			blobCodeGateway, "invalid_gateway_receipt", "gateway attachment receipt exceeds the blob result limit",
		})
	}
	b.finish(upload, &response, true)
}

func (b *blobConnection) handleAbort(ctx context.Context, fields map[string]json.RawMessage, id string) error {
	if err := exactBlobFields(fields, "ch", "op", "id"); err != nil {
		return b.replyError(ctx, id, invalidBlob(err.Error()))
	}
	b.mu.Lock()
	if b.active != nil && b.active.id == id {
		b.active.cancel()
	}
	b.mu.Unlock()
	return nil
}

func (b *blobConnection) failMatchingUpload(ctx context.Context, id string, local blobLocalError) error {
	b.mu.Lock()
	upload := b.active
	if upload == nil || upload.id != id {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "unknown_upload", "no matching attachment upload"})
	}
	if upload.finished || upload.ctx.Err() != nil {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "upload_finished", "attachment upload has finished"})
	}
	if upload.phase != blobReceiving {
		b.mu.Unlock()
		return b.replyError(ctx, id, blobLocalError{blobCodeInvalidState, "commit_in_progress", "attachment commit is already in progress"})
	}
	b.mu.Unlock()
	return b.failUpload(upload, local)
}

func (b *blobConnection) failUpload(upload *blobUpload, local blobLocalError) error {
	response := blobErrorMessage(upload.id, local)
	b.finish(upload, &response, false)
	return nil
}

func (b *blobConnection) finish(upload *blobUpload, response *protocol.BlobMessage, cache bool) {
	upload.finishOnce.Do(func() {
		b.mu.Lock()
		upload.finished = true
		b.mu.Unlock()

		upload.cancel()
		if upload.file != nil {
			_ = upload.file.Close()
			upload.file = nil
		}
		cleanupErr := cleanupOwnedUpload(b.conn.srv.blobs.root, upload.dirID)
		if cleanupErr != nil {
			b.conn.srv.blobs.settleCleanup(upload.size, cleanupErr)
			cleanupResponse := blobErrorMessage(upload.id, blobLocalError{
				blobCodeIO, "spool_cleanup_failed", "could not clean attachment spool data",
			})
			response = &cleanupResponse
			cache = true
		}

		b.mu.Lock()
		if cache && response != nil {
			b.completed = cloneBlobMessage(response)
		}
		b.mu.Unlock()

		var sendErr error
		if response != nil && upload.parent.Err() == nil {
			sendErr = b.conn.sendJSON(upload.parent, *response)
		}
		if cleanupErr == nil {
			b.conn.srv.blobs.settleCleanup(upload.size, nil)
		}

		b.mu.Lock()
		if b.active == upload {
			b.active = nil
		}
		b.mu.Unlock()
		if sendErr != nil && upload.fail != nil {
			upload.fail(fmt.Errorf("blob result write: %w", sendErr))
		}
	})
}

func (b *blobConnection) replyError(ctx context.Context, id string, local blobLocalError) error {
	return b.conn.sendJSON(ctx, blobErrorMessage(id, local))
}

func blobErrorMessage(id string, local blobLocalError) protocol.BlobMessage {
	return protocol.BlobMessage{Ch: protocol.ChBlob, Op: "result", ID: id, Error: local.wire()}
}

func cloneBlobMessage(message *protocol.BlobMessage) *protocol.BlobMessage {
	if message == nil {
		return nil
	}
	cloned := *message
	cloned.Result = bytes.Clone(message.Result)
	if message.Error != nil {
		value := *message.Error
		cloned.Error = &value
	}
	return &cloned
}

type gatewayRPCError struct {
	code    int
	message string
}

func (e gatewayRPCError) Error() string {
	return fmt.Sprintf("gateway RPC %d: %s", e.code, e.message)
}

type malformedGatewayResult struct {
	message string
}

func (e malformedGatewayResult) Error() string { return e.message }

func (b *blobConnection) callFileAttach(ctx context.Context, upload *blobUpload) (json.RawMessage, error) {
	base, ok := b.conn.srv.Gateway.BaseURL()
	if !ok {
		return nil, errors.New("gateway is not ready")
	}
	url := "ws" + strings.TrimPrefix(base, "http") + "/api/ws?token=" + b.conn.srv.Gateway.Token()
	gw, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, err
	}
	gw.SetReadLimit(maxFrame)
	defer gw.CloseNow()

	file, err := os.Open(upload.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != upload.size {
		return nil, malformedGatewayResult{message: "verified attachment spool file changed before commit"}
	}
	b.conn.srv.blobs.recordAuxiliaryDispatch()

	requestID := "attachment-blob-" + upload.id
	requestIDJSON, _ := json.Marshal(requestID)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type readResult struct {
		result json.RawMessage
		err    error
	}
	readDone := make(chan readResult, 1)
	writeDone := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		result, readErr := readMatchingGatewayResult(workerCtx, gw, requestIDJSON)
		readDone <- readResult{result: result, err: readErr}
	}()
	go func() {
		defer workers.Done()
		writer, writerErr := gw.Writer(workerCtx, websocket.MessageText)
		var stats blobWriteStats
		if writerErr == nil {
			_, stats, writerErr = writeFileAttachMessage(
				writer, file, upload.size, requestID, upload.sessionID, upload.name,
				b.conn.srv.deps.blobChunkBytes,
			)
		}
		b.conn.srv.blobs.recordAuxiliaryWrite(stats)
		writeDone <- writerErr
	}()

	var outcome *readResult
	writerFinished := false
	for outcome == nil || !writerFinished {
		select {
		case read := <-readDone:
			outcome = &read
			readDone = nil
			if read.err != nil {
				cancel()
				_ = gw.CloseNow()
			}
		case writeErr := <-writeDone:
			if writeErr != nil {
				cancel()
				_ = gw.CloseNow()
				workers.Wait()
				if outcome != nil && outcome.err != nil {
					return outcome.result, outcome.err
				}
				return nil, writeErr
			}
			writerFinished = true
			writeDone = nil
		case <-ctx.Done():
			cancel()
			_ = gw.CloseNow()
			workers.Wait()
			return nil, ctx.Err()
		}
	}
	cancel()
	_ = gw.CloseNow()
	workers.Wait()
	if outcome.err == nil {
		outcome.err = b.validateReceiptOutsideSpool(outcome.result)
	}
	return outcome.result, outcome.err
}

func readMatchingGatewayResult(
	ctx context.Context, gw *websocket.Conn, requestIDJSON []byte,
) (json.RawMessage, error) {
	for {
		_, raw, err := gw.Read(ctx)
		if err != nil {
			return nil, err
		}
		var probe struct {
			ID json.RawMessage `json:"id"`
		}
		if json.Unmarshal(raw, &probe) != nil || len(probe.ID) == 0 {
			continue
		}
		if !bytes.Equal(bytes.TrimSpace(probe.ID), requestIDJSON) {
			continue
		}
		var reply struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  json.RawMessage `json:"result"`
			Error   *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &reply) != nil {
			return nil, malformedGatewayResult{message: "gateway returned a malformed attachment response"}
		}
		if reply.JSONRPC != "2.0" {
			return nil, malformedGatewayResult{message: "gateway returned a malformed attachment response"}
		}
		if reply.Error != nil && len(reply.Result) != 0 && string(reply.Result) != "null" {
			return nil, malformedGatewayResult{message: "gateway returned both an attachment result and error"}
		}
		if reply.Error != nil {
			return nil, gatewayRPCError{code: reply.Error.Code, message: boundedMessage(reply.Error.Message)}
		}
		if len(reply.Result) == 0 || len(reply.Result) > blobResultMaxBytes {
			return nil, malformedGatewayResult{message: "gateway returned an invalid attachment receipt"}
		}
		if err := validateAttachmentReceipt(reply.Result); err != nil {
			return nil, err
		}
		return bytes.Clone(reply.Result), nil
	}
}

func validateAttachmentReceipt(raw json.RawMessage) error {
	var receipt struct {
		Attached bool   `json:"attached"`
		Uploaded bool   `json:"uploaded"`
		Name     string `json:"name"`
		Path     string `json:"path"`
		RefPath  string `json:"ref_path"`
		RefText  string `json:"ref_text"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return malformedGatewayResult{message: "gateway returned a malformed attachment receipt"}
	}
	if !receipt.Attached || !receipt.Uploaded ||
		len(receipt.Name) == 0 || len(receipt.Name) > 255 ||
		len(receipt.Path) == 0 || len(receipt.Path) > blobStringMaxBytes ||
		len(receipt.RefPath) == 0 || len(receipt.RefPath) > blobStringMaxBytes ||
		len(receipt.RefText) == 0 || len(receipt.RefText) > blobStringMaxBytes ||
		!validAttachmentName(receipt.Name) {
		return malformedGatewayResult{message: "gateway returned an invalid attachment receipt"}
	}
	parsedRef, ok := parseFileToken(receipt.RefText)
	if !ok || parsedRef != receipt.RefPath {
		return malformedGatewayResult{message: "gateway attachment reference does not match ref_path"}
	}
	if !filepath.IsAbs(receipt.Path) {
		return malformedGatewayResult{message: "gateway attachment path is not absolute"}
	}
	info, err := os.Stat(receipt.Path)
	if err != nil || !info.Mode().IsRegular() {
		return malformedGatewayResult{message: "gateway attachment path is not a readable regular file"}
	}
	return nil
}

func (b *blobConnection) validateReceiptOutsideSpool(raw json.RawMessage) error {
	var receipt struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return err
	}
	return requirePathOutsideRoot(b.conn.srv.blobs.root, receipt.Path)
}

func requirePathOutsideRoot(root, path string) error {
	rootAbs, rootErr := filepath.Abs(root)
	pathAbs, pathErr := filepath.Abs(path)
	if rootErr != nil || pathErr != nil {
		return malformedGatewayResult{message: "gateway attachment path could not be validated"}
	}
	if evaluated, err := filepath.EvalSymlinks(rootAbs); err == nil {
		rootAbs = evaluated
	}
	if evaluated, err := filepath.EvalSymlinks(pathAbs); err == nil {
		pathAbs = evaluated
	}
	relative, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
		return malformedGatewayResult{message: "gateway attachment path remained inside the bridge spool"}
	}
	return nil
}

func blobErrorForGateway(err error) blobLocalError {
	var rpc gatewayRPCError
	if errors.As(err, &rpc) {
		return blobLocalError{rpc.code, "gateway_rpc", rpc.message}
	}
	if websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
		return blobLocalError{
			blobCodeGatewayUpdate, "gateway_update_required",
			"the desktop gateway rejected this attachment size; update Hermes on the Mac and retry",
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return blobLocalError{blobCodeTimeout, "commit_timeout", "attachment commit timed out"}
	}
	var malformed malformedGatewayResult
	if errors.As(err, &malformed) {
		return blobLocalError{blobCodeGateway, "invalid_gateway_receipt", malformed.message}
	}
	return blobLocalError{blobCodeGateway, "gateway_transport", "attachment commit failed at the desktop gateway"}
}
