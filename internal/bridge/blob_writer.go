package bridge

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
)

func writeFileAttachMessage(
	writer io.WriteCloser,
	source io.Reader,
	declaredBytes int64,
	requestID, sessionID, name string,
	frameBytes int,
) (copied int64, stats blobWriteStats, err error) {
	if frameBytes <= 0 || frameBytes > defaultBlobChunkBytes {
		return 0, stats, errors.New("invalid attachment writer frame size")
	}
	observed := &observedWriteCloser{WriteCloser: writer}
	writer = observed
	closed := false
	defer func() {
		if !closed {
			closeErr := writer.Close()
			if err == nil {
				err = closeErr
			}
		}
		stats = observed.stats()
	}()
	idJSON, err := json.Marshal(requestID)
	if err != nil {
		return 0, stats, err
	}
	sessionJSON, err := json.Marshal(sessionID)
	if err != nil {
		return 0, stats, err
	}
	nameJSON, err := json.Marshal(name)
	if err != nil {
		return 0, stats, err
	}
	buffered := bufio.NewWriterSize(writer, frameBytes)
	prefix := bytes.Join([][]byte{
		[]byte(`{"jsonrpc":"2.0","id":`), idJSON,
		[]byte(`,"method":"file.attach","params":{"session_id":`), sessionJSON,
		[]byte(`,"data_url":"data:application/octet-stream;base64,`),
	}, nil)
	if _, err = buffered.Write(prefix); err != nil {
		return 0, stats, err
	}
	encoder := base64.NewEncoder(base64.StdEncoding, buffered)
	copyBuffer := make([]byte, 64<<10)
	copied, err = io.CopyBuffer(encoder, io.LimitReader(source, declaredBytes), copyBuffer)
	if err != nil {
		_ = encoder.Close()
		return copied, stats, err
	}
	if copied != declaredBytes {
		_ = encoder.Close()
		return copied, stats, io.ErrUnexpectedEOF
	}
	if err = encoder.Close(); err != nil {
		return copied, stats, err
	}
	suffix := bytes.Join([][]byte{[]byte(`","name":`), nameJSON, []byte(`}}`)}, nil)
	if _, err = buffered.Write(suffix); err != nil {
		return copied, stats, err
	}
	if err = buffered.Flush(); err != nil {
		return copied, stats, err
	}
	err = writer.Close()
	closed = true
	return copied, stats, err
}

type blobWriteStats struct {
	writeCalls    int64
	writtenBytes  int64
	maxWriteBytes int
}

type observedWriteCloser struct {
	io.WriteCloser
	statsValue blobWriteStats
}

func (w *observedWriteCloser) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	w.statsValue.writeCalls++
	w.statsValue.writtenBytes += int64(n)
	if n > w.statsValue.maxWriteBytes {
		w.statsValue.maxWriteBytes = n
	}
	return n, err
}

func (w *observedWriteCloser) stats() blobWriteStats { return w.statsValue }
