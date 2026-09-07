package bridge

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
)

type recordingWriteCloser struct {
	bytes.Buffer
	closed    bool
	failAfter int64
}

func (w *recordingWriteCloser) Write(p []byte) (int, error) {
	if w.failAfter > 0 && int64(w.Len()+len(p)) > w.failAfter {
		remaining := int(w.failAfter - int64(w.Len()))
		if remaining > 0 {
			_, _ = w.Buffer.Write(p[:remaining])
		}
		return remaining, errors.New("injected writer failure")
	}
	return w.Buffer.Write(p)
}

func (w *recordingWriteCloser) Close() error {
	w.closed = true
	return nil
}

func TestWriteFileAttachMessageBatchesAndCounts(t *testing.T) {
	payload := bytes.Repeat([]byte("streaming-payload-"), 80_000)
	writer := &recordingWriteCloser{}
	copied, stats, err := writeFileAttachMessage(
		writer, bytes.NewReader(payload), int64(len(payload)),
		"request-1", "runtime-1", "résumé [final] $HOME.pdf", defaultBlobChunkBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !writer.closed || copied != int64(len(payload)) {
		t.Fatalf("closed=%v copied=%d want=%d", writer.closed, copied, len(payload))
	}
	if stats.maxWriteBytes > defaultBlobChunkBytes {
		t.Fatalf("maximum writer block=%d exceeds %d", stats.maxWriteBytes, defaultBlobChunkBytes)
	}
	wantWrites := (stats.writtenBytes + int64(defaultBlobChunkBytes) - 1) / int64(defaultBlobChunkBytes)
	if stats.writeCalls != wantWrites || stats.writeCalls < 2 {
		t.Fatalf("writer calls=%d want=%d bytes=%d", stats.writeCalls, wantWrites, stats.writtenBytes)
	}
	if stats.writtenBytes != int64(writer.Len()) {
		t.Fatalf("observed bytes=%d buffer=%d", stats.writtenBytes, writer.Len())
	}
	var request struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Method  string `json:"method"`
		Params  struct {
			SessionID string `json:"session_id"`
			DataURL   string `json:"data_url"`
			Name      string `json:"name"`
			Path      string `json:"path"`
		} `json:"params"`
	}
	if err := json.Unmarshal(writer.Bytes(), &request); err != nil {
		t.Fatal(err)
	}
	if request.JSONRPC != "2.0" || request.ID != "request-1" || request.Method != "file.attach" ||
		request.Params.SessionID != "runtime-1" || request.Params.Name != "résumé [final] $HOME.pdf" || request.Params.Path != "" {
		t.Fatalf("unexpected request metadata: %+v", request)
	}
	const prefix = "data:application/octet-stream;base64,"
	if len(request.Params.DataURL) < len(prefix) || request.Params.DataURL[:len(prefix)] != prefix {
		t.Fatal("missing data URL prefix")
	}
	decoded, err := base64.StdEncoding.DecodeString(request.Params.DataURL[len(prefix):])
	if err != nil || !bytes.Equal(decoded, payload) {
		t.Fatalf("streamed payload mismatch: decode=%v bytes=%d", err, len(decoded))
	}
}

func TestWriteFileAttachMessageShortReadAndWriterFailure(t *testing.T) {
	t.Run("short source", func(t *testing.T) {
		writer := &recordingWriteCloser{}
		copied, stats, err := writeFileAttachMessage(
			writer, bytes.NewReader([]byte("short")), 6,
			"request", "runtime", "name.txt", defaultBlobChunkBytes,
		)
		if !errors.Is(err, io.ErrUnexpectedEOF) || copied != 5 || !writer.closed {
			t.Fatalf("err=%v copied=%d closed=%v", err, copied, writer.closed)
		}
		if stats.maxWriteBytes > defaultBlobChunkBytes {
			t.Fatalf("maximum writer block=%d", stats.maxWriteBytes)
		}
	})

	t.Run("writer error closes", func(t *testing.T) {
		writer := &recordingWriteCloser{failAfter: 128}
		_, _, err := writeFileAttachMessage(
			writer, bytes.NewReader(bytes.Repeat([]byte{1}, 4096)), 4096,
			"request", "runtime", "name.txt", 256,
		)
		if err == nil || !writer.closed {
			t.Fatalf("err=%v closed=%v", err, writer.closed)
		}
	})

	t.Run("invalid frame size", func(t *testing.T) {
		writer := &recordingWriteCloser{}
		_, _, err := writeFileAttachMessage(writer, bytes.NewReader(nil), 0, "r", "s", "n", 0)
		if err == nil {
			t.Fatal("zero frame size accepted")
		}
	})
}

func TestAttachmentReceiptRequiresAuthoritativeRefPath(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/result.pdf"
	if err := writeTestFile(path, []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	valid := map[string]any{
		"attached": true, "uploaded": true, "name": "result.pdf", "path": path,
		"ref_path": "attachments/result file.pdf", "ref_text": "@file:`attachments/result file.pdf`",
	}
	raw, _ := json.Marshal(valid)
	if err := validateAttachmentReceipt(raw); err != nil {
		t.Fatalf("valid receipt rejected: %v", err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing ref_path": func(v map[string]any) { delete(v, "ref_path") },
		"mismatched ref":   func(v map[string]any) { v["ref_text"] = "@file:attachments/other.pdf" },
		"unquoted space":   func(v map[string]any) { v["ref_text"] = "@file:attachments/result file.pdf" },
		"not uploaded":     func(v map[string]any) { v["uploaded"] = false },
		"relative path":    func(v map[string]any) { v["path"] = "attachments/result.pdf" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := make(map[string]any, len(valid))
			for key, value := range valid {
				copy[key] = value
			}
			mutate(copy)
			raw, _ := json.Marshal(copy)
			if err := validateAttachmentReceipt(raw); err == nil {
				t.Fatal("malformed receipt accepted")
			}
		})
	}
}

func writeTestFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}

func TestAttachmentReceiptAcceptsGatewayBareAllQuoteFallback(t *testing.T) {
	name := "a'\"`b.txt"
	path := "attachments/" + name
	root := t.TempDir()
	storagePath := root + "/" + name
	if err := writeTestFile(storagePath, []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	receipt := map[string]any{
		"attached": true, "uploaded": true, "name": name, "path": storagePath,
		"ref_path": path, "ref_text": "@file:" + path,
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateAttachmentReceipt(raw); err != nil {
		t.Fatalf("canonical bare fallback rejected: %v", err)
	}
	receipt["ref_text"] = "@file:" + path + " @file:other.txt"
	raw, _ = json.Marshal(receipt)
	if err := validateAttachmentReceipt(raw); err == nil {
		t.Fatal("multiple references accepted")
	}
	receipt["name"] = "space " + name
	receipt["path"] = root + "/space " + name
	if err := writeTestFile(receipt["path"].(string), []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	receipt["ref_path"] = "attachments/space " + name
	receipt["ref_text"] = "@file:attachments/space " + name
	raw, _ = json.Marshal(receipt)
	if err := validateAttachmentReceipt(raw); err == nil {
		t.Fatal("unparseable whitespace fallback accepted")
	}
}

func TestFileTokenLeadingDelimiterMatchesOrderedGatewayScanner(t *testing.T) {
	for _, test := range []struct {
		token, value string
		valid        bool
	}{
		{"@file:'a\"`b.txt", "'a\"`b.txt", true},
		{"@file:'a'\"`b.txt", "", false},
		{"@file:'a\"`b.txt'", "a\"`b.txt", true},
		{"@file:''a\"`b.txt", "''a\"`b.txt", true},
		{"@file:'a \"`b.txt", "", false},
	} {
		value, valid := parseFileToken(test.token)
		if valid != test.valid || value != test.value {
			t.Errorf("parse %q=(%q,%v), want(%q,%v)", test.token, value, valid, test.value, test.valid)
		}
	}
}
