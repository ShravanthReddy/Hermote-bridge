package update

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestIdentityKeepsDigestAfterPathReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hermote-bridge")
	original := []byte("the binary this process started from")
	if err := os.WriteFile(path, original, 0o755); err != nil {
		t.Fatal(err)
	}
	id, err := captureIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	sum := sha256.Sum256(original)
	if id.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("start-up digest %q, want the running file's", id.SHA256)
	}
	if len(id.Incarnation) != 32 {
		t.Fatalf("incarnation %q is not 16 bytes of hex", id.Incarnation)
	}
	if !id.SameFile(path) {
		t.Fatal("the start-up path is the running file before anything replaces it")
	}

	// An installer replaces the path by rename, as install.sh does.
	staged := filepath.Join(dir, ".hermote-bridge.new")
	if err := os.WriteFile(staged, []byte("a newer release"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	if id.SHA256 != hex.EncodeToString(sum[:]) || id.Path != path {
		t.Fatalf("identity changed with the path: %+v", id)
	}
	if id.SameFile(path) {
		t.Fatal("the replaced path still reads as the running file")
	}
	other, err := captureIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if other.Incarnation == id.Incarnation {
		t.Fatal("two processes drew the same incarnation")
	}
}
