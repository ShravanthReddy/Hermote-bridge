package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the golden e2ee vectors consumed by the Swift tests")

// detRNG is a deterministic byte stream (SHA-256 counter mode) for vectors.
type detRNG struct {
	seed []byte
	ctr  uint64
	buf  []byte
}

func newDetRNG(label string) *detRNG { return &detRNG{seed: []byte(label)} }

func (r *detRNG) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if len(r.buf) == 0 {
			var c [8]byte
			binary.BigEndian.PutUint64(c[:], r.ctr)
			r.ctr++
			sum := sha256.Sum256(append(append([]byte{}, r.seed...), c[:]...))
			r.buf = sum[:]
		}
		k := copy(p[n:], r.buf)
		r.buf = r.buf[k:]
		n += k
	}
	return n, nil
}

func mustIdentity(t *testing.T, label string) *Identity {
	t.Helper()
	id, err := NewIdentity(newDetRNG(label))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSessionIDShape(t *testing.T) {
	id := mustIdentity(t, "bridge")
	if got := id.SessionID(); len(got) != SessionIDLength || got != SessionIDFor(id.Public()) {
		t.Fatalf("session id %q", got)
	}
	again, _ := IdentityFromSeed(id.Seed())
	if !bytes.Equal(again.Public(), id.Public()) {
		t.Fatal("seed round-trip changed the key")
	}
}

func TestPairRoundTrip(t *testing.T) {
	bridge := mustIdentity(t, "bridge")
	code, _ := NewPairCode(newDetRNG("code"))
	p := PairPayload{
		Version: Version, Transport: TransportDirect,
		URL: "wss://mac.tail1234.ts.net:8443/v1/bridge", SessionID: bridge.SessionID(),
		BridgeKey: bridge.Public(), Code: code, Expires: time.Unix(1_800_000_000, 0).UTC(),
		Name: "Shravanth's Mac mini",
	}
	back, err := ParsePair(p.String())
	if err != nil {
		t.Fatal(err)
	}
	if back.String() != p.String() || back.Name != p.Name || !back.Expires.Equal(p.Expires) {
		t.Fatalf("round trip mismatch:\n%s\n%s", p.String(), back.String())
	}
	bad := p
	bad.SessionID = "AAAAAAAAAAAAAAAAAAAAAA"
	if _, err := ParsePair(bad.String()); err == nil {
		t.Fatal("mismatched session id accepted")
	}
	if _, err := ParsePair("https://example.com/pair"); err == nil {
		t.Fatal("non-hermes URL accepted")
	}
}

type handshakeResult struct {
	hello   Hello
	accept  Accept
	confirm []byte
	phone   *Suite
	bridge  *Suite
	usedKey []byte
}

func runHandshake(t *testing.T, bridge, phone *Identity, code []byte, trusted bool, rngLabel string) (handshakeResult, error) {
	t.Helper()
	hello, ps, err := PhoneHello(phone, bridge.SessionID(), newDetRNG(rngLabel+"/phone"))
	if err != nil {
		t.Fatal(err)
	}
	accept, pending, err := BridgeAccept(bridge, hello, newDetRNG(rngLabel+"/bridge"))
	if err != nil {
		t.Fatal(err)
	}
	confirm, phoneSuite, err := ps.Finish(accept, bridge.Public(), code)
	if err != nil {
		return handshakeResult{}, err
	}
	var codes [][]byte
	if code != nil {
		codes = [][]byte{code}
	}
	bridgeSuite, used, err := pending.Finish(confirm, func(id []byte) bool { return trusted && bytes.Equal(id, phone.Public()) }, codes)
	if err != nil {
		return handshakeResult{}, err
	}
	return handshakeResult{hello, accept, confirm, phoneSuite, bridgeSuite, used}, nil
}

func TestHandshakePairsUnknownPhoneWithCode(t *testing.T) {
	bridge, phone := mustIdentity(t, "bridge"), mustIdentity(t, "phone")
	code, _ := NewPairCode(newDetRNG("code"))
	r, err := runHandshake(t, bridge, phone, code, false, "hs1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.usedKey, code) {
		t.Fatal("pairing code not reported as used")
	}
	// Both directions work after the handshake and counters continue from confirm.
	frame, _ := r.phone.Seal([]byte(`{"ch":"ws","d":"hi"}`))
	if got, err := r.bridge.Open(frame); err != nil || string(got) != `{"ch":"ws","d":"hi"}` {
		t.Fatalf("phone→bridge failed: %v %q", err, got)
	}
	frame, _ = r.bridge.Seal([]byte(`{"ch":"ctl","op":"pong"}`))
	if got, err := r.phone.Open(frame); err != nil || string(got) != `{"ch":"ctl","op":"pong"}` {
		t.Fatalf("bridge→phone failed: %v %q", err, got)
	}
}

func TestHandshakeAdmitsTrustedPhoneWithoutCode(t *testing.T) {
	bridge, phone := mustIdentity(t, "bridge"), mustIdentity(t, "phone")
	if _, err := runHandshake(t, bridge, phone, nil, true, "hs2"); err != nil {
		t.Fatal(err)
	}
}

func TestHandshakeRejectsUnknownPhoneWithoutOrWrongCode(t *testing.T) {
	bridge, phone := mustIdentity(t, "bridge"), mustIdentity(t, "phone")
	if _, err := runHandshake(t, bridge, phone, nil, false, "hs3"); !errors.Is(err, ErrUntrustedPhone) {
		t.Fatalf("expected ErrUntrustedPhone, got %v", err)
	}
	right, _ := NewPairCode(newDetRNG("right"))
	wrong, _ := NewPairCode(newDetRNG("wrong"))
	hello, ps, _ := PhoneHello(phone, bridge.SessionID(), newDetRNG("hs4/phone"))
	accept, pending, _ := BridgeAccept(bridge, hello, newDetRNG("hs4/bridge"))
	confirm, _, _ := ps.Finish(accept, bridge.Public(), wrong)
	if _, _, err := pending.Finish(confirm, nil, [][]byte{right}); !errors.Is(err, ErrUntrustedPhone) {
		t.Fatalf("wrong code accepted: %v", err)
	}
}

func TestHandshakeRejectsImpostorBridge(t *testing.T) {
	bridge, impostor, phone := mustIdentity(t, "bridge"), mustIdentity(t, "impostor"), mustIdentity(t, "phone")
	hello, ps, _ := PhoneHello(phone, impostor.SessionID(), newDetRNG("hs5/phone"))
	accept, _, err := BridgeAccept(impostor, hello, newDetRNG("hs5/bridge"))
	if err != nil {
		t.Fatal(err)
	}
	// The phone pinned the real bridge's key from the QR; the impostor's signature must fail.
	if _, _, err := ps.Finish(accept, bridge.Public(), nil); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("expected ErrBadSignature, got %v", err)
	}
	// And a hello addressed to another session is refused outright.
	if _, _, err := BridgeAccept(bridge, hello, nil); err == nil {
		t.Fatal("hello for a different session accepted")
	}
}

func TestEnvelopeCounterAndTamper(t *testing.T) {
	key1, key2 := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	a, _ := SuiteFromKeys(key1, key2)
	b, _ := SuiteFromKeys(key2, key1)
	f1, _ := a.Seal([]byte("one"))
	f2, _ := a.Seal([]byte("two"))
	if _, err := b.Open(f2); !errors.Is(err, ErrCounter) {
		t.Fatalf("skipped counter accepted: %v", err)
	}
	if _, err := b.Open(f1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Open(f1); !errors.Is(err, ErrCounter) {
		t.Fatalf("replay accepted: %v", err)
	}
	f2[len(f2)-1] ^= 0xff
	if _, err := b.Open(f2); !errors.Is(err, ErrFrame) {
		t.Fatalf("tampered frame accepted: %v", err)
	}
	if _, err := b.Open([]byte{0x02, 0, 0, 0, 0, 0, 0, 0, 1}); !errors.Is(err, ErrFrame) {
		t.Fatal("bad version accepted")
	}
}

func TestChunks(t *testing.T) {
	if SplitChunks(make([]byte, ChunkThreshold)) != nil {
		t.Fatal("threshold-sized message should not chunk")
	}
	big := bytes.Repeat([]byte("x"), 2*ChunkThreshold+5)
	chunks := SplitChunks(big)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks", len(chunks))
	}
	var asm ChunkAssembler
	var out []byte
	for _, c := range chunks {
		var err error
		if out, err = asm.Add(c); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(out, big) {
		t.Fatal("reassembly mismatch")
	}
	if _, err := asm.Add(chunks[1]); !errors.Is(err, ErrChunkOrder) {
		t.Fatal("out-of-order chunk accepted")
	}
	if ch, _ := PeekChannel([]byte(`{"ch":"chunk","i":0,"n":1,"d":""}`)); ch != ChChunk {
		t.Fatal("peek failed")
	}
	if _, err := PeekChannel([]byte(`{"ch":"nope"}`)); !errors.Is(err, ErrUnknownChannel) {
		t.Fatal("unknown channel accepted")
	}
}

// ── Golden vectors shared with the Swift client ─────────────────────────────

type vectorFile struct {
	Note       string        `json:"_note"`
	Identity   vecIdentity   `json:"identity"`
	Pair       vecPair       `json:"pair"`
	Handshake  vecHandshake  `json:"handshake"`
	Envelopes  []vecEnvelope `json:"envelopes"`
	ChunkSplit vecChunk      `json:"chunk"`
}

type vecIdentity struct {
	Seed, Public, SessionID string `json:",omitempty"`
}

type vecPair struct {
	URL, Name, Transport, WSURL string
	Code                        string
	Expires                     int64
}

type vecHandshake struct {
	BridgeSeed, PhoneSeed         string
	HelloJSON, AcceptJSON         string
	Transcript, KeyP2B, KeyB2P    string
	Code, PairProof, ConfirmPlain string
	ConfirmFrame                  string
	PhoneSigLabel, BridgeSigLabel string
	TranscriptLabel, KeysLabel    string
	PairProofLabel                string
}

type vecEnvelope struct {
	Direction string `json:"direction"` // "p2b" | "b2p"
	Counter   uint64 `json:"counter"`
	Plaintext string `json:"plaintext"`
	Frame     string `json:"frame"`
}

type vecChunk struct {
	Threshold int    `json:"threshold"`
	Sizes     []int  `json:"sizes"`
	Chunks    []int  `json:"chunks"`
	Note      string `json:"note"`
}

// vectorsPath is the canonical copy inside this module (so the standalone
// mirror repo tests itself). mirrorPath is the iOS client's copy, refreshed by
// -update when this module lives inside the app repository.
func vectorsPath(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("testdata", "e2ee-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mirrorPath() (string, bool) {
	p, err := filepath.Abs(filepath.Join("..", "..", "..", "HermesKit", "Tests", "HermesKitTests", "Fixtures", "e2ee-vectors.json"))
	if err != nil {
		return "", false
	}
	if _, err := os.Stat(filepath.Dir(p)); err != nil {
		return "", false
	}
	return p, true
}

func TestGoldenVectors(t *testing.T) {
	bridge := mustIdentity(t, "vectors/bridge")
	phone := mustIdentity(t, "vectors/phone")
	code, _ := NewPairCode(newDetRNG("vectors/code"))
	pair := PairPayload{
		Version: Version, Transport: TransportDirect,
		URL: "wss://mac.tail1234.ts.net:8443/v1/bridge", SessionID: bridge.SessionID(),
		BridgeKey: bridge.Public(), Code: code, Expires: time.Unix(1_800_000_000, 0).UTC(), Name: "Test Mac",
	}
	hello, ps, err := PhoneHello(phone, bridge.SessionID(), newDetRNG("vectors/phone-eph"))
	if err != nil {
		t.Fatal(err)
	}
	accept, pending, err := BridgeAccept(bridge, hello, newDetRNG("vectors/bridge-eph"))
	if err != nil {
		t.Fatal(err)
	}
	transcript := Transcript(hello, bridge.Public(), accept)
	confirm, phoneSuite, err := ps.Finish(accept, bridge.Public(), code)
	if err != nil {
		t.Fatal(err)
	}
	bridgeSuite, _, err := pending.Finish(confirm, nil, [][]byte{code})
	if err != nil {
		t.Fatal(err)
	}
	// Recompute the derived keys explicitly for the vector file.
	keysSuite := deriveKeysForVector(t, ps, accept, transcript)
	confirmPlain, _ := json.Marshal(Confirm{Ch: ChConfirm, Sig: phone.sign(labelSigPhone, transcript), Proof: pairProof(code, transcript)})

	var envs []vecEnvelope
	for _, pt := range []string{`{"ch":"ws","d":"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session.list\",\"params\":{}}"}`, `{"ch":"ctl","op":"ping"}`} {
		f, _ := phoneSuite.Seal([]byte(pt))
		envs = append(envs, vecEnvelope{"p2b", binary.BigEndian.Uint64(f[1:9]), pt, hex.EncodeToString(f)})
		if _, err := bridgeSuite.Open(f); err != nil {
			t.Fatal(err)
		}
	}
	for _, pt := range []string{`{"ch":"ctl","op":"pong"}`, `{"ch":"http","id":1,"status":200,"body":{"ok":true}}`} {
		f, _ := bridgeSuite.Seal([]byte(pt))
		envs = append(envs, vecEnvelope{"b2p", binary.BigEndian.Uint64(f[1:9]), pt, hex.EncodeToString(f)})
		if _, err := phoneSuite.Open(f); err != nil {
			t.Fatal(err)
		}
	}
	helloJSON, _ := json.Marshal(hello)
	acceptJSON, _ := json.Marshal(accept)
	got := vectorFile{
		Note:     "Generated by remote/internal/protocol (go test -update). Consumed by HermesKit E2EETests. Do not edit by hand.",
		Identity: vecIdentity{Seed: hex.EncodeToString(bridge.Seed()), Public: hex.EncodeToString(bridge.Public()), SessionID: bridge.SessionID()},
		Pair: vecPair{URL: pair.String(), Name: pair.Name, Transport: string(pair.Transport), WSURL: pair.URL,
			Code: hex.EncodeToString(code), Expires: pair.Expires.Unix()},
		Handshake: vecHandshake{
			BridgeSeed: hex.EncodeToString(bridge.Seed()), PhoneSeed: hex.EncodeToString(phone.Seed()),
			HelloJSON: string(helloJSON), AcceptJSON: string(acceptJSON),
			Transcript: hex.EncodeToString(transcript), KeyP2B: keysSuite[0], KeyB2P: keysSuite[1],
			Code: hex.EncodeToString(code), PairProof: hex.EncodeToString(pairProof(code, transcript)),
			ConfirmPlain: string(confirmPlain), ConfirmFrame: hex.EncodeToString(confirm),
			PhoneSigLabel: labelSigPhone, BridgeSigLabel: labelSigBridge, TranscriptLabel: labelTranscript,
			KeysLabel: labelKeys, PairProofLabel: labelPairProof,
		},
		Envelopes: envs,
		ChunkSplit: vecChunk{Threshold: ChunkThreshold, Sizes: []int{ChunkThreshold, ChunkThreshold + 1, 2*ChunkThreshold + 5},
			Chunks: []int{0, 2, 3}, Note: "0 means sent unchunked"},
	}
	for i, size := range got.ChunkSplit.Sizes {
		if n := len(SplitChunks(make([]byte, size))); n != got.ChunkSplit.Chunks[i] {
			t.Fatalf("chunk count for %d: %d", size, n)
		}
	}

	path := vectorsPath(t)
	data, _ := json.MarshalIndent(got, "", "  ")
	data = append(data, '\n')
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", path)
		if mirror, ok := mirrorPath(); ok {
			if err := os.WriteFile(mirror, data, 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("wrote %s", mirror)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden vectors (%v); run: go test ./internal/protocol -update", err)
	}
	if !bytes.Equal(want, data) {
		t.Fatalf("golden vectors changed; if intentional run: go test ./internal/protocol -update\n--- want\n%s\n--- got\n%s", want, data)
	}
}

// deriveKeysForVector re-derives k_p2b/k_b2p from the phone's ephemeral key so
// the vector file can state them explicitly (hex).
func deriveKeysForVector(t *testing.T, ps *PhoneState, a Accept, transcript []byte) [2]string {
	t.Helper()
	pub, err := ecdhPublic(a.BridgeEph)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := ps.eph.ECDH(pub)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := hkdfKeys(shared, transcript)
	if err != nil {
		t.Fatal(err)
	}
	return [2]string{hex.EncodeToString(keys[:32]), hex.EncodeToString(keys[32:])}
}
