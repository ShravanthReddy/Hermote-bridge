package push

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

func writeTestKey(t *testing.T, dir string) (string, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "AuthKey_TEST.p8")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, key
}

func testConfig(t *testing.T, dir string) (Config, *ecdsa.PrivateKey) {
	t.Helper()
	path, key := writeTestKey(t, dir)
	return Config{TeamID: "TEAMID1234", KeyID: "KEYID12345", KeyPath: path, BundleID: "com.example.hermes"}, key
}

func TestBearerIsAValidES256JWT(t *testing.T) {
	cfg, key := testConfig(t, t.TempDir())
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	bearer, err := client.token()
	if err != nil {
		t.Fatal(err)
	}
	signing, signature, err := ParseBearer(bearer)
	if err != nil {
		t.Fatal(err)
	}
	if len(signature) != 64 {
		t.Fatalf("signature must be R‖S (64 bytes), got %d", len(signature))
	}
	digest := sha256.Sum256([]byte(signing))
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	if !ecdsa.Verify(&key.PublicKey, digest[:], r, s) {
		t.Fatal("signature does not verify with the key's public half")
	}
	parts := strings.Split(signing, ".")
	if len(parts) != 2 {
		t.Fatalf("signing input has %d parts", len(parts))
	}
	if !strings.Contains(decodeSegment(t, parts[0]), `"kid":"KEYID12345"`) {
		t.Fatalf("header lacks kid: %s", decodeSegment(t, parts[0]))
	}
	if !strings.Contains(decodeSegment(t, parts[1]), `"iss":"TEAMID1234"`) {
		t.Fatalf("claims lack iss: %s", decodeSegment(t, parts[1]))
	}
	again, _ := client.token()
	if again != bearer {
		t.Fatal("token should be reused within 50 minutes")
	}
}

func decodeSegment(t *testing.T, seg string) string {
	t.Helper()
	_, raw, err := ParseBearer("x." + seg)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestSendShapesTheRequestAndMapsUnregistered(t *testing.T) {
	cfg, _ := testConfig(t, t.TempDir())
	var got *http.Request
	var body []byte
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(status)
		if status != http.StatusOK {
			_, _ = w.Write([]byte(`{"reason":"BadDeviceToken"}`))
		}
	}))
	defer server.Close()
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client.Host = server.URL
	alert := Alert{
		Kind: KindApproval, Title: "Approval needed", Body: "rm -rf build", Category: CategoryApproval,
		SessionID: "abc", RequestID: "req1", CollapseID: "approval:abc",
	}
	if err := client.Send(context.Background(), "deadbeef", protocol.PushSandbox, alert); err != nil {
		t.Fatal(err)
	}
	if got.URL.Path != "/3/device/deadbeef" {
		t.Fatalf("path %s", got.URL.Path)
	}
	for header, want := range map[string]string{
		"apns-topic": "com.example.hermes", "apns-push-type": "alert", "apns-priority": "10",
		"apns-collapse-id": "approval:abc",
	} {
		if got.Header.Get(header) != want {
			t.Errorf("%s = %q, want %q", header, got.Header.Get(header), want)
		}
	}
	if !strings.HasPrefix(got.Header.Get("authorization"), "bearer ") {
		t.Errorf("authorization = %q", got.Header.Get("authorization"))
	}
	var payload struct {
		APS struct {
			Alert    map[string]string `json:"alert"`
			Category string            `json:"category"`
			ThreadID string            `json:"thread-id"`
			Sound    string            `json:"sound"`
		} `json:"aps"`
		Hermes struct {
			Kind      string `json:"kind"`
			SessionID string `json:"session_id"`
			RequestID string `json:"request_id"`
		} `json:"hermes"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.APS.Alert["title"] != "Approval needed" || payload.APS.Category != CategoryApproval ||
		payload.APS.ThreadID != "abc" || payload.APS.Sound != "default" {
		t.Fatalf("aps: %+v", payload.APS)
	}
	if payload.Hermes.Kind != "approval" || payload.Hermes.SessionID != "abc" || payload.Hermes.RequestID != "req1" {
		t.Fatalf("hermes: %+v", payload.Hermes)
	}

	status = http.StatusBadRequest
	if err := client.Send(context.Background(), "deadbeef", protocol.PushSandbox, alert); err != ErrUnregistered {
		t.Fatalf("BadDeviceToken should map to ErrUnregistered, got %v", err)
	}
}

func TestClassifyMapsGatewayEventsToKinds(t *testing.T) {
	cases := []struct {
		event    Event
		wantKind Kind
		wantBody string
		ok       bool
	}{
		{Event{Type: "approval.request", SessionID: "s", Payload: json.RawMessage(`{"command":"rm -rf x","request_id":"r"}`)}, KindApproval, "rm -rf x", true},
		{Event{Type: "clarify.request", Payload: json.RawMessage(`{"questions":[{"text":"Which one?"}]}`)}, KindInput, "Which one?", true},
		{Event{Type: "secret.request", Payload: json.RawMessage(`{"env_var":"OPENAI_API_KEY"}`)}, KindInput, "OPENAI_API_KEY", true},
		{Event{Type: "message.complete", SessionID: "s"}, KindTurnDone, "Deploy notes", true},
		{Event{Type: "error", Payload: json.RawMessage(`{"message":"boom"}`)}, KindTurnError, "boom", true},
		{Event{Type: "background.complete", Payload: json.RawMessage(`{"text":"done"}`)}, KindBackgroundDone, "done", true},
		{Event{Type: "notification.show", Payload: json.RawMessage(`{"kind":"credits","text":"Low credits"}`)}, KindCredits, "Low credits", true},
		{Event{Type: "notification.show", Payload: json.RawMessage(`{"kind":"agent","text":"Still starting"}`)}, "", "", false},
		{Event{Type: "message.delta"}, "", "", false},
	}
	for _, tc := range cases {
		alert, ok := Classify(tc.event, "Deploy notes")
		if ok != tc.ok {
			t.Errorf("%s: ok=%v want %v", tc.event.Type, ok, tc.ok)
			continue
		}
		if ok && (alert.Kind != tc.wantKind || alert.Body != tc.wantBody) {
			t.Errorf("%s: got %s %q, want %s %q", tc.event.Type, alert.Kind, alert.Body, tc.wantKind, tc.wantBody)
		}
	}
	long := Event{Type: "approval.request", Payload: json.RawMessage(`{"command":"` + strings.Repeat("x", 300) + `"}`)}
	alert, _ := Classify(long, "")
	if len(alert.Body) > 140+len("…") {
		t.Fatalf("body not clipped: %d", len(alert.Body))
	}
}

type fakeGateway struct {
	active string
	events map[string]string
}

func (f *fakeGateway) Call(_ context.Context, method string, params any) (json.RawMessage, error) {
	switch method {
	case "session.active_list":
		return json.RawMessage(f.active), nil
	case "session.events.since":
		p := params.(map[string]any)
		return json.RawMessage(f.events[p["session_id"].(string)]), nil
	}
	return nil, nil
}

type fakeSender struct{ sent []string }

func (f *fakeSender) Send(_ context.Context, token, _ string, a Alert) error {
	f.sent = append(f.sent, token+":"+string(a.Kind))
	return nil
}

func TestWatcherPushesOnlyNewEventsToOfflinePhonesThatWantThem(t *testing.T) {
	dir := t.TempDir()
	registry, err := OpenRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	_ = registry.Set("phone-offline", protocol.PushRegistration{Token: "tok1", Environment: "sandbox", Kinds: []string{"approval"}}, now)
	_ = registry.Set("phone-online", protocol.PushRegistration{Token: "tok2", Environment: "sandbox", Kinds: []string{"approval", "turnDone"}}, now)
	_ = registry.Set("phone-quiet", protocol.PushRegistration{Token: "tok3", Environment: "sandbox", Kinds: []string{"turnDone"}}, now)
	gateway := &fakeGateway{
		active: `{"sessions":[{"id":"s1","title":"Notes"}]}`,
		events: map[string]string{"s1": `{"events":[{"type":"approval.request","session_id":"s1","payload":{"command":"old"},"seq":3}]}`},
	}
	sender := &fakeSender{}
	w := &Watcher{Gateway: gateway, Sender: sender, Registry: registry, Online: func() []string { return []string{"phone-online"} }}

	// First observation of a session: history is not news.
	if err := w.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 0 {
		t.Fatalf("history should not be pushed: %v", sender.sent)
	}
	gateway.events["s1"] = `{"events":[{"type":"approval.request","session_id":"s1","payload":{"command":"new"},"seq":4},{"type":"message.complete","session_id":"s1","payload":{},"seq":5}]}`
	if err := w.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"tok1:approval", "tok3:turnDone"}
	if strings.Join(sender.sent, ",") != strings.Join(want, ",") {
		t.Fatalf("sent %v, want %v", sender.sent, want)
	}
	// Nothing new: nothing sent.
	if err := w.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 2 {
		t.Fatalf("repeat poll re-sent: %v", sender.sent)
	}
}

func TestRegistryRoundTripsAndWithdrawsOnEmptyToken(t *testing.T) {
	dir := t.TempDir()
	registry, err := OpenRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	if err := registry.Set("dev", protocol.PushRegistration{Token: "t", Environment: "production", Kinds: []string{"turnDone"}}, now); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	all := reopened.All()
	if len(all) != 1 || all[0].Token != "t" || !all[0].Wants(KindTurnDone) || all[0].Wants(KindApproval) {
		t.Fatalf("registry: %+v", all)
	}
	_ = reopened.Set("dev", protocol.PushRegistration{}, now)
	if len(reopened.All()) != 0 {
		t.Fatal("empty token should withdraw")
	}
	info, err := os.Stat(filepath.Join(dir, "push-devices.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("registry file mode %v", info.Mode().Perm())
	}
}

func TestConfigValidatesAppleIdentifiers(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadConfig(dir); err != ErrNotConfigured {
		t.Fatalf("missing config should be ErrNotConfigured, got %v", err)
	}
	bad := Config{TeamID: "short", KeyID: "KEYID12345", KeyPath: "k", BundleID: "b"}
	if err := bad.Save(dir); err == nil {
		t.Fatal("short team id should be rejected")
	}
	good := Config{TeamID: "TEAMID1234", KeyID: "KEYID12345", KeyPath: "k", BundleID: "b"}
	if err := good.Save(dir); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(dir)
	if err != nil || loaded != good {
		t.Fatalf("round trip: %+v %v", loaded, err)
	}
}
