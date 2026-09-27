package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ShravanthReddy/Hermote-bridge/internal/gateway"
	"github.com/ShravanthReddy/Hermote-bridge/internal/protocol"
	"github.com/ShravanthReddy/Hermote-bridge/internal/update"
)

// Bridge-local routes (docs/specs/bridge-status-updates.md D1, §3): paths
// under /bridge/ are answered by the bridge itself and never forwarded to
// the gateway. They arrive on the http channel, so they are read only while
// the tunnel is up, which needs Hermes connected (D2).

// bridgeRoutePrefix is reserved for bridge-local routes.
const bridgeRoutePrefix = "/bridge/"

// UnknownBridgeRouteError is the 404 body for a path under /bridge/ this
// bridge does not serve.
const UnknownBridgeRouteError = `{"error":"unknown bridge route"}`

// bridgeAPIVersion is caps.bridge.api. Within API 1 fields are only added.
const bridgeAPIVersion = 1

// StatusProvider supplies what only the daemon knows: its capability and its
// status. Server.Status holds one.
type StatusProvider interface {
	BridgeCapability() protocol.BridgeCapability
	BridgeStatus(ctx context.Context) BridgeStatus
}

// UpdateChecker is implemented by a StatusProvider that also checks for
// releases (GET /bridge/v1/update/check). The phone's only input is force.
type UpdateChecker interface {
	CheckForUpdate(ctx context.Context, force bool) update.CheckResponse
}

// NewBridgeCapability is the capability a Phase 1 bridge advertises in
// `ctl accepted`: status served, no update engine.
func NewBridgeCapability(version, incarnation string) protocol.BridgeCapability {
	return protocol.BridgeCapability{
		API: bridgeAPIVersion, Version: version, Incarnation: incarnation,
		Features: protocol.BridgeFeatures{Status: true, Update: false},
	}
}

// StatusFacts are the daemon's facts a status reply is built from.
type StatusFacts struct {
	Version       string
	Incarnation   string
	StartedAt     time.Time
	GOOS          string
	GOARCH        string
	Transport     protocol.Transport
	RelayURL      string
	RelayAttached bool
	Install       update.Install
	// ServiceLabel is the service manager's job label; empty when no service
	// manager runs this bridge (a foreground daemon).
	ServiceLabel string
	Gateway      gateway.Health
	Paired       int
	Connected    int
}

// BridgeStatus is the reply to GET /bridge/v1/status (§3). Optional values
// are pointers so they encode as null rather than being left out.
type BridgeStatus struct {
	API         int                `json:"api"`
	Version     string             `json:"version"`
	Incarnation string             `json:"incarnation"`
	StartedAt   time.Time          `json:"started_at"`
	Platform    StatusPlatform     `json:"platform"`
	Transport   protocol.Transport `json:"transport"`
	Relay       *StatusRelay       `json:"relay"`
	Install     StatusInstall      `json:"install"`
	Service     *StatusService     `json:"service"`
	Gateway     StatusGateway      `json:"gateway"`
	Devices     StatusDevices      `json:"devices"`
	Update      StatusUpdate       `json:"update"`
}

// StatusPlatform is the bridge's GOOS and GOARCH.
type StatusPlatform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

// StatusRelay is present only for the relay transport.
type StatusRelay struct {
	URL      string `json:"url"`
	Attached bool   `json:"attached"`
}

// StatusInstall is how the bridge was installed and the path it runs from.
type StatusInstall struct {
	Method update.Method `json:"method"`
	Path   string        `json:"path"`
}

// StatusService names the service manager job running the bridge.
type StatusService struct {
	Manager string `json:"manager"`
	Label   string `json:"label"`
}

// StatusGateway is the Hermes child's health as the supervisor records it.
type StatusGateway struct {
	State             gateway.State          `json:"state"`
	Since             *time.Time             `json:"since"`
	LastProbeOKAt     *time.Time             `json:"last_probe_ok_at"`
	Restarts          int                    `json:"restarts"`
	LastRestartReason *gateway.RestartReason `json:"last_restart_reason"`
}

// StatusDevices counts paired phones and live phone connections.
type StatusDevices struct {
	Paired    int `json:"paired"`
	Connected int `json:"connected"`
}

// StatusUpdate is whether this install can be updated from the phone.
// Current and Last are update records (§3); a bridge without the update
// engine has none, so they encode as null.
type StatusUpdate struct {
	CanApply      bool            `json:"can_apply"`
	Unavailable   *update.Reason  `json:"unavailable"`
	UpdateCommand string          `json:"update_command"`
	Current       json.RawMessage `json:"current"`
	Last          json.RawMessage `json:"last"`
}

// NewBridgeStatus builds the status reply from the daemon's facts. Times are
// RFC 3339 UTC to the second (§3).
func NewBridgeStatus(f StatusFacts) BridgeStatus {
	status := BridgeStatus{
		API: bridgeAPIVersion, Version: f.Version, Incarnation: f.Incarnation,
		StartedAt: wireTime(f.StartedAt),
		Platform:  StatusPlatform{OS: f.GOOS, Arch: f.GOARCH},
		Transport: f.Transport,
		Install:   StatusInstall{Method: f.Install.Method, Path: f.Install.Path},
		Gateway: StatusGateway{
			State: f.Gateway.State, Since: optionalTime(f.Gateway.Since),
			LastProbeOKAt: optionalTime(f.Gateway.LastProbeOK), Restarts: f.Gateway.Restarts,
		},
		Devices: StatusDevices{Paired: f.Paired, Connected: f.Connected},
	}
	if f.Transport == protocol.TransportRelay {
		status.Relay = &StatusRelay{URL: f.RelayURL, Attached: f.RelayAttached}
	}
	if f.ServiceLabel != "" {
		status.Service = &StatusService{Manager: "launchd", Label: f.ServiceLabel}
	}
	if reason := f.Gateway.LastRestartReason; reason != "" {
		status.Gateway.LastRestartReason = &reason
	}
	canApply, unavailable := f.Install.Availability()
	status.Update = StatusUpdate{CanApply: canApply, UpdateCommand: f.Install.UpdateCommand}
	if unavailable != "" {
		status.Update.Unavailable = &unavailable
	}
	return status
}

func wireTime(t time.Time) time.Time { return t.UTC().Truncate(time.Second) }

func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	wire := wireTime(t)
	return &wire
}

// bridgeHandler answers one bridge-local route with a status and JSON body.
type bridgeHandler func(ctx context.Context, provider StatusProvider, query string) (int, json.RawMessage)

// bridgeRoutes is the exact method and path table; nothing is matched by prefix.
var bridgeRoutes = map[string]bridgeHandler{
	http.MethodGet + " /bridge/v1/status":       serveBridgeStatus,
	http.MethodGet + " /bridge/v1/update/check": serveUpdateCheck,
}

func isBridgeRoute(path string) bool { return strings.HasPrefix(path, bridgeRoutePrefix) }

// startBridgeRoute answers a validated bridge-local request under the same
// HTTP admission pools as proxied routes.
func (c *conn) startBridgeRoute(
	ctx context.Context, req protocol.HTTPRequest, path canonicalPath, fail func(error),
) error {
	handler, ok := bridgeRoutes[req.Method+" "+path.Path]
	if !ok {
		return c.replyHTTP(ctx, req.ID, http.StatusNotFound, json.RawMessage(UnknownBridgeRouteError), fail)
	}
	lease, result := acquirePools(ctx, c.srv.deps.httpAdmissionWait, c.httpSlots, c.srv.httpSlots)
	switch result {
	case admissionCanceled:
		return ctx.Err()
	case admissionBusy:
		return c.replyHTTP(
			ctx, req.ID, http.StatusTooManyRequests,
			responseBody("bridge busy", "too many bridge HTTP requests"), fail,
		)
	default:
		go func() {
			status, body := handler(ctx, c.srv.Status, req.Query)
			c.finishHTTP(ctx, req.ID, status, body, lease, fail)
		}()
		return nil
	}
}

func serveBridgeStatus(ctx context.Context, provider StatusProvider, _ string) (int, json.RawMessage) {
	return bridgeReply(provider.BridgeStatus(ctx))
}

// serveUpdateCheck forces a check only for the literal query "force=true".
func serveUpdateCheck(ctx context.Context, provider StatusProvider, query string) (int, json.RawMessage) {
	checker, ok := provider.(UpdateChecker)
	if !ok {
		return http.StatusNotFound, json.RawMessage(UnknownBridgeRouteError)
	}
	return bridgeReply(checker.CheckForUpdate(ctx, query == "force=true"))
}

// bridgeReply encodes a 200 body without HTML escaping, so a command such as
// `brew upgrade … && …` reaches the phone as written.
func bridgeReply(value any) (int, json.RawMessage) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return http.StatusInternalServerError, responseBody("bridge route failed", "")
	}
	return http.StatusOK, bytes.TrimRight(body.Bytes(), "\n")
}
