package push

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Kind is a notification kind a phone can opt into (the desktop's native
// notification kinds).
type Kind string

// Kinds.
const (
	KindApproval       Kind = "approval"
	KindInput          Kind = "input"
	KindTurnDone       Kind = "turnDone"
	KindTurnError      Kind = "turnError"
	KindBackgroundDone Kind = "backgroundDone"
	KindCredits        Kind = "credits"
)

// Categories the app registers; actions hang off them (Approve / Deny).
const (
	CategoryApproval = "APPROVAL"
	CategoryInput    = "INPUT"
	CategoryTurn     = "TURN"
)

// Event is one replayed gateway event (`session.events.since`).
type Event struct {
	Type      string          `json:"type"`
	SessionID string          `json:"session_id"`
	Payload   json.RawMessage `json:"payload"`
	Seq       int             `json:"seq"`
}

// Classify maps a gateway event to the alert a phone should get, if any.
// `title` is the session's title for the alert body when the event carries
// nothing better.
func Classify(ev Event, title string) (Alert, bool) {
	var p struct {
		Command   string `json:"command"`
		RequestID string `json:"request_id"`
		Prompt    string `json:"prompt"`
		Questions []struct {
			Text string `json:"text"`
		} `json:"questions"`
		EnvVar  string `json:"env_var"`
		Server  string `json:"server"`
		Message string `json:"message"`
		Text    string `json:"text"`
		Kind    string `json:"kind"`
	}
	_ = json.Unmarshal(ev.Payload, &p)
	base := Alert{SessionID: ev.SessionID, RequestID: p.RequestID}
	switch ev.Type {
	case "approval.request":
		base.Kind, base.Category = KindApproval, CategoryApproval
		base.Title, base.Body = "Approval needed", clip(p.Command, 140)
		base.CollapseID = "approval:" + ev.SessionID
	case "clarify.request":
		base.Kind, base.Category = KindInput, CategoryInput
		base.Title = "Hermes has a question"
		if len(p.Questions) > 0 {
			base.Body = clip(p.Questions[0].Text, 140)
		} else {
			base.Body = clip(p.Prompt, 140)
		}
	case "sudo.request":
		base.Kind, base.Category = KindInput, CategoryInput
		base.Title, base.Body = "Hermes needs the Mac's administrator password", clip(p.Command, 140)
	case "secret.request":
		base.Kind, base.Category = KindInput, CategoryInput
		base.Title, base.Body = "Hermes needs a credential", p.EnvVar
	case "mcp.setup.request":
		base.Kind, base.Category = KindInput, CategoryInput
		base.Title, base.Body = "Hermes wants to set up an MCP server", p.Server
	case "message.complete":
		base.Kind, base.Category = KindTurnDone, CategoryTurn
		base.Title, base.Body = "Hermes finished", fallback(title, "Your reply is ready.")
		base.CollapseID = "turn:" + ev.SessionID
	case "error":
		base.Kind, base.Category = KindTurnError, CategoryTurn
		base.Title, base.Body = "Turn failed", clip(fallback(p.Message, title), 140)
		base.CollapseID = "turn:" + ev.SessionID
	case "background.complete":
		base.Kind, base.Category = KindBackgroundDone, CategoryTurn
		base.Title, base.Body = "Background task finished", clip(fallback(p.Text, title), 140)
	case "notification.show":
		if p.Kind != "credits" {
			return Alert{}, false
		}
		base.Kind = KindCredits
		base.Title, base.Body = "Hermes credits", clip(p.Text, 140)
	default:
		return Alert{}, false
	}
	return base, true
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func fallback(s, alt string) string {
	if strings.TrimSpace(s) == "" {
		return alt
	}
	return s
}

// Gateway is the slice of the loopback gateway the watcher uses.
type Gateway interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
}

// Sender delivers alerts (the APNs client, or a fake in tests).
type Sender interface {
	Send(ctx context.Context, deviceToken, environment string, a Alert) error
}

// Watcher polls the gateway while at least one registered phone is offline
// and pushes what it finds. It never resumes a session — that would steal the
// live event stream from whichever client holds it — so it reads the replay
// ring (`session.events.since`) instead. Events seen before a session was
// first observed are not replayed as alerts.
type Watcher struct {
	Gateway  Gateway
	Sender   Sender
	Registry *Registry
	// Online lists device ids currently connected to the bridge; those
	// phones see events live and get no push.
	Online   func() []string
	Interval time.Duration
	Logger   *slog.Logger

	seen map[string]int // session id → last seq handled
}

// Run polls until ctx ends.
func (w *Watcher) Run(ctx context.Context) {
	if w.Interval == 0 {
		w.Interval = 3 * time.Second
	}
	w.seen = map[string]int{}
	ticker := time.NewTicker(w.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Poll(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.log().Debug("push poll", "err", err)
			}
		}
	}
}

// Poll runs one round: skipped when every registered phone is connected.
func (w *Watcher) Poll(ctx context.Context) error {
	offline := w.offlineDevices()
	if len(offline) == 0 {
		return nil
	}
	raw, err := w.Gateway.Call(ctx, "session.active_list", map[string]any{})
	if err != nil {
		return err
	}
	var active struct {
		Sessions []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
		} `json:"sessions"`
	}
	if err := json.Unmarshal(raw, &active); err != nil {
		return fmt.Errorf("session.active_list: %w", err)
	}
	if w.seen == nil {
		w.seen = map[string]int{}
	}
	for _, s := range active.Sessions {
		last, known := w.seen[s.ID]
		events, err := w.eventsSince(ctx, s.ID, last)
		if err != nil {
			w.log().Debug("push events", "session", s.ID, "err", err)
			continue
		}
		for _, ev := range events {
			// The ring may replay what was already handled; only newer seqs count.
			if ev.Seq <= last {
				continue
			}
			if ev.Seq > w.seen[s.ID] {
				w.seen[s.ID] = ev.Seq
			}
			if !known {
				continue // a session seen for the first time: history is not news
			}
			alert, ok := Classify(ev, s.Title)
			if !ok {
				continue
			}
			w.deliver(ctx, offline, alert)
		}
		if !known {
			w.seen[s.ID] = maxSeq(events)
		}
	}
	return nil
}

func (w *Watcher) eventsSince(ctx context.Context, sessionID string, lastSeen int) ([]Event, error) {
	raw, err := w.Gateway.Call(ctx, "session.events.since", map[string]any{"session_id": sessionID, "last_seen": lastSeen})
	if err != nil {
		return nil, err
	}
	var reply struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal(raw, &reply); err != nil {
		return nil, err
	}
	return reply.Events, nil
}

func (w *Watcher) deliver(ctx context.Context, offline []Entry, alert Alert) {
	for _, entry := range offline {
		if !entry.Wants(alert.Kind) {
			continue
		}
		if alert.Kind == KindTurnDone && entry.Sound != "" {
			alert.Sound = entry.Sound // the phone's chosen completion chime
		}
		err := w.Sender.Send(ctx, entry.Token, entry.Environment, alert)
		switch {
		case err == nil:
			w.log().Info("pushed", "kind", alert.Kind, "device", short(entry.DeviceID))
		case errors.Is(err, ErrUnregistered):
			w.log().Info("push token unregistered; dropping", "device", short(entry.DeviceID))
			_ = w.Registry.Remove(entry.DeviceID)
		default:
			w.log().Warn("push failed", "device", short(entry.DeviceID), "err", err)
		}
	}
}

func (w *Watcher) log() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}

// offlineDevices is every registration whose phone is not connected now.
func (w *Watcher) offlineDevices() []Entry {
	online := map[string]bool{}
	if w.Online != nil {
		for _, id := range w.Online() {
			online[id] = true
		}
	}
	var out []Entry
	for _, entry := range w.Registry.All() {
		if !online[entry.DeviceID] && len(entry.Kinds) > 0 {
			out = append(out, entry)
		}
	}
	return out
}

func maxSeq(events []Event) int {
	m := 0
	for _, ev := range events {
		if ev.Seq > m {
			m = ev.Seq
		}
	}
	return m
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
