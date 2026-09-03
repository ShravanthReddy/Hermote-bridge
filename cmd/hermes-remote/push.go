package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ShravanthReddy/hermes-remote/internal/launchd"
	"github.com/ShravanthReddy/hermes-remote/internal/push"
	"github.com/ShravanthReddy/hermes-remote/internal/state"
)

const pushUsage = `hermes-remote push — notify paired phones while they are away

  push setup --team-id TEAM --key-id KEY --p8 AuthKey_KEY.p8 [--bundle-id ID]
      Store an APNs authentication key (Certificates, Identifiers & Profiles ▸ Keys,
      with "Apple Push Notifications service" enabled). Copies the key into the
      hermes-remote state directory and restarts the daemon.
  push status   Show the key in use and the phones registered for notifications.
  push test     Send a test notification to every registered phone.
`

const defaultBundleID = "com.shravanthreddy.hermes"

func cmdPush(args []string) error {
	if len(args) == 0 {
		fmt.Print(pushUsage)
		return nil
	}
	switch args[0] {
	case "setup":
		return cmdPushSetup(args[1:])
	case "status":
		return cmdPushStatus()
	case "test":
		return cmdPushTest()
	}
	fmt.Print(pushUsage)
	return fmt.Errorf("unknown push command %q", args[0])
}

func cmdPushSetup(args []string) error {
	fs := flag.NewFlagSet("push setup", flag.ContinueOnError)
	teamID := fs.String("team-id", "", "Apple Developer team id")
	keyID := fs.String("key-id", "", "APNs key id")
	p8 := fs.String("p8", "", "path to the AuthKey_<KEY>.p8 file")
	bundleID := fs.String("bundle-id", defaultBundleID, "app bundle identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *teamID == "" || *keyID == "" || *p8 == "" {
		fmt.Print(pushUsage)
		return errors.New("--team-id, --key-id and --p8 are required")
	}
	store, err := state.Open()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(*p8)
	if err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	dest := store.Path("apns.p8")
	if err := os.WriteFile(dest, raw, 0o600); err != nil {
		return err
	}
	cfg := push.Config{TeamID: *teamID, KeyID: *keyID, KeyPath: dest, BundleID: *bundleID}
	if _, err := push.NewClient(cfg); err != nil {
		_ = os.Remove(dest)
		return err
	}
	if err := cfg.Save(store.Path("")); err != nil {
		return err
	}
	bold("Push")
	ok("APNs key %s for team %s stored at %s", cfg.KeyID, cfg.TeamID, dest)
	ok("Notifications go to %s", cfg.BundleID)
	fmt.Println("  Restarting the daemon so the watcher starts…")
	if err := withCtxRestart(); err != nil {
		warn("restart failed (%v) — run `hermes-remote restart`", err)
	}
	return nil
}

func cmdPushStatus() error {
	store, err := state.Open()
	if err != nil {
		return err
	}
	bold("Push")
	cfg, err := push.LoadConfig(store.Path(""))
	switch {
	case errors.Is(err, push.ErrNotConfigured):
		warn("No APNs key — run `hermes-remote push setup`")
	case err != nil:
		return err
	default:
		ok("APNs key %s, team %s, topic %s", cfg.KeyID, cfg.TeamID, cfg.BundleID)
	}
	registry, err := push.OpenRegistry(store.Path(""))
	if err != nil {
		return err
	}
	entries := registry.All()
	if len(entries) == 0 {
		fmt.Println("  No phones registered for notifications yet (open the app once, allow notifications).")
		return nil
	}
	fmt.Println("  Registered phones:")
	for _, e := range entries {
		fmt.Printf("    %s  %s  %s  kinds: %s\n", short8(e.DeviceID), e.Environment,
			e.UpdatedAt.Local().Format("Jan _2 15:04"), strings.Join(e.Kinds, ","))
	}
	return nil
}

func cmdPushTest() error {
	store, err := state.Open()
	if err != nil {
		return err
	}
	cfg, err := push.LoadConfig(store.Path(""))
	if err != nil {
		return err
	}
	client, err := push.NewClient(cfg)
	if err != nil {
		return err
	}
	registry, err := push.OpenRegistry(store.Path(""))
	if err != nil {
		return err
	}
	entries := registry.All()
	if len(entries) == 0 {
		return errors.New("no phones registered for notifications")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bold("Push test")
	for _, e := range entries {
		alert := push.Alert{
			Kind: push.KindTurnDone, Title: "Hermes", Body: "Test notification from your Mac.", Category: push.CategoryTurn,
		}
		if err := client.Send(ctx, e.Token, e.Environment, alert); err != nil {
			fail("%s (%s): %v", short8(e.DeviceID), e.Environment, err)
			continue
		}
		ok("sent to %s (%s)", short8(e.DeviceID), e.Environment)
	}
	return nil
}

func short8(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// withCtxRestart restarts the LaunchAgent when it is installed; a daemon run
// by hand is left alone.
func withCtxRestart() error {
	if _, err := os.Stat(filepath.Join(os.Getenv("HOME"), "Library", "LaunchAgents", "ai.hermes.remote.plist")); err != nil {
		return nil
	}
	return withCtx(launchd.Restart)
}
