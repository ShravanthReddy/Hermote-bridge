package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

func TestChooseTransport(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  protocol.Transport
	}{
		{"default is direct", "\n", protocol.TransportDirect},
		{"1 is direct", "1\n", protocol.TransportDirect},
		{"word direct", "Direct\n", protocol.TransportDirect},
		{"2 is relay", "2\n", protocol.TransportRelay},
		{"word relay", " relay \n", protocol.TransportRelay},
		{"retries after a bad answer", "maybe\n2\n", protocol.TransportRelay},
		{"three bad answers fall back to direct", "a\nb\nc\n", protocol.TransportDirect},
		{"EOF falls back to direct", "", protocol.TransportDirect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			got := chooseTransport(strings.NewReader(tc.input), &out, true, true)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			if !strings.Contains(out.String(), "Hosted relay") {
				t.Fatalf("prompt should describe the relay option:\n%s", out.String())
			}
		})
	}
}

func TestChooseTransportNonInteractiveIgnoresInput(t *testing.T) {
	got := chooseTransport(strings.NewReader("2\n"), io.Discard, false, true)
	if got != protocol.TransportDirect {
		t.Fatalf("non-interactive run must default to direct, got %q", got)
	}
}

func TestChooseTransportMentionsMissingTailscale(t *testing.T) {
	var out bytes.Buffer
	_ = chooseTransport(strings.NewReader("\n"), &out, true, false)
	if !strings.Contains(out.String(), "Not installed yet") {
		t.Fatalf("prompt should say Tailscale is missing:\n%s", out.String())
	}
}
