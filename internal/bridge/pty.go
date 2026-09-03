package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/creack/pty"

	"github.com/ShravanthReddy/hermes-remote/internal/protocol"
)

// Terminals over the tunnel (plan 10 / WP6, ADR-028). Each phone connection
// owns the shells it opened; they die with the connection. The shell is the
// user's login shell in the requested folder, with a sane TERM, so the phone
// sees exactly what a Terminal window on the Mac would.

const (
	maxTerminals   = 8
	ptyReadChunk   = 32 * 1024
	defaultPTYCols = 80
	defaultPTYRows = 24
)

// shellCommand resolves the login shell: $SHELL, else zsh, else sh.
func shellCommand() string {
	if sh := strings.TrimSpace(os.Getenv("SHELL")); sh != "" {
		return sh
	}
	for _, candidate := range []string{"/bin/zsh", "/bin/bash", "/bin/sh"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "/bin/sh"
}

type terminal struct {
	cmd *exec.Cmd
	tty *os.File
}

// terminalManager runs the shells of one phone connection.
type terminalManager struct {
	send  func(ctx context.Context, v any) error
	shell string
	mu    sync.Mutex
	open  map[string]*terminal
}

func newTerminalManager(send func(ctx context.Context, v any) error) *terminalManager {
	return &terminalManager{send: send, shell: shellCommand(), open: map[string]*terminal{}}
}

// handle applies one phone→bridge pty frame.
func (m *terminalManager) handle(ctx context.Context, msg protocol.PTYMessage) error {
	if msg.ID == "" {
		return errors.New("pty frame without id")
	}
	switch msg.Op {
	case protocol.PTYOpen:
		return m.openTerminal(ctx, msg)
	case protocol.PTYData:
		return m.write(msg.ID, msg.Data)
	case protocol.PTYResize:
		return m.resize(msg.ID, msg.Cols, msg.Rows)
	case protocol.PTYClose:
		m.closeTerminal(msg.ID)
		return nil
	}
	return fmt.Errorf("pty: unknown op %q", msg.Op)
}

func (m *terminalManager) openTerminal(ctx context.Context, msg protocol.PTYMessage) error {
	m.mu.Lock()
	if _, exists := m.open[msg.ID]; exists {
		m.mu.Unlock()
		return m.refuse(ctx, msg.ID, "terminal id already open")
	}
	if len(m.open) >= maxTerminals {
		m.mu.Unlock()
		return m.refuse(ctx, msg.ID, "too many terminals")
	}
	m.mu.Unlock()

	cols, rows := msg.Cols, msg.Rows
	if cols <= 0 {
		cols = defaultPTYCols
	}
	if rows <= 0 {
		rows = defaultPTYRows
	}
	cwd := msg.Cwd
	if cwd == "" || !isDir(cwd) {
		cwd, _ = os.UserHomeDir()
	}
	cmd := exec.Command(m.shell, "-l")
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "TERM=xterm-256color", "COLORTERM=truecolor", "LANG=en_US.UTF-8")
	tty, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
	if err != nil {
		return m.refuse(ctx, msg.ID, "could not start the shell: "+err.Error())
	}
	t := &terminal{cmd: cmd, tty: tty}
	m.mu.Lock()
	m.open[msg.ID] = t
	m.mu.Unlock()
	go m.pump(ctx, msg.ID, t)
	return nil
}

// pump copies shell output to the phone until the shell ends, then reports
// the exit code and forgets the terminal.
func (m *terminalManager) pump(ctx context.Context, id string, t *terminal) {
	buf := make([]byte, ptyReadChunk)
	for {
		n, err := t.tty.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := m.send(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYData, ID: id, Data: data}); sendErr != nil {
				break
			}
		}
		if err != nil {
			break // EOF (shell exited) or the tty was closed by closeTerminal
		}
	}
	code := 0
	if err := t.cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	_ = t.tty.Close()
	m.mu.Lock()
	delete(m.open, id)
	m.mu.Unlock()
	_ = m.send(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYExit, ID: id, Code: code})
}

func (m *terminalManager) write(id string, data []byte) error {
	m.mu.Lock()
	t := m.open[id]
	m.mu.Unlock()
	if t == nil {
		return nil // already gone; the phone will see exit/close
	}
	_, err := t.tty.Write(data)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil // the pump reports the end
	}
	return nil
}

func (m *terminalManager) resize(id string, cols, rows int) error {
	m.mu.Lock()
	t := m.open[id]
	m.mu.Unlock()
	if t == nil || cols <= 0 || rows <= 0 {
		return nil
	}
	return pty.Setsize(t.tty, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
}

// closeTerminal ends one shell: hang up the tty and let the pump report exit.
func (m *terminalManager) closeTerminal(id string) {
	m.mu.Lock()
	t := m.open[id]
	m.mu.Unlock()
	if t == nil {
		return
	}
	_ = t.tty.Close()
	if t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
}

// closeAll ends every shell of this connection (called when the phone drops).
func (m *terminalManager) closeAll() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.open))
	for id := range m.open {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.closeTerminal(id)
	}
}

func (m *terminalManager) refuse(ctx context.Context, id, reason string) error {
	return m.send(ctx, protocol.PTYMessage{Ch: protocol.ChPTY, Op: protocol.PTYClose, ID: id, Reason: reason})
}

func isDir(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
