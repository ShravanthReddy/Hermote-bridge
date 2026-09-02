// Package gateway supervises the dedicated `hermes serve` child the bridge
// owns: loopback bind, OS-assigned port, minted session token, restart on
// exit. `hermes update` never respawns `--port 0` backends (spike S11), so the
// bridge is the only thing that manages this process.
package gateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// State of the child as seen by the bridge.
type State string

const (
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateDown     State = "down"
)

// Options configure the supervisor.
type Options struct {
	Python     string // Hermes venv interpreter
	HermesHome string
	Logger     *slog.Logger
	// StartTimeout bounds the wait for the readiness sentinel (default 90 s).
	StartTimeout time.Duration
}

// Supervisor runs and restarts the gateway child.
type Supervisor struct {
	opts  Options
	token string

	mu       sync.RWMutex
	state    State
	port     int
	watchers []chan State

	stop chan struct{}
	done chan struct{}
	cmd  *exec.Cmd
}

var readyRe = regexp.MustCompile(`HERMES_(?:BACKEND|DASHBOARD)_READY port=(\d+)`)

// New creates a supervisor with a fresh 256-bit session token.
func New(opts Options) (*Supervisor, error) {
	if opts.Python == "" {
		return nil, errors.New("gateway: Python interpreter required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 90 * time.Second
	}
	tok := make([]byte, 32)
	if _, err := rand.Read(tok); err != nil {
		return nil, err
	}
	return &Supervisor{
		opts:  opts,
		token: hex.EncodeToString(tok),
		state: StateDown,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}, nil
}

// Token is the session token the child accepts (header, Bearer or ?token=).
func (s *Supervisor) Token() string { return s.token }

// Port returns the child's port and whether it is ready.
func (s *Supervisor) Port() (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.port, s.state == StateReady
}

// BaseURL is http://127.0.0.1:<port> when ready.
func (s *Supervisor) BaseURL() (string, bool) {
	p, ok := s.Port()
	return "http://127.0.0.1:" + strconv.Itoa(p), ok
}

// State returns the current state.
func (s *Supervisor) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Watch returns a channel that receives every state change (buffered; slow
// readers drop intermediate states).
func (s *Supervisor) Watch() <-chan State {
	ch := make(chan State, 8)
	s.mu.Lock()
	s.watchers = append(s.watchers, ch)
	s.mu.Unlock()
	return ch
}

func (s *Supervisor) setState(st State, port int) {
	s.mu.Lock()
	s.state, s.port = st, port
	watchers := append([]chan State(nil), s.watchers...)
	s.mu.Unlock()
	for _, w := range watchers {
		select {
		case w <- st:
		default:
		}
	}
}

// Run starts the supervision loop and blocks until ctx is cancelled or Stop
// is called. The child is terminated on exit.
func (s *Supervisor) Run(ctx context.Context) error {
	defer close(s.done)
	backoff := time.Second
	for {
		started := time.Now()
		err := s.runOnce(ctx)
		if ctx.Err() != nil || s.stopped() {
			return nil
		}
		if err != nil {
			s.opts.Logger.Warn("gateway child exited", "err", err)
		}
		s.setState(StateDown, 0)
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		s.opts.Logger.Info("restarting gateway child", "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		case <-s.stop:
			return nil
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Stop terminates the child and ends Run.
func (s *Supervisor) Stop() {
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	s.mu.Lock()
	cmd := s.cmd
	s.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(os.Interrupt)
	}
	<-s.done
}

func (s *Supervisor) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func (s *Supervisor) runOnce(ctx context.Context) error {
	s.setState(StateStarting, 0)
	cmd := exec.CommandContext(ctx, s.opts.Python, "-m", "hermes_cli.main", "serve",
		"--host", "127.0.0.1", "--port", "0", "--skip-build")
	cmd.Dir = s.opts.HermesHome
	venv := filepath.Dir(filepath.Dir(s.opts.Python))
	cmd.Env = append(os.Environ(),
		"HERMES_HOME="+s.opts.HermesHome,
		// A *loopback* public URL keeps the child ungated even when config.yaml
		// declares a tailnet dashboard.public_url (an empty value would be
		// treated as unset and fall back to config.yaml — spike S10).
		"HERMES_DASHBOARD_PUBLIC_URL=http://127.0.0.1",
		"HERMES_DASHBOARD_SESSION_TOKEN="+s.token,
		"VIRTUAL_ENV="+venv,
		"PATH="+filepath.Join(venv, "bin")+":"+os.Getenv("PATH"),
		"PYTHONUNBUFFERED=1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("gateway: start: %w", err)
	}
	s.mu.Lock()
	s.cmd = cmd
	s.mu.Unlock()
	s.opts.Logger.Info("gateway child started", "pid", cmd.Process.Pid)

	portCh := make(chan int, 1)
	go s.pump(stdout, "stdout", portCh)
	go s.pump(stderr, "stderr", nil)

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case port := <-portCh:
		if err := s.probe(ctx, port); err != nil {
			s.opts.Logger.Warn("gateway probe failed", "err", err)
			_ = cmd.Process.Kill()
			return <-waitErr
		}
		s.setState(StateReady, port)
		s.opts.Logger.Info("gateway ready", "port", port)
	case err := <-waitErr:
		return fmt.Errorf("gateway: exited before ready: %w", err)
	case <-time.After(s.opts.StartTimeout):
		_ = cmd.Process.Kill()
		<-waitErr
		return errors.New("gateway: readiness sentinel not seen in time")
	case <-s.stop:
		_ = cmd.Process.Signal(os.Interrupt)
		return <-waitErr
	}
	return <-waitErr
}

func (s *Supervisor) pump(r io.Reader, stream string, portCh chan<- int) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if portCh != nil {
			if m := readyRe.FindStringSubmatch(line); m != nil {
				if p, err := strconv.Atoi(m[1]); err == nil {
					select {
					case portCh <- p:
					default:
					}
				}
				portCh = nil
			}
		}
		if strings.TrimSpace(line) != "" {
			s.opts.Logger.Debug("gateway", "stream", stream, "line", line)
		}
	}
}

// probe confirms the child answers, is ungated, and honours our token.
// /api/status is public, so a token-protected route is what proves it.
func (s *Supervisor) probe(ctx context.Context, port int) error {
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(port)+"/api/sessions?limit=1", nil)
	req.Header.Set("X-Hermes-Session-Token", s.token)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("gateway child is gated or ignores the session token (HTTP %d)", resp.StatusCode)
	default:
		return fmt.Errorf("status %d", resp.StatusCode)
	}
}

// FindPython locates the Hermes venv interpreter: $HERMES_HOME/hermes-agent/venv,
// else the venv referenced by the `hermes` launcher on PATH.
func FindPython(hermesHome string) (string, error) {
	candidates := []string{filepath.Join(hermesHome, "hermes-agent", "venv", "bin", "python")}
	if launcher, err := exec.LookPath("hermes"); err == nil {
		if raw, err := os.ReadFile(launcher); err == nil {
			re := regexp.MustCompile(`"([^"]*venv/bin/python[0-9.]*)"`)
			if m := re.FindStringSubmatch(string(raw)); m != nil {
				candidates = append(candidates, m[1])
			}
		}
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			if out, err := exec.Command(c, "-c", "import hermes_cli").CombinedOutput(); err == nil {
				return c, nil
			} else if len(out) > 0 {
				continue
			}
		}
	}
	return "", errors.New("Hermes Agent not found; install it first (https://hermes-agent.nousresearch.com) or set HERMES_HOME")
}
