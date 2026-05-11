// Copyright Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package interpreter

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daytonaio/session-daemon/internal/config"
)

//go:embed repl_host.js
var typescriptHostScript string

// TSFactory creates one shared Node host process per daemon, then carves out
// per-context "workers" that multiplex over the host's stdin/stdout JSON-line
// protocol. This is the V8-session strategy described in plan §3 — many
// contexts in one host means ~10MB per context vs ~50MB for subprocess workers.
type TSFactory struct {
	cfg    *config.Config
	logger *slog.Logger

	mu          sync.Mutex
	host        *tsHost
	hostScript  string
	pkgsCache   []PackageInfo
	pkgsAt      time.Time
	replyMu     sync.Mutex
	pendingReps map[string]chan *WorkerChunk
	replyN      atomic.Uint64
}

func NewTSFactory(cfg *config.Config, logger *slog.Logger) (*TSFactory, error) {
	dir := cfg.HostScriptCacheDir
	if dir == "" {
		dir = os.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create host script dir: %w", err)
	}
	scriptPath := filepath.Join(dir, "daytona_session_repl_host.js")
	if err := os.WriteFile(scriptPath, []byte(typescriptHostScript), workerScriptPerms); err != nil {
		return nil, fmt.Errorf("write host script: %w", err)
	}
	return &TSFactory{
		cfg:         cfg,
		logger:      logger.With(slog.String("component", "ts_factory")),
		hostScript:  scriptPath,
		pendingReps: make(map[string]chan *WorkerChunk),
	}, nil
}

func (f *TSFactory) Create(ctxID string, req CreateSessionRequest, onChunk func(*WorkerChunk)) (Worker, error) {
	host, err := f.ensureHost()
	if err != nil {
		return nil, err
	}

	memMB := req.MemoryLimitMB
	if memMB <= 0 {
		memMB = f.cfg.TSDefaultMemoryMB
	}
	if memMB > f.cfg.TSMaxMemoryMB {
		return nil, fmt.Errorf("memoryLimitMb %d exceeds cap %d", memMB, f.cfg.TSMaxMemoryMB)
	}

	host.register(ctxID, onChunk)
	if err := host.send(WorkerCommand{Op: "create", SessionID: ctxID, MemoryLimitMB: memMB}); err != nil {
		host.unregister(ctxID)
		return nil, err
	}

	w := &tsHostWorker{ctxID: ctxID, host: host}
	w.active.Store(true)
	return w, nil
}

func (f *TSFactory) ListPackages() ([]PackageInfo, error) {
	host, err := f.ensureHost()
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if time.Since(f.pkgsAt) < 5*time.Minute && f.pkgsCache != nil {
		out := f.pkgsCache
		f.mu.Unlock()
		return out, nil
	}
	f.mu.Unlock()

	replyID := fmt.Sprintf("list-packages-%d", f.replyN.Add(1))
	ch := make(chan *WorkerChunk, 1)
	f.replyMu.Lock()
	f.pendingReps[replyID] = ch
	f.replyMu.Unlock()
	defer func() {
		f.replyMu.Lock()
		delete(f.pendingReps, replyID)
		f.replyMu.Unlock()
	}()

	if err := host.send(WorkerCommand{Op: "list-packages", Reply: replyID}); err != nil {
		return nil, err
	}

	select {
	case chunk := <-ch:
		f.mu.Lock()
		f.pkgsCache = chunk.Packages
		f.pkgsAt = time.Now()
		f.mu.Unlock()
		return chunk.Packages, nil
	case <-time.After(15 * time.Second):
		return nil, errors.New("ts host: list-packages timeout")
	}
}

func (f *TSFactory) Shutdown() {
	f.mu.Lock()
	host := f.host
	f.host = nil
	f.mu.Unlock()
	if host != nil {
		host.shutdown()
	}
}

func (f *TSFactory) ensureHost() (*tsHost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.host != nil && f.host.active.Load() {
		return f.host, nil
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(parentCtx, f.cfg.NodeInterpreter, f.hostScript)
	// NODE_PATH points Node's module resolver at the host-side node_modules baked
	// into the image (isolated-vm + esbuild-wasm). The script itself is written
	// to a temp dir for portability, so we must explicitly tell Node where its
	// dependencies live — adjacent-directory resolution from /tmp does not find
	// /usr/lib/daytona/repl_host/node_modules.
	hostNodeModules := filepath.Join(f.cfg.NodeBundleRoot, "node_modules")
	cmd.Env = append(os.Environ(),
		"SESSION_DAEMON_USER_NODE_MODULES_ROOT="+f.cfg.WorkspaceRoot,
		"NODE_PATH="+hostNodeModules,
	)
	cmd.Dir = f.cfg.NodeBundleRoot

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		cancel()
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		cancel()
		return nil, fmt.Errorf("start ts host: %w", err)
	}

	host := &tsHost{
		factory:   f,
		cmd:       cmd,
		stdin:     stdin,
		stdout:    stdout,
		cancel:    cancel,
		done:      make(chan struct{}),
		logger:    f.logger.With(slog.String("component", "ts_host"), slog.Int("pid", cmd.Process.Pid)),
		listeners: make(map[string]func(*WorkerChunk)),
	}
	host.active.Store(true)

	go host.readLoop()
	go host.waitLoop()
	f.host = host
	return host, nil
}

// tsHost is the long-lived Node process. It demuxes incoming chunks by
// `sessionId` and dispatches them to the chunk handler the corresponding
// worker registered at create time.
type tsHost struct {
	factory *TSFactory
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	cancel  context.CancelFunc
	done    chan struct{}
	logger  *slog.Logger

	writeMu sync.Mutex

	mu        sync.Mutex
	listeners map[string]func(*WorkerChunk)
	active    activeFlag
}

func (h *tsHost) register(ctxID string, onChunk func(*WorkerChunk)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.listeners[ctxID] = onChunk
}

func (h *tsHost) unregister(ctxID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.listeners, ctxID)
}

func (h *tsHost) send(cmd WorkerCommand) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	if !h.active.Load() {
		return errors.New("ts host: not active")
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := h.stdin.Write(data); err != nil {
		return err
	}
	return nil
}

func (h *tsHost) shutdown() {
	if !h.active.Swap(false) {
		return
	}
	if h.stdin != nil {
		_ = h.stdin.Close()
	}
	if h.cmd != nil && h.cmd.Process != nil {
		_ = h.cmd.Process.Signal(os.Interrupt)
	}
	t := time.NewTimer(gracePeriod)
	defer t.Stop()
	select {
	case <-h.done:
	case <-t.C:
		if h.cmd != nil && h.cmd.Process != nil {
			_ = h.cmd.Process.Kill()
		}
	}
	if h.cancel != nil {
		h.cancel()
	}
}

// Swap behavior for activeFlag (compat helper).
func (a *activeFlag) Swap(v bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	prev := a.v
	a.v = v
	return prev
}

func (h *tsHost) readLoop() {
	scanner := bufio.NewScanner(h.stdout)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var chunk WorkerChunk
		if err := json.Unmarshal(line, &chunk); err != nil {
			h.logger.Warn("malformed ts host chunk", slog.String("error", err.Error()))
			continue
		}
		// Reply chunks (e.g., list-packages) are routed to the factory's reply table.
		if chunk.Type == ChunkTypeControl && chunk.Text == "list-packages-result" {
			h.factory.replyMu.Lock()
			ch := h.factory.pendingReps[chunk.Reply]
			h.factory.replyMu.Unlock()
			if ch != nil {
				ch <- &chunk
			}
			continue
		}
		// Lifecycle control chunks ("created"/"deleted"/"interrupted"/"host-ready") are
		// not user-visible; only "completed" must reach the per-context handler.
		if chunk.Type == ChunkTypeControl && chunk.Text != ControlChunkTypeCompleted &&
			chunk.Text != ControlChunkTypeInterrupted {
			continue
		}
		// Per-context chunk → look up the listener and dispatch.
		if chunk.SessionID == "" {
			h.logger.Debug("dropping chunk without sessionId")
			continue
		}
		h.mu.Lock()
		listener := h.listeners[chunk.SessionID]
		h.mu.Unlock()
		if listener != nil {
			listener(&chunk)
		}
	}
	if err := scanner.Err(); err != nil {
		h.logger.Debug("ts host readLoop ended", slog.String("error", err.Error()))
	}
}

func (h *tsHost) waitLoop() {
	err := h.cmd.Wait()
	h.active.Store(false)
	close(h.done)
	if err != nil {
		h.logger.Warn("ts host exited", slog.String("error", err.Error()))
	}
}

// tsHostWorker is the per-context Worker view of the shared host.
type tsHostWorker struct {
	ctxID  string
	host   *tsHost
	active activeFlag
}

func (w *tsHostWorker) Active() bool { return w.active.Load() }

func (w *tsHostWorker) Send(cmd WorkerCommand) error {
	if !w.active.Load() {
		return errors.New("worker closed")
	}
	cmd.SessionID = w.ctxID
	return w.host.send(cmd)
}

func (w *tsHostWorker) Interrupt() error {
	return w.host.send(WorkerCommand{Op: "interrupt", SessionID: w.ctxID})
}

func (w *tsHostWorker) Shutdown() {
	if !w.active.Swap(false) {
		return
	}
	_ = w.host.send(WorkerCommand{Op: "delete", SessionID: w.ctxID})
	w.host.unregister(w.ctxID)
}
