package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/codex"
)

// waker pushes mail to sessions without a subscriber when the harness has a way in:
// Codex through its app-server, idle crush and agy through a herdr nudge. Others wait for
// their next hook.
type waker struct {
	b        *broker.Broker
	mu       sync.Mutex
	inflight map[string]bool
	nudged   map[string]string // session → newest message id already nudged about
}

func (w *waker) wake(info broker.SessionInfo) {
	switch info.Harness {
	case "codex":
		w.codex(info)
	case "crush", "agy":
		w.nudge(info)
	}
}

func (w *waker) codex(info broker.SessionInfo) {
	w.mu.Lock()
	if w.inflight[info.ID] {
		w.mu.Unlock()
		return // the running delivery picks up new mail on its next round
	}
	w.inflight[info.ID] = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.inflight, info.ID)
		w.mu.Unlock()
	}()
	for {
		msgs, err := w.b.Take(info.ID)
		if err != nil || len(msgs) == 0 {
			return
		}
		if err := codex.Deliver(info.ID, formatMail(msgs)); err != nil {
			w.b.Requeue(info.ID, msgs)
			if !errors.Is(err, codex.ErrNotLoaded) {
				log.Printf("wake %s: %v", info.ID, err)
			}
			return // retried on the next sweep tick
		}
	}
}

// nudge types a one-line notice into an idle crush pane; the agent then runs mesh
// and its PreToolUse hook delivers the mail. Typing into a pane is only safe when
// the pane is not focused (the user could be mid-prompt) and still runs this session.
// ponytail: herdr agent prompt would be cleaner, but it only targets agents started
// with `herdr agent start`, which does not support crush.
func (w *waker) nudge(info broker.SessionInfo) {
	if info.Pane == "" || info.PID == 0 || syscall.Kill(info.PID, 0) != nil {
		return
	}
	msgs, err := w.b.Inbox(info.ID)
	if err != nil || len(msgs) == 0 {
		return
	}
	newest := msgs[len(msgs)-1].ID
	w.mu.Lock()
	if w.nudged[info.ID] == newest {
		w.mu.Unlock()
		return
	}
	w.nudged[info.ID] = newest
	w.mu.Unlock()

	var pane struct {
		Agent       string `json:"agent"`
		Status      string `json:"agent_status"`
		Focused     bool   `json:"focused"`
		TabID       string `json:"tab_id"`
		WorkspaceID string `json:"workspace_id"`
	}
	if herdrGet("pane", info.Pane, &pane) != nil {
		return
	}
	idle := pane.Status == "idle" || pane.Status == "done" // done: turn finished, unseen
	if pane.Agent != info.Harness || !idle || userLooking(pane.Focused, pane.TabID, pane.WorkspaceID) {
		w.mu.Lock()
		delete(w.nudged, info.ID) // retry on the next sweep
		w.mu.Unlock()
		return
	}
	var from []string
	for _, m := range msgs {
		if n := cmp(m.FromName, m.From); !slices.Contains(from, n) {
			from = append(from, n)
		}
	}
	text := fmt.Sprintf("[agent-mesh] %d new message(s) from %s. Read them with: %s inbox -ack", len(msgs), strings.Join(from, ", "), meshCmd())
	if err := exec.Command("herdr", "pane", "send-text", info.Pane, text).Run(); err != nil {
		log.Printf("nudge %s: %v", info.ID, err)
		return
	}
	exec.Command("herdr", "pane", "send-keys", info.Pane, "Enter").Run()
}

// herdrGet runs `herdr <kind> get <id>` and decodes result.<kind> into v.
func herdrGet(kind, id string, v any) error {
	out, err := exec.Command("herdr", kind, "get", id).Output()
	if err != nil {
		return err
	}
	var r struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return err
	}
	raw, ok := r.Result[kind]
	if !ok {
		return fmt.Errorf("herdr %s get: no %s in result", kind, kind)
	}
	return json.Unmarshal(raw, v)
}

// userLooking: the pane is focused in its tab, the tab in its workspace, and the
// workspace is the focused one. Errors count as looking (do not type).
func userLooking(paneFocused bool, tab, ws string) bool {
	if !paneFocused {
		return false
	}
	var t, w struct {
		Focused bool `json:"focused"`
	}
	if herdrGet("tab", tab, &t) != nil || herdrGet("workspace", ws, &w) != nil {
		return true
	}
	return t.Focused && w.Focused
}
