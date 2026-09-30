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
	"github.com/alpertarhan/agent-mesh/internal/integrations"
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
	// retry clears our marker (not a newer attempt's) so the next sweep tries again.
	retry := func() {
		w.mu.Lock()
		if w.nudged[info.ID] == newest {
			delete(w.nudged, info.ID)
		}
		w.mu.Unlock()
	}
	if err := herdrGet("pane", info.Pane, &pane); err != nil {
		retry()
		return
	}
	idle := pane.Status == "idle" || pane.Status == "done" // done: turn finished, unseen
	if pane.Agent != info.Harness || !idle || userLooking(pane.Focused, pane.TabID, pane.WorkspaceID) {
		retry()
		return
	}
	var from []string
	for _, m := range msgs {
		// Names are peer-controlled and this text is typed as user input: one line,
		// whitespace collapsed, capped (Preview), and framed so it reads as peer mail.
		if n := broker.Preview(cmp(m.FromName, m.From), 32); !slices.Contains(from, n) {
			from = append(from, n)
		}
	}
	text := fmt.Sprintf("[agent-mesh] %d new message(s) from %s. %s Read them with: %s inbox -ack", len(msgs), strings.Join(from, ", "), integrations.Frame(), meshCmd())
	if err := herdrRun("pane", "send-text", info.Pane, text); err != nil {
		log.Printf("nudge %s: %v", info.ID, err)
		retry()
		return
	}
	// The text is typed: stay marked even if Enter fails (a retry would type it twice).
	if err := herdrRun("pane", "send-keys", info.Pane, "Enter"); err != nil {
		log.Printf("nudge %s: send-keys: %v", info.ID, err)
	}
}

// herdrOutput and herdrRun run herdr; variables so tests can fake it.
var (
	herdrOutput = func(args ...string) ([]byte, error) { return exec.Command("herdr", args...).Output() }
	herdrRun    = func(args ...string) error { return exec.Command("herdr", args...).Run() }
)

// herdrGet runs `herdr <kind> get <id>` and decodes result.<kind> into v.
func herdrGet(kind, id string, v any) error {
	out, err := herdrOutput(kind, "get", id)
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
