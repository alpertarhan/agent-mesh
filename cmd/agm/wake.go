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
	"time"

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
	// tried: session → newest message a wake already tried to hand over and could not.
	// The periodic retries go on; this only says the daemon may sleep.
	tried    map[string]string
	lastWake time.Time // when a delivery last handed mail over
}

func (w *waker) wake(info broker.SessionInfo) {
	if strings.Contains(info.ID, "/") {
		return // link and bridge sessions are never woken on this host (3c)
	}
	if !wakesLocally(info.Harness) {
		return
	}
	w.mu.Lock()
	if w.inflight[info.ID] {
		w.mu.Unlock()
		return // the next sweep picks up mail left by the running delivery
	}
	if w.inflight == nil { // tests build a waker by hand
		w.inflight = map[string]bool{}
	}
	if w.nudged == nil {
		w.nudged = map[string]string{}
	}
	if w.tried == nil {
		w.tried = map[string]string{}
	}
	w.inflight[info.ID] = true
	w.mu.Unlock()
	did := false // the delivery handed mail over: the idle grace starts again from it
	defer func() {
		w.mu.Lock()
		delete(w.inflight, info.ID)
		if did {
			w.lastWake = time.Now()
		}
		w.mu.Unlock()
	}()
	if info.Harness == "codex" {
		did = w.codex(info)
	} else {
		before := w.marker(info.ID)
		w.nudge(info)                     // crush, agy
		did = w.marker(info.ID) != before // typed: a failed attempt clears its marker
	}
	if !did {
		if newest := w.newestQueued(info.ID); newest != "" {
			w.mu.Lock()
			w.tried[info.ID] = newest // tried and refused: the daemon may sleep, retries go on
			w.mu.Unlock()
		}
	}
}

// marker is the newest message a nudge already told the session about.
func (w *waker) marker(id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nudged[id]
}

// owed reports whether a wake for this session's newest queued message is still to
// do: it was not nudged yet, and no Codex wake found the thread unloaded for it.
// The periodic retries run either way; owed only decides whether a daemon may sleep.
// Called with the broker lock held: it never calls back into the broker.
func (w *waker) owed(info broker.SessionInfo, newest string) bool {
	if !wakesLocally(info.Harness) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tried[info.ID] != newest
}

// newestQueued is the last message of the session's oldest frame, or "" without mail.
func (w *waker) newestQueued(id string) string {
	msgs, err := w.b.Inbox(id)
	if err != nil || len(msgs) == 0 {
		return ""
	}
	return msgs[len(msgs)-1].ID
}

// busy reports whether a delivery is in flight; lastWake, when the last one finished.
func (w *waker) busy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight) > 0
}

func (w *waker) lastWakeAt() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastWake
}

func (w *waker) codex(info broker.SessionInfo) bool {
	msgs, err := w.b.Inbox(info.ID)
	if err != nil || len(msgs) == 0 {
		return false
	}
	_, count := formatMail(msgs)
	more := count < len(msgs)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = msgs[i].ID
	}
	if count == 0 {
		return false
	}
	held, err := w.b.Hold(info.ID, ids...)
	if err != nil {
		return false
	}
	msgs = held.Messages()
	if len(msgs) == 0 { // a hook took them in between
		held.Release()
		return false
	}
	text, _ := formatMail(msgs)
	if more {
		text += queuedMailHint
	}
	if err := codexDeliver(info.ID, text); err != nil {
		held.Release() // still queued and on disk: the next sweep tries again
		if !errors.Is(err, codex.ErrNotLoaded) {
			log.Printf("wake %s: %v", info.ID, err)
		}
		return false
	}
	// The host has the mail, so it leaves the queue only now. If that save fails, or
	// the daemon dies before it, the mail is delivered once more.
	if err := held.Commit(); err != nil {
		log.Printf("wake %s: delivered, but still queued (%v); it will be delivered again", info.ID, err)
	}
	// Excess mail waits for the next sweep, rather than an immediate context-filling loop.
	return true
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
		// Names are peer-controlled and this text is typed as user input: one clean
		// line via the same helper hello uses (also covers names spooled before that
		// normalization existed), and framed so it reads as peer mail.
		if n := broker.CleanLine(cmp(m.FromName, m.From), 32); !slices.Contains(from, n) {
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

// codexDeliver starts the turn on the Codex host; a variable so tests can fake it.
var codexDeliver = codex.Deliver

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
