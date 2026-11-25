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
// their next hook. It is also the daemon's wake bookkeeping for an idle exit.
type waker struct {
	b        *broker.Broker
	mu       sync.Mutex
	pending  int               // wakes scheduled, their goroutine not started yet
	inflight map[string]bool   // sessions with a delivery in flight
	nudged   map[string]string // session → newest message id already nudged about
	// settled: session → newest message at the time a wake last dealt with its queue,
	// handed over or refused. A newer message is still owed a wake.
	settled map[string]string
	last    time.Time // when a delivery last handed mail over
}

// wakesHere: link and bridge sessions are never woken on this host, and only some
// harnesses have a way in.
func wakesHere(info broker.SessionInfo) bool {
	return !strings.Contains(info.ID, "/") && wakesLocally(info.Harness)
}

// schedule launches a wake and counts it before its goroutine exists, so an idle
// exit cannot slip in between.
func (w *waker) schedule(info broker.SessionInfo) {
	w.mu.Lock()
	w.pending++
	w.mu.Unlock()
	go w.wake(info)
}

func (w *waker) wake(info broker.SessionInfo) {
	w.mu.Lock()
	if w.pending > 0 { // scheduled; a direct call in a test has no count to undo
		w.pending--
	}
	if w.inflight[info.ID] || !wakesHere(info) {
		w.mu.Unlock()
		return
	}
	if w.inflight == nil { // tests build a waker by hand
		w.inflight = map[string]bool{}
	}
	if w.nudged == nil {
		w.nudged = map[string]string{}
	}
	if w.settled == nil {
		w.settled = map[string]string{}
	}
	w.inflight[info.ID] = true
	w.mu.Unlock()
	did := false // the delivery handed mail over: the idle grace starts again from it
	defer func() {
		w.mu.Lock()
		delete(w.inflight, info.ID)
		if did {
			w.last = time.Now()
		}
		w.mu.Unlock()
	}()
	tail := w.b.Tail(info.ID) // the marker Idle compares with, read before the attempt
	if tail == "" {
		return // taken meanwhile
	}
	if info.Harness == "codex" {
		did = w.codex(info)
	} else {
		before := w.marker(info.ID)
		w.nudge(info)                     // crush, agy
		did = w.marker(info.ID) != before // typed: a failed attempt clears its marker
	}
	w.mu.Lock()
	w.settled[info.ID] = tail // dealt with as far as this host can; retries go on
	w.mu.Unlock()
}

// marker is the newest message a nudge already told the session about.
func (w *waker) marker(id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nudged[id]
}

// Busy reports whether a wake is scheduled or in flight. Called with the broker
// lock held: it takes only this waker's lock, never the broker's.
func (w *waker) Busy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pending > 0 || len(w.inflight) > 0
}

// Done reports when the last delivery handed mail over.
func (w *waker) Done() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}

// Owed reports whether the session's newest queued message still needs a wake: no
// wake has dealt with the queue since that message arrived. Called with the broker
// lock held; it never calls back into the broker.
func (w *waker) Owed(info broker.SessionInfo, newest string) bool {
	if !wakesHere(info) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.settled[info.ID] != newest
}

// prune drops the markers of sessions that are gone, so the maps stay as small as
// the mesh. Not under this waker's lock: it calls the broker.
func (w *waker) prune() {
	live := map[string]bool{}
	for _, info := range w.b.List() {
		live[info.ID] = true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range []map[string]string{w.nudged, w.settled} {
		for id := range m {
			if !live[id] {
				delete(m, id)
			}
		}
	}
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
