package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

const hookBodyMax = 2000

// exitCode makes main exit with code without printing an error.
type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit %d", int(e)) }

// hookEvent is the stdin payload of crush, Claude Code, Codex and Antigravity hooks.
type hookEvent struct {
	// Antigravity: camelCase fields; the event name is passed as an argument.
	ConversationID string   `json:"conversationId"`
	WorkspacePaths []string `json:"workspacePaths"`

	SessionID      string `json:"session_id"`
	Cwd            string `json:"cwd"`
	Event          string `json:"hook_event_name"`
	StopHookActive bool   `json:"stop_hook_active"`
	Prompt         string `json:"prompt"` // UserPromptSubmit
	ToolName       string `json:"tool_name"`
	ToolInput      struct {
		Command string `json:"command"`
	} `json:"tool_input"`
}

// hook is a harness hook entry point. It registers the calling session and delivers
// its mailbox in the harness's hook output format. Delivery errors are swallowed so
// a mesh problem never breaks the harness.
func hook(harness string, args []string, in io.Reader, out, errOut io.Writer) error {
	switch harness {
	case "crush", "claude", "codex", "agy":
	default:
		return fmt.Errorf("unsupported harness %q", harness)
	}
	var ev hookEvent
	if json.NewDecoder(in).Decode(&ev) != nil {
		return nil
	}
	if harness == "agy" {
		if len(args) != 1 {
			return nil
		}
		ev.SessionID, ev.Event, args = ev.ConversationID, args[0], nil
		if len(ev.WorkspacePaths) > 0 {
			ev.Cwd = ev.WorkspacePaths[0]
		}
	}
	if ev.SessionID == "" {
		return nil
	}
	// A dead harness closes our stdout: get EPIPE (and requeue) instead of dying by SIGPIPE.
	signal.Ignore(syscall.SIGPIPE)
	info := broker.SessionInfo{Name: cmp(spawnName(ev.Prompt), os.Getenv("AGM_NAME")), Harness: harness, Cwd: ev.Cwd, PID: harnessPID()}
	if harness != "codex" { // Codex hooks run under its shared daemon: env is not the TUI's
		info.Pane = herdrPane()
	}

	if len(args) == 1 && args[0] == "--wait" {
		return wait(ev.SessionID, info, errOut)
	}
	if ev.Event == "SessionEnd" {
		if c, err := session(ev.SessionID, broker.SessionInfo{}); err == nil {
			c.call(broker.Request{Op: "bye"}, nil)
		}
		return nil
	}
	c, err := session(ev.SessionID, info)
	if err != nil {
		return nil
	}
	mail, msgs := take(c)
	intro := "[agent-mesh] " + integrations.Instruction(meshCmd(), ev.SessionID) + "\n"
	if harness == "agy" {
		return requeueOnErr(c, msgs, agyOutput(out, ev.Event, intro, mail))
	}
	ctx := mail
	if ev.Event == "SessionStart" {
		ctx = intro + mail
	}
	// crush has no command-level allow list: pre-approve plain mesh messaging commands.
	allow := harness == "crush" && ev.ToolName == "bash" && isMeshMessaging(ev.ToolInput.Command)
	if ctx == "" && !allow {
		return nil
	}
	var resp any
	switch {
	case harness == "crush":
		r := map[string]string{"decision": "none"}
		if allow {
			r["decision"] = "allow"
		}
		if ctx != "" {
			r["context"] = ctx
		}
		resp = r
	case ev.Event == "Stop":
		// Not delivered at stop → the agent would idle with unread mail. Block = keep going.
		resp = map[string]string{"decision": "block", "reason": ctx}
	default:
		resp = map[string]any{"hookSpecificOutput": map[string]string{"hookEventName": ev.Event, "additionalContext": ctx}}
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return requeueOnErr(c, msgs, enc.Encode(resp))
}

// requeueOnErr puts taken mail back if it could not be written to the harness.
// ponytail: mail written to a harness that dies before reading it is still lost; the
// hooks have no ack channel.
func requeueOnErr(c *client, msgs []*broker.Message, err error) error {
	if err != nil && len(msgs) > 0 {
		c.call(broker.Request{Op: "requeue", Messages: msgs}, nil)
	}
	return nil // never fail the harness
}

var spawnPrompt = regexp.MustCompile(`^\[agent-mesh\] You are "([^"]+)", an agent spawned by `)

// spawnName is the reserved name in a `agm spawn` first prompt. Harnesses whose hooks
// cannot see the spawn tab's AGM_NAME (Codex) get their name from it.
func spawnName(prompt string) string {
	if m := spawnPrompt.FindStringSubmatch(prompt); m != nil {
		return m[1]
	}
	return ""
}

// agyOutput: PreInvocation injects the intro (ephemeral, every model call: agy has
// no session-start event) and mail (a persistent message, so later steps still see
// it). Stop with mail keeps the agent going.
func agyOutput(out io.Writer, event, intro, mail string) error {
	var resp any
	switch event {
	case "PreInvocation":
		steps := []map[string]string{{"ephemeralMessage": intro}}
		if mail != "" {
			steps = append(steps, map[string]string{"userMessage": mail})
		}
		resp = map[string]any{"injectSteps": steps}
	case "Stop":
		if mail == "" {
			return nil
		}
		resp = map[string]string{"decision": "continue", "reason": mail}
	default:
		return nil
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc.Encode(resp)
}

const waitMax = 55 * time.Minute // below the hook timeout we install (3600s)

// wait is a Claude Code asyncRewake hook: it blocks until mail arrives, prints it to
// stderr, and exits 2, which wakes an idle Claude. A newer waiter for the same session
// replaces this one (the daemon closes our connection); we then exit 0 quietly.
func wait(id string, info broker.SessionInfo, errOut io.Writer) error {
	c, err := dial()
	if err != nil {
		return nil
	}
	defer c.nc.Close()
	info.ID = id
	if c.call(broker.Request{Op: "hello", Session: &info, Wait: true}, nil) != nil {
		return nil
	}
	// This connection only listens; takes go over a fresh connection. Closed channel =
	// replaced by a newer waiter, or daemon gone.
	mail := make(chan struct{}, 1)
	if len(c.events) > 0 { // mailbox replayed before the hello response
		mail <- struct{}{}
	}
	go func() {
		defer close(mail)
		for {
			var ev broker.Event
			if c.next(&ev) != nil {
				return
			}
			if ev.Message != nil {
				select {
				case mail <- struct{}{}:
				default:
				}
			}
		}
	}()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	timeout := time.After(waitMax)
	for {
		select {
		case _, ok := <-mail:
			if !ok {
				return nil
			}
			t, err := session(id, broker.SessionInfo{})
			if err != nil {
				return nil
			}
			ctx, msgs := take(t)
			if ctx != "" { // empty: another hook took it first
				_, err := fmt.Fprint(errOut, ctx)
				requeueOnErr(t, msgs, err)
				t.nc.Close()
				return exitCode(2)
			}
			t.nc.Close()
		case <-tick.C:
			if info.PID != 0 && syscall.Kill(info.PID, 0) == syscall.ESRCH {
				return nil // harness is gone
			}
		case <-timeout:
			return nil
		}
	}
}

// herdrPane is the herdr pane this process runs in, if any.
func herdrPane() string {
	if os.Getenv("HERDR_ENV") != "1" {
		return ""
	}
	return os.Getenv("HERDR_PANE_ID")
}

// binPath is the path configs and agents should use to run this binary, and whether
// it is the `agm` on PATH. The PATH entry (brew/AUR symlink, curl or npm install
// location) survives upgrades; the resolved versioned path (e.g. brew's Cellar) does not.
func binPath() (string, bool) {
	exe, err := os.Executable()
	if err != nil {
		return "agm", true
	}
	if p, err := exec.LookPath("agm"); err == nil {
		if abs, err := filepath.Abs(p); err == nil && sameFile(abs, exe) {
			return abs, true
		}
	}
	return exe, false
}

func sameFile(a, b string) bool {
	x, err1 := os.Stat(a)
	y, err2 := os.Stat(b)
	return err1 == nil && err2 == nil && os.SameFile(x, y)
}

// meshCmd is how agents should invoke us: "agm" if that is this binary, else its path.
func meshCmd() string {
	if p, onPath := binPath(); !onPath {
		return p
	}
	return "agm"
}

// Inspect first, then atomically claim only the messages that fit this delivery.
func take(c *client) (string, []*broker.Message) {
	var msgs []*broker.Message
	if c.call(broker.Request{Op: "inbox"}, &msgs) != nil || len(msgs) == 0 {
		return "", nil
	}
	if c.needProtocol("bounded hook delivery") != nil || c.takeIDs < broker.TakeIDsSupport {
		return "", nil // old daemons ignore take.ids: leave mail queued, never consume it speculatively
	}
	_, count := formatMail(msgs)
	more := count < len(msgs)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = msgs[i].ID
	}
	if count == 0 || c.call(broker.Request{Op: "take", IDs: ids}, &msgs) != nil || len(msgs) == 0 {
		return "", nil
	}
	c.events = c.events[:0]
	text, _ := formatMail(msgs)
	if more {
		text += queuedMailHint
	}
	return text, msgs
}

const mailMessageMax = 8000 // UTF-8 bytes, including metadata and retrieval/reply hints
const mailBatchMax = 24000
const queuedMailHint = "[agent-mesh] More mail remains queued for a later delivery.\n"

// formatMail returns a bounded preview and the consumed prefix length. Callers must
// leave the suffix queued; truncating output must never silently consume its mail.
func formatMail(msgs []*broker.Message) (string, int) {
	if len(msgs) == 0 {
		return "", 0
	}
	header := func(n int) string {
		return fmt.Sprintf("[agent-mesh] %d message(s) from other agents. %s\n", n, integrations.Frame())
	}
	overhead := len(header(len(msgs))) + len(queuedMailHint)
	var body strings.Builder
	count := 0
	for _, m := range msgs {
		text := formatMailMessage(m, mailMessageMax-overhead)
		if overhead+body.Len()+len(text) > mailBatchMax {
			break
		}
		body.WriteString(text)
		count++
	}
	text := header(count) + body.String()
	if count < len(msgs) {
		text += queuedMailHint
	}
	return text, count
}

func formatMailMessage(m *broker.Message, budget int) string {
	name := broker.Preview(cmp(m.FromName, m.From), 128)
	from := broker.Preview(m.From, 256)
	replyTo := broker.Preview(m.ReplyTo, 256)
	metadataCut := name != cmp(m.FromName, m.From) || from != m.From || replyTo != m.ReplyTo
	hdr := m.ID
	if m.ReplyTo != "" {
		hdr += " re " + replyTo
	}
	head := fmt.Sprintf("[%s %s] from %s (%s): ", kindOf(m), hdr, name, from)
	full := fmt.Sprintf("  -> %s\n", integrations.FullTextHint(meshCmd(), m.ID))
	reply := ""
	if m.ExpectsReply {
		reply = fmt.Sprintf("  -> %s\n", integrations.ReplyHint(meshCmd(), m.ID))
	}
	refNote := ""
	for _, a := range m.Attachments {
		if a.Type == "ref" {
			refNote = "  " + integrations.RefNote() + "\n"
			break
		}
	}
	left := budget - len(head) - len(full) - len(reply) - len(refNote) - 1
	if left < 32 {
		return "Message preview omitted: metadata exceeds the budget. Inspect inbox/history for the full message before acting.\n"
	}
	body := m.Text
	bodyCut := len(body) > min(hookBodyMax, left)
	cut := bodyCut || metadataCut
	if bodyCut {
		body = broker.CutUTF8(body, min(hookBodyMax, left)-len("... (truncated)")) + "... (truncated)"
	}
	var out strings.Builder
	out.WriteString(head + body + "\n")
	left -= len(body)
	refShown := false
	for _, a := range m.Attachments {
		var text string
		if a.Type == "ref" {
			text = "  file: " + shellQuote(a.Path) + "\n"
		} else {
			cut = true // inline attachments remain previews; show retrieves the original
			text = fmt.Sprintf("  attachment %s %q (%d bytes): %s\n", broker.Preview(a.Type, 32), broker.Preview(a.Name, 80), len(a.Content), broker.Preview(a.Content, 300))
		}
		if len(text) > left {
			cut = true
			continue // never render half a reference path or attachment heading
		}
		out.WriteString(text)
		left -= len(text)
		refShown = refShown || a.Type == "ref"
	}
	if refShown {
		out.WriteString(refNote)
	}
	if cut {
		out.WriteString(full)
	}
	out.WriteString(reply)
	return out.String()
}

// isMeshMessaging reports whether cmd is one messaging, discovery, or task-metadata command
// invocation: no chaining, pipes, redirects, substitution, or other programs. Quoted
// text may contain anything except substitution inside double quotes.
func isMeshMessaging(cmd string) bool {
	var words []string
	var cur strings.Builder
	inWord := false
	quote := byte(0)
	for i := 0; i < len(cmd); i++ {
		ch := cmd[i]
		switch {
		case quote == '\'':
			if ch == '\'' {
				quote = 0
			} else {
				cur.WriteByte(ch)
			}
		case quote == '"':
			switch ch {
			case '"':
				quote = 0
			case '`', '$', '\\':
				return false // substitution / escapes inside double quotes
			default:
				cur.WriteByte(ch)
			}
		case ch == '\'' || ch == '"':
			quote, inWord = ch, true
		case ch == ' ' || ch == '\t':
			if inWord {
				words = append(words, cur.String())
				cur.Reset()
				inWord = false
			}
		case strings.IndexByte(";&|<>`$()\\\n\r{}*?[]~#!", ch) >= 0:
			return false
		default:
			cur.WriteByte(ch)
			inWord = true
		}
	}
	if quote != 0 {
		return false
	}
	if inWord {
		words = append(words, cur.String())
	}
	if bin, _ := binPath(); len(words) < 2 || (words[0] != "agm" && words[0] != bin) {
		return false
	}
	args := words[1:]
	if len(args) >= 2 && args[0] == "-as" {
		args = args[2:]
	}
	// Not the *-file verbs: they read local files and need normal approval.
	return len(args) > 0 && slices.Contains([]string{"list", "inspect", "watch", "task", "send", "ask", "reply", "inbox", "history", "show", "whoami", "resolve", "wait", "ack"}, args[0])
}
