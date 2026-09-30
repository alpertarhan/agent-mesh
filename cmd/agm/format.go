package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

// stdoutTerm/stderrTerm: whether plain output goes to a terminal. Peer-controlled
// strings in plain output are then control-stripped (termSafe): escape sequences in
// peer text could otherwise write the clipboard (OSC 52), retitle or clear the
// terminal. Piped output (what agents read through their shell tool) and -json are
// byte-identical and never sanitized. Tests flip the flags directly.
var (
	stdoutTerm = isTTY(os.Stdout)
	stderrTerm = isTTY(os.Stderr)
)

func peer(s string) string {
	if !stdoutTerm {
		return s
	}
	return termSafe(s)
}

func peerErr(s string) string {
	if !stderrTerm {
		return s
	}
	return termSafe(s)
}

// raw is the identity sanitizer for paths that must stay byte-identical (hook and
// Codex output, -json): they are agent data, not terminal input.
func raw(s string) string { return s }

// termSafe replaces terminal-dangerous runes in s with U+FFFD: C0 controls except
// \n and \t, DEL, C1 controls (CSI at U+009B) and the bidi controls. With ESC gone,
// what remains of a sequence is harmless text, and the markers show what was removed.
func termSafe(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case r < 0x20 || r == 0x7f, r >= 0x80 && r <= 0x9f, r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069:
			return utf8.RuneError
		}
		return r
	}, s)
}

func kindOf(m *broker.Message) string {
	switch {
	case m.ExpectsReply:
		return "ASK"
	case m.ReplyTo != "":
		return "REPLY"
	}
	return "MSG"
}

// refLines describes file references for an agent: absolute, shell-quoted paths.
// safe is the caller's sanitizer (peer on a terminal, raw for agent data paths).
func refLines(atts []broker.Attachment, indent string, safe func(string) string) string {
	var sb strings.Builder
	for _, a := range atts {
		if a.Type == "ref" {
			fmt.Fprintf(&sb, "%sfile: %s\n", indent, safe(shellQuote(a.Path)))
		}
	}
	if sb.Len() > 0 {
		sb.WriteString(indent + integrations.RefNote() + "\n")
	}
	return sb.String()
}

// printMessage prints one full message for `agm show`.
func printMessage(w io.Writer, m *broker.Message) {
	fmt.Fprintf(w, "%s %s  from %s (%s) to %s  %s\n", kindOf(m), peer(m.ID), peer(cmp(m.FromName, m.From)), peer(m.From), peer(m.To), m.At.Local().Format(time.DateTime))
	if m.ReplyTo != "" {
		fmt.Fprintf(w, "reply to %s\n", peer(m.ReplyTo))
	}
	fmt.Fprintf(w, "\n%s\n", peer(m.Text))
	for _, a := range m.Attachments {
		if a.Type != "ref" {
			fmt.Fprintf(w, "\n--- %s: %s ---\n%s\n", peer(a.Type), peer(a.Name), peer(a.Content))
		}
	}
	if refs := refLines(m.Attachments, "", peer); refs != "" {
		fmt.Fprintf(w, "\n%s", refs)
	}
}

// printHistory prints history summaries, newest last.
func printHistory(w, errOut io.Writer, h []broker.Summary) {
	if len(h) == 0 {
		fmt.Fprintln(errOut, "no messages in history")
		return
	}
	for _, s := range h {
		name := cmp(s.ToName, s.To)
		arrow := "->"
		if s.Dir == "in" {
			name, arrow = cmp(s.FromName, s.From), "<-"
		}
		kind := "MSG"
		if s.ExpectsReply {
			kind = "ASK"
		} else if s.ReplyTo != "" {
			kind = "REPLY"
		}
		fmt.Fprintf(w, "%s %s %-5s %s %s: %s", s.At.Local().Format("01-02 15:04"), peer(s.ID), kind, arrow, peer(name), peer(s.Preview))
		if len(s.Refs) > 0 {
			refs := make([]string, len(s.Refs))
			for i, r := range s.Refs {
				refs[i] = peer(r)
			}
			fmt.Fprintf(w, " [files: %s]", strings.Join(refs, ", "))
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(errOut, "full text: agm show <id>")
}

// printList prints sessions for humans: live first, then by name.
func printList(w, errOut io.Writer, list []broker.SessionInfo) {
	if len(list) == 0 {
		fmt.Fprintln(errOut, "no sessions yet: start a harness with an installed adapter (agm status)")
		return
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].Live != list[j].Live {
			return list[i].Live
		}
		return strings.ToLower(list[i].Name) < strings.ToLower(list[j].Name)
	})
	ids := make([]string, len(list))
	for i, s := range list {
		ids[i] = s.ID
	}
	short := shortIDs(ids)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tHARNESS\tSTATE\tQUEUED\tPANE\tID\tCWD")
	for _, s := range list {
		state := "offline"
		if s.Live {
			state = "live"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", peer(s.Name), peer(cmp(s.Harness, "-")), state, s.Queued, peer(cmp(s.Pane, "-")), peer(short[s.ID]), peer(tildePath(s.Cwd)))
	}
	tw.Flush()
}

// printInbox prints queued messages in full (inbox is where they are read).
func printInbox(w, errOut io.Writer, msgs []*broker.Message) {
	if len(msgs) == 0 {
		fmt.Fprintln(errOut, "inbox empty")
		return
	}
	fmt.Fprintf(w, "[agent-mesh] %d message(s) from other agents. %s\n", len(msgs), integrations.Frame())
	for i, m := range msgs {
		if i > 0 {
			fmt.Fprintln(w)
		}
		hdr := m.ID
		if m.ReplyTo != "" {
			hdr += " re " + m.ReplyTo
		}
		fmt.Fprintf(w, "[%s %s] from %s (%s) at %s\n%s\n", kindOf(m), peer(hdr), peer(cmp(m.FromName, m.From)), peer(m.From), m.At.Local().Format("15:04"), peer(m.Text))
		for _, a := range m.Attachments {
			if a.Type != "ref" {
				fmt.Fprintf(w, "--- %s: %s ---\n%s\n", peer(a.Type), peer(a.Name), peer(a.Content))
			}
		}
		fmt.Fprint(w, refLines(m.Attachments, "", peer))
		if m.ExpectsReply {
			fmt.Fprintf(w, "-> %s\n", integrations.ReplyHint(meshCmd(), m.ID))
		}
	}
}

// shortIDs maps each id longer than 12 characters to its shortest prefix of at least 8 characters that no other
// id starts with (a unique prefix is a valid target); the full id if there is none.
func shortIDs(ids []string) map[string]string {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		r := []rune(id)
		out[id] = id
		if len(r) <= 12 { // short enough to show in full
			continue
		}
		for n := 8; n < len(r); n++ {
			p := string(r[:n])
			unique := true
			for _, o := range ids {
				if o != id && strings.HasPrefix(o, p) {
					unique = false
					break
				}
			}
			if unique {
				out[id] = p
				break
			}
		}
	}
	return out
}

func tildePath(p string) string {
	if home, err := os.UserHomeDir(); err == nil && home != "" && (p == home || strings.HasPrefix(p, home+"/")) {
		return "~" + p[len(home):]
	}
	return cmp(p, "-")
}

// isTTY reports whether f is a terminal (human progress goes there only).
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// replyExtras writes what a blocking ask cannot put on stdout: file references and
// attachments of the reply (always, not only on a terminal: it is message content).
func replyExtras(w io.Writer, m *broker.Message) {
	if len(m.Attachments) == 0 {
		return
	}
	fmt.Fprintf(w, "reply %s has attachments:\n", peerErr(m.ID))
	fmt.Fprint(w, refLines(m.Attachments, "  ", peerErr))
	for _, a := range m.Attachments {
		if a.Type != "ref" {
			fmt.Fprintf(w, "  attachment %s %q (%d bytes): full text with %s show %s\n", peerErr(a.Type), peerErr(broker.Preview(a.Name, 80)), len(a.Content), meshCmd(), peerErr(m.ID))
		}
	}
}

// whoami is the output of `agm whoami`.
type whoami struct {
	ID         string              `json:"id"`
	Source     string              `json:"source"`
	Registered bool                `json:"registered"`
	Session    *broker.SessionInfo `json:"session,omitempty"`
}

func printWhoami(w io.Writer, who whoami) {
	fmt.Fprintf(w, "session  %s\nsource   %s\n", peer(who.ID), who.Source)
	if !who.Registered {
		fmt.Fprintln(w, "state    not registered with the daemon")
		return
	}
	printSessionFields(w, *who.Session)
}

// printSession prints one session in full (`agm resolve`): the full id, never a prefix.
func printSession(w io.Writer, s broker.SessionInfo) {
	fmt.Fprintf(w, "session  %s\n", peer(s.ID))
	printSessionFields(w, s)
}

func printSessionFields(w io.Writer, s broker.SessionInfo) {
	state := "offline"
	if s.Live {
		state = "live"
	}
	fmt.Fprintf(w, "name     %s\nharness  %s\nstate    %s\nqueued   %d\npane     %s\ncwd      %s\n", peer(s.Name), peer(cmp(s.Harness, "-")), state, s.Queued, peer(cmp(s.Pane, "-")), peer(tildePath(s.Cwd)))
}

// ackResult is the output of `agm ack -json`.
type ackResult struct {
	Acked     []string `json:"acked"`
	NotQueued []string `json:"not_queued"`
}

// isMsgID reports whether s is a full message id (16 lowercase hex digits):
// ack is destructive, so it takes no prefixes.
func isMsgID(s string) bool {
	if len(s) != 16 {
		return false
	}
	for _, r := range s {
		if !('0' <= r && r <= '9' || 'a' <= r && r <= 'f') {
			return false
		}
	}
	return true
}
