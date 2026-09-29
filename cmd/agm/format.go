package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

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
func refLines(atts []broker.Attachment, indent string) string {
	var sb strings.Builder
	for _, a := range atts {
		if a.Type == "ref" {
			fmt.Fprintf(&sb, "%sfile: %s\n", indent, shellQuote(a.Path))
		}
	}
	if sb.Len() > 0 {
		sb.WriteString(indent + "(referenced files are not attached: open them with your own file-read tool; you see their current content, which may have changed since sending)\n")
	}
	return sb.String()
}

// printMessage prints one full message for `agm show`.
func printMessage(w io.Writer, m *broker.Message) {
	fmt.Fprintf(w, "%s %s  from %s (%s) to %s  %s\n", kindOf(m), m.ID, cmp(m.FromName, m.From), m.From, m.To, m.At.Local().Format(time.DateTime))
	if m.ReplyTo != "" {
		fmt.Fprintf(w, "reply to %s\n", m.ReplyTo)
	}
	fmt.Fprintf(w, "\n%s\n", m.Text)
	for _, a := range m.Attachments {
		if a.Type != "ref" {
			fmt.Fprintf(w, "\n--- %s: %s ---\n%s\n", a.Type, a.Name, a.Content)
		}
	}
	if refs := refLines(m.Attachments, ""); refs != "" {
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
		peer := cmp(s.ToName, s.To)
		arrow := "->"
		if s.Dir == "in" {
			peer, arrow = cmp(s.FromName, s.From), "<-"
		}
		kind := "MSG"
		if s.ExpectsReply {
			kind = "ASK"
		} else if s.ReplyTo != "" {
			kind = "REPLY"
		}
		fmt.Fprintf(w, "%s %s %-5s %s %s: %s", s.At.Local().Format("01-02 15:04"), s.ID, kind, arrow, peer, s.Preview)
		if len(s.Refs) > 0 {
			fmt.Fprintf(w, " [files: %s]", strings.Join(s.Refs, ", "))
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
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", s.Name, cmp(s.Harness, "-"), state, s.Queued, cmp(s.Pane, "-"), short[s.ID], tildePath(s.Cwd))
	}
	tw.Flush()
}

// printInbox prints queued messages in full (inbox is where they are read).
func printInbox(w, errOut io.Writer, msgs []*broker.Message) {
	if len(msgs) == 0 {
		fmt.Fprintln(errOut, "inbox empty")
		return
	}
	for i, m := range msgs {
		if i > 0 {
			fmt.Fprintln(w)
		}
		hdr := m.ID
		if m.ReplyTo != "" {
			hdr += " re " + m.ReplyTo
		}
		fmt.Fprintf(w, "[%s %s] from %s (%s) at %s\n%s\n", kindOf(m), hdr, cmp(m.FromName, m.From), m.From, m.At.Local().Format("15:04"), m.Text)
		for _, a := range m.Attachments {
			if a.Type != "ref" {
				fmt.Fprintf(w, "--- %s: %s ---\n%s\n", a.Type, a.Name, a.Content)
			}
		}
		fmt.Fprint(w, refLines(m.Attachments, ""))
		if m.ExpectsReply {
			fmt.Fprintf(w, "-> the sender asked for a reply; answer: %s reply %s '<answer>' (single quotes; '\"'\"' for an apostrophe)\n", meshCmd(), m.ID)
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
	fmt.Fprintf(w, "reply %s has attachments:\n", m.ID)
	fmt.Fprint(w, refLines(m.Attachments, "  "))
	for _, a := range m.Attachments {
		if a.Type != "ref" {
			fmt.Fprintf(w, "  attachment %s %q (%d bytes): full text with %s show %s\n", a.Type, broker.Preview(a.Name, 80), len(a.Content), meshCmd(), m.ID)
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
	fmt.Fprintf(w, "session  %s\nsource   %s\n", who.ID, who.Source)
	if !who.Registered {
		fmt.Fprintln(w, "state    not registered with the daemon")
		return
	}
	printSessionFields(w, *who.Session)
}

// printSession prints one session in full (`agm resolve`): the full id, never a prefix.
func printSession(w io.Writer, s broker.SessionInfo) {
	fmt.Fprintf(w, "session  %s\n", s.ID)
	printSessionFields(w, s)
}

func printSessionFields(w io.Writer, s broker.SessionInfo) {
	state := "offline"
	if s.Live {
		state = "live"
	}
	fmt.Fprintf(w, "name     %s\nharness  %s\nstate    %s\nqueued   %d\npane     %s\ncwd      %s\n", s.Name, cmp(s.Harness, "-"), state, s.Queued, cmp(s.Pane, "-"), tildePath(s.Cwd))
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
