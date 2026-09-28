package broker

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"
)

// History: a bounded copy of every routed message, recorded at Send, so a body stays
// retrievable (`agm show`) after it was acked, taken or cut short in a hook preview.
// Bounds: HistoryMax messages AND HistoryBytes in total (oldest evicted first). Each
// message is already at most MaxMessage encoded, so stored bodies are complete.
const (
	HistoryMax      = 500
	HistoryBytes    = 4 << 20
	previewRunes    = 160
	historyLimitMax = 200
	// MaxMessage bounds the JSON of a pushed message event, so every push, reply and
	// `show` response fits a MaxFrame line with room for the envelope.
	MaxMessage = MaxFrame - 4<<10
)

// Summary is one history entry as listed by `history` (no full body).
type Summary struct {
	ID           string    `json:"id"`
	Dir          string    `json:"dir"` // in | out, from the caller's perspective
	From         string    `json:"from"`
	FromName     string    `json:"from_name,omitempty"`
	To           string    `json:"to"`
	ToName       string    `json:"to_name,omitempty"`
	ReplyTo      string    `json:"reply_to,omitempty"`
	ExpectsReply bool      `json:"expects_reply,omitempty"`
	At           time.Time `json:"at"`
	Bytes        int       `json:"bytes"`
	Preview      string    `json:"preview"`
	Refs         []string  `json:"refs,omitempty"` // names of ref attachments
}

// msgSize is m's encoded size in the spool (JSON plus a separator), so metadata and
// escaping count toward HistoryBytes.
func msgSize(m *Message) int {
	data, _ := json.Marshal(m)
	return len(data) + 1
}

// CutUTF8 returns s cut to at most n bytes, never inside a UTF-8 sequence.
func CutUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// Preview is s on one line, at most runes code points, with "…" if cut.
func Preview(s string, runes int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= runes {
		return s
	}
	i := 0
	for j := range s {
		if i == runes {
			return s[:j] + "…"
		}
		i++
	}
	return s
}

// archive stores m (already bounded by checkSize). Caller holds b.mu.
func (b *Broker) archive(m *Message) {
	b.history = append(b.history, m)
	b.historyBytes += msgSize(m)
	b.trimHistory()
}

func (b *Broker) trimHistory() {
	n := 0
	for len(b.history)-n > HistoryMax || (b.historyBytes > HistoryBytes && len(b.history)-n > 1) {
		b.historyBytes -= msgSize(b.history[n])
		b.history[n] = nil
		n++
	}
	b.history = b.history[n:]
}

// HistoryFilter narrows History. Both fields are optional and combine.
type HistoryFilter struct {
	With   string // peer: a target (resolved like Send) or an exact session id from your history
	Thread string // any retained message of yours: its reply thread
}

// History lists up to limit (default 20, max 200) of the newest messages that
// session id sent or received and that match f, oldest first. Filters apply before
// the limit. Read-only.
func (b *Broker) History(id string, limit int, f HistoryFilter) ([]Summary, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.sessions[id]; !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	if limit <= 0 {
		limit = 20
	}
	limit = min(limit, historyLimitMax)
	match, err := b.historyMatch(id, f)
	if err != nil {
		return nil, err
	}
	var out []Summary
	size := 64 // response envelope
	for i := len(b.history) - 1; i >= 0 && len(out) < limit; i-- {
		m := b.history[i]
		if (m.From != id && m.To != id) || !match(m) {
			continue
		}
		s := Summary{ID: m.ID, Dir: "in", From: m.From, FromName: m.FromName, To: m.To, ReplyTo: m.ReplyTo,
			ExpectsReply: m.ExpectsReply, At: m.At, Bytes: msgSize(m), Preview: Preview(m.Text, previewRunes)}
		if m.From == id {
			s.Dir = "out"
		}
		if d, ok := b.sessions[m.To]; ok {
			s.ToName = d.info.Name
		}
		for _, a := range m.Attachments {
			if a.Type == "ref" {
				s.Refs = append(s.Refs, Preview(a.Name, 80))
			}
		}
		// Names are peer-controlled: measure, so the response always fits one frame.
		data, _ := json.Marshal(s)
		if size += len(data) + 1; size > MaxMessage {
			break
		}
		out = append(out, s)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// historyMatch builds the filter for History. Caller holds b.mu.
func (b *Broker) historyMatch(id string, f HistoryFilter) (func(*Message) bool, error) {
	peer := ""
	if f.With != "" {
		if s, err := b.resolve(f.With); err == nil {
			peer = s.info.ID
		} else if e, ok := err.(*Error); !ok || e.Code != CodeUnknownTarget {
			return nil, err // e.g. ambiguous: never pick a peer silently
		} else {
			for _, m := range b.history { // a peer that is gone: its exact id, as seen in your history
				if (m.From == id && m.To == f.With) || (m.To == id && m.From == f.With) {
					peer = f.With
					break
				}
			}
			if peer == "" {
				return nil, err
			}
		}
	}
	var thread map[string]bool
	if f.Thread != "" {
		var err error
		if thread, err = b.thread(id, f.Thread); err != nil {
			return nil, err
		}
	}
	return func(m *Message) bool {
		if peer != "" && !(m.From == id && m.To == peer || m.To == id && m.From == peer) {
			return false
		}
		return thread == nil || thread[m.ID]
	}, nil
}

// thread returns the ids of the retained messages connected to anchor by reply_to
// links, through any retained message (also other sessions' ones: the caller only
// ever sees its own) and through evicted parents (their retained replies stay
// linked). anchor must be a retained message that id sent or received.
func (b *Broker) thread(id, anchor string) (map[string]bool, error) {
	parent := map[string]string{} // union-find over message ids, evicted parents included
	var find func(string) string
	find = func(x string) string {
		p, ok := parent[x]
		if !ok || p == x {
			return x
		}
		r := find(p)
		parent[x] = r
		return r
	}
	own := false
	for _, m := range b.history {
		if m.ID == anchor && (m.From == id || m.To == id) {
			own = true
		}
		if m.ReplyTo != "" {
			if a, c := find(m.ID), find(m.ReplyTo); a != c {
				parent[a] = c
			}
		}
	}
	if !own {
		return nil, errf(CodeUnknownMsg, "no retained message %q that you sent or received", anchor)
	}
	root, out := find(anchor), map[string]bool{}
	for _, m := range b.history {
		if find(m.ID) == root {
			out[m.ID] = true
		}
	}
	return out, nil
}

// Show returns the stored message msgID (exact id) if session id sent or received
// it. Read-only.
func (b *Broker) Show(id, msgID string) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	mine := func(m *Message) bool { return m.ID == msgID && (m.From == id || m.To == id) }
	for i := len(b.history) - 1; i >= 0; i-- {
		if mine(b.history[i]) {
			return b.history[i], nil
		}
	}
	for _, m := range s.mailbox { // e.g. mail queued before history existed
		if mine(m) {
			return m, nil
		}
	}
	return nil, errf(CodeUnknownMsg, "message %q is not in your history (only messages you sent or received, full id; the daemon keeps the last %d messages / %d MiB). Recent ones: agm history",
		msgID, HistoryMax, HistoryBytes>>20)
}

// MaxRefs and MaxRefPath bound file references (attachments of type "ref").
const (
	MaxRefs    = 16
	MaxRefPath = 4096
)

// checkRefs validates the shape of file references; the sender checks existence.
func checkRefs(atts []Attachment) error {
	n := 0
	for _, a := range atts {
		if a.Type != "ref" {
			continue
		}
		if n++; n > MaxRefs {
			return errf(CodeBadRequest, "at most %d file references", MaxRefs)
		}
		if !strings.HasPrefix(a.Path, "/") || len(a.Path) > MaxRefPath || a.Content != "" || !utf8.ValidString(a.Path) || strings.ContainsAny(a.Path, "\x00\n\r") {
			return errf(CodeBadRequest, "file reference %q: need an absolute path (max %d bytes, one line) and no content", Preview(a.Path, 80), MaxRefPath)
		}
	}
	return nil
}

// checkSize rejects a message whose push event would not fit one frame (JSON
// escaping and metadata included).
func checkSize(m *Message) error {
	data, err := json.Marshal(Event{Event: "message", Message: m})
	if err != nil {
		return errf(CodeBadRequest, "encode: %v", err)
	}
	if len(data) > MaxMessage {
		return errf(CodeTooLarge, "message is %d bytes encoded, limit %d; share big content with -ref PATH instead", len(data), MaxMessage)
	}
	return nil
}

// batch returns the oldest messages whose encoded list fits one response frame
// (at least one: every accepted message fits by checkSize).
func batch(msgs []*Message) []*Message {
	size := 64
	for i, m := range msgs {
		data, _ := json.Marshal(m)
		if size += len(data) + 1; size > MaxMessage && i > 0 {
			return msgs[:i]
		}
	}
	return msgs
}

// Wait returns the reply to question qid that session id asked, if one was already
// sent (retained history or mailbox; oldest first). Otherwise it registers sink to get
// the reply pushed when it arrives and returns nil. Lookup and registration happen
// under one lock, so no reply is missed in between. Nothing is acked or removed.
func (b *Broker) Wait(id, qid string, sink Sink) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	mine := false
	if seen, ok := b.seen[qid]; ok && seen.From == id {
		mine = true
	}
	for _, m := range b.history {
		if m.ID == qid && m.From == id {
			mine = true
		}
		if m.ReplyTo == qid && m.To == id && mine {
			return m, nil
		}
	}
	if !mine {
		return nil, errf(CodeUnknownMsg, "message %q is not one you sent, or it is too old (the daemon remembers the last %d message ids)", qid, seenCap)
	}
	for _, m := range s.mailbox { // e.g. mail queued before history existed
		if m.ReplyTo == qid && m.To == id {
			return m, nil
		}
	}
	if sink == nil {
		return nil, nil
	}
	if b.watchers[qid] == nil {
		b.watchers[qid] = map[Sink]string{}
	}
	b.watchers[qid][sink] = id
	return nil, nil
}

// notifyWatchers pushes reply m to every `wait` for its question from its receiver.
// A failed push never affects normal delivery. Caller holds b.mu.
func (b *Broker) notifyWatchers(m *Message) {
	w := b.watchers[m.ReplyTo]
	if m.ReplyTo == "" || w == nil {
		return
	}
	for sink, sess := range w {
		if sess == m.To {
			sink.Push(m)
			delete(w, sink)
		}
	}
	if len(w) == 0 {
		delete(b.watchers, m.ReplyTo)
	}
}

// unwatch drops sink's registrations (connection closed). Caller holds b.mu.
func (b *Broker) unwatch(sink Sink) {
	for q, w := range b.watchers {
		delete(w, sink)
		if len(w) == 0 {
			delete(b.watchers, q)
		}
	}
}
