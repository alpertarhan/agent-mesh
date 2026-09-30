package broker

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryAfterAckAndTake(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, _ := New(DefaultLimits(), spool)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	hello(t, b, "c", "carol")
	m1, _ := b.Send("a", SendReq{To: "b", Text: "first"}, nil)
	m2, _ := b.Send("b", SendReq{To: "a", Text: "second", Attachments: []Attachment{{Type: "ref", Name: "x.md", Path: "/tmp/x.md"}}}, nil)
	b.Send("a", SendReq{To: "c", Text: "not for b"}, nil)
	b.Ack("b", []string{m1.ID})
	b.Take("a")

	b2, _ := New(DefaultLimits(), spool) // survives restart
	h, err := b2.History("b", 0, HistoryFilter{})
	if err != nil || len(h) != 2 || h[0].ID != m1.ID || h[0].Dir != "in" || h[1].ID != m2.ID || h[1].Dir != "out" || h[1].ToName != "alice" || len(h[1].Refs) != 1 {
		t.Fatalf("history of b: %+v %v", h, err)
	}
	got, err := b2.Show("b", m1.ID)
	if err != nil || got.Text != "first" {
		t.Fatalf("show: %v %v", got, err)
	}
	if _, err := b2.Show("b", "0000000000000000"); code(err) != CodeUnknownMsg || !strings.Contains(err.Error(), "agm history") {
		t.Fatalf("missing id: %v", err)
	}
	// c cannot read a↔b mail.
	if _, err := b2.Show("c", m1.ID); code(err) != CodeUnknownMsg {
		t.Fatalf("foreign show: %v", err)
	}
	// Read-only: no queue change.
	if in, _ := b2.Inbox("b"); len(in) != 0 {
		t.Fatalf("history changed mailbox: %v", in)
	}
}

func TestHistoryBounds(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	for range HistoryMax + 10 {
		b.Send("a", SendReq{To: "b", Text: "x"}, nil)
		b.Ack("b", nil)
		b.Take("b")
	}
	if len(b.history) != HistoryMax {
		t.Fatalf("count bound: %d", len(b.history))
	}
	if h, _ := b.History("a", 1000, HistoryFilter{}); len(h) != historyLimitMax {
		t.Fatalf("limit cap: %d", len(h))
	}
	// Total bytes bounded; stored bodies are complete.
	big := strings.Repeat("é", MaxMessage/4)
	for range 20 {
		if _, err := b.Send("a", SendReq{To: "b", Text: big}, nil); err != nil {
			t.Fatal(err)
		}
		b.Take("b")
	}
	if last := b.history[len(b.history)-1]; last.Text != big {
		t.Fatal("stored body cut")
	}
	if b.historyBytes > HistoryBytes {
		t.Fatalf("total bound: %d", b.historyBytes)
	}
}

func TestHistoryResponseFitsFrame(t *testing.T) {
	b := newBroker(t, nil)
	// Names are capped to one clean 64-rune line at hello, so summaries stay small;
	// ref-heavy summaries can still grow (TestHistoryRefTrim), but this batch fits.
	hello(t, b, "a", "n\n"+strings.Repeat("m", 100<<10))
	hello(t, b, "b", "bob")
	for range 50 {
		b.Send("a", SendReq{To: "b", Text: "x"}, nil)
	}
	h, _ := b.History("b", 200, HistoryFilter{})
	data, _ := json.Marshal(Response{ID: 1, Result: h})
	if len(h) != 50 || len(data) >= MaxFrame {
		t.Fatalf("%d summaries, %d bytes", len(h), len(data))
	}
	if n := "n " + strings.Repeat("m", 62); h[0].FromName != n {
		t.Fatalf("name not capped to one clean line: %q", h[0].FromName)
	}
}

func TestSendFrameLimit(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	// Control characters escape to \u00XX (6 bytes): raw size is under the frame, encoded is not.
	if _, err := b.Send("a", SendReq{To: "b", Text: strings.Repeat("\x01", MaxFrame/4)}, nil); code(err) != CodeTooLarge {
		t.Fatalf("escape-heavy text: %v", err)
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: strings.Repeat("y", MaxMessage-1000)}, nil); err != nil {
		t.Fatalf("near-limit text: %v", err)
	}
	if in, _ := b.Inbox("b"); len(in) != 1 {
		t.Fatalf("rejected message queued: %d", len(in))
	}
}

func TestRefValidation(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	for _, a := range []Attachment{
		{Type: "ref", Name: "x", Path: "relative/x"},
		{Type: "ref", Name: "x", Path: "/x", Content: "embedded"},
		{Type: "ref", Name: "x", Path: "/x\nrm -rf"},
	} {
		if _, err := b.Send("a", SendReq{To: "b", Text: "t", Attachments: []Attachment{a}}, nil); code(err) != CodeBadRequest {
			t.Errorf("%+v accepted: %v", a, err)
		}
	}
	refs := make([]Attachment, MaxRefs+1)
	for i := range refs {
		refs[i] = Attachment{Type: "ref", Name: "x", Path: "/x"}
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: "t", Attachments: refs}, nil); code(err) != CodeBadRequest {
		t.Errorf("too many refs: %v", err)
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: "t", Attachments: refs[:2]}, nil); err != nil {
		t.Errorf("repeated refs: %v", err)
	}
}

func TestPreviewUnicode(t *testing.T) {
	if got := Preview("a😀b\n c", 2); got != "a😀…" {
		t.Fatalf("%q", got)
	}
	if got := CutUTF8("aé", 2); got != "a" {
		t.Fatalf("%q", got)
	}
}

func TestBulkFitsFrame(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	big := strings.Repeat("z", 400<<10)
	for range 3 {
		if _, err := b.Send("a", SendReq{To: "b", Text: big}, nil); err != nil {
			t.Fatal(err)
		}
	}
	in, _ := b.Inbox("b")
	if data, _ := json.Marshal(Response{ID: 1, Result: in}); len(in) != 2 || len(data) >= MaxFrame {
		t.Fatalf("inbox: %d msgs, %d bytes", len(in), len(data))
	}
	got := 0
	for {
		msgs, _ := b.Take("b")
		if len(msgs) == 0 {
			break
		}
		if data, _ := json.Marshal(Response{ID: 1, Result: msgs}); len(data) >= MaxFrame {
			t.Fatalf("take: %d bytes", len(data))
		}
		got += len(msgs)
	}
	if got != 3 {
		t.Fatalf("took %d, want 3 (nothing lost)", got)
	}
}

func TestHistoryEncodedBound(t *testing.T) {
	for _, tc := range []struct {
		name, fromName, body string
		count                int
	}{
		{"sender-metadata", strings.Repeat("n", 100<<10), "x", 60},
		{"json-escaping", "alice", strings.Repeat("\x01", 32<<10), 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBroker(t, nil)
			hello(t, b, "a", tc.fromName)
			hello(t, b, "b", "bob")
			for range tc.count {
				if _, err := b.Send("a", SendReq{To: "b", Text: tc.body}, nil); err != nil {
					t.Fatal(err)
				}
				b.Take("b")
			}
			data, err := json.Marshal(b.history)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > HistoryBytes {
				t.Fatalf("archive JSON is %d bytes, limit %d; accounting reports %d", len(data), HistoryBytes, b.historyBytes)
			}
		})
	}
}

func TestAsyncAsk(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, _ := New(DefaultLimits(), spool)
	b.lim.AsksInFlight = 2
	sa, sb := hello(t, b, "a", "alice"), hello(t, b, "b", "bob")
	q, err := b.Send("a", SendReq{To: "b", Text: "async?", ExpectsReply: true, NoWait: true}, sa)
	if err != nil || !b.asks[q.ID].Async || b.asks[q.ID].sink != nil {
		t.Fatalf("%v %+v", err, b.asks[q.ID])
	}
	// No false deadlock: a does not wait, so b may ask a synchronously.
	back, err := b.Send("b", SendReq{To: "a", Text: "sync?", ExpectsReply: true}, sb)
	if err != nil {
		t.Fatalf("false would_deadlock: %v", err)
	}
	// ...but a blocking ask back would close a real cycle.
	if _, err := b.Send("a", SendReq{To: "b", Text: "x", ExpectsReply: true}, sa); code(err) != CodeDeadlock {
		t.Fatalf("sync cycle: %v", err)
	}
	// Async asks count toward AsksInFlight.
	if _, err := b.Send("a", SendReq{To: "b", Text: "2", ExpectsReply: true, NoWait: true}, sa); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: "3", ExpectsReply: true, NoWait: true}, sa); code(err) != CodeTooManyAsks {
		t.Fatalf("limit: %v", err)
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: "x", NoWait: true}, sa); code(err) != CodeBadRequest {
		t.Fatalf("no_wait without ask: %v", err)
	}
	b.Send("b", SendReq{ReplyTo: back.ID, Text: "unrelated"}, nil) // wrong direction for q
	n := sa.len()
	// The reply is queued (never swallowed by the asking connection) and archived.
	r, _ := b.Send("b", SendReq{ReplyTo: q.ID, Text: "yes"}, sb)
	if in, _ := b.Inbox("a"); in[len(in)-1].ID != r.ID || sa.len() != n+1 {
		t.Fatalf("reply not queued: %v", in)
	}
	b2, _ := New(DefaultLimits(), spool) // Async survives the spool
	for id, a := range b2.asks {
		if a.To == "b" && !a.Async && id != back.ID {
			t.Fatalf("async flag lost for %s", id)
		}
	}
}

func TestWait(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	hello(t, b, "c", "carol")
	q, _ := b.Send("a", SendReq{To: "b", Text: "q?", ExpectsReply: true, NoWait: true}, nil)
	q2, _ := b.Send("a", SendReq{To: "b", Text: "q2?", ExpectsReply: true, NoWait: true}, nil)

	// Nothing yet: two concurrent waiters are registered; unrelated mail does not wake them.
	w1, w2, other := &sink{}, &sink{}, &sink{}
	for _, w := range []*sink{w1, w2} {
		if m, err := b.Wait("a", q.ID, w); m != nil || err != nil {
			t.Fatalf("early: %v %v", m, err)
		}
	}
	b.Wait("a", q2.ID, other)
	w2.Close() // a dead watcher must not break delivery
	b.Send("b", SendReq{To: "a", Text: "fyi"}, nil)
	b.Send("c", SendReq{To: "a", Text: "fyi"}, nil)
	r, err := b.Send("b", SendReq{ReplyTo: q.ID, Text: "answer"}, nil)
	if err != nil || w1.len() != 1 || w1.msgs[0].ID != r.ID || other.len() != 0 {
		t.Fatalf("push: %v %d %d", err, w1.len(), other.len())
	}
	if in, _ := b.Inbox("a"); len(in) != 3 {
		t.Fatalf("normal delivery changed: %d", len(in))
	}
	if _, ok := b.watchers[q.ID]; ok {
		t.Fatal("watchers kept after the reply")
	}
	b.Detach("a", other)
	if len(b.watchers) != 0 {
		t.Fatalf("watchers after detach: %v", b.watchers)
	}

	// Already answered (and acked by an adapter): found in history, nothing removed.
	b.Take("a")
	if m, err := b.Wait("a", q.ID, nil); err != nil || m == nil || m.ID != r.ID {
		t.Fatalf("history: %v %v", m, err)
	}
	// Not your question.
	if _, err := b.Wait("c", q.ID, nil); code(err) != CodeUnknownMsg {
		t.Fatalf("foreign: %v", err)
	}
	if _, err := b.Wait("a", "0000", nil); code(err) != CodeUnknownMsg {
		t.Fatalf("unknown: %v", err)
	}
}

func ids(h []Summary) string {
	var s []string
	for _, x := range h {
		s = append(s, x.Preview)
	}
	return strings.Join(s, ",")
}

func TestHistoryFilters(t *testing.T) {
	b := newBroker(t, nil)
	b.lim.SendPerMin, b.lim.SendBurst = 1e6, 1e6
	for _, id := range []string{"a", "b", "c"} {
		hello(t, b, id, id+"name")
	}
	send := func(from, to, replyTo, text string) *Message {
		t.Helper()
		m, err := b.Send(from, SendReq{To: to, ReplyTo: replyTo, Text: text}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	root := send("a", "b", "", "root")    // a→b
	r1 := send("b", "", root.ID, "r1")    // b→a
	fwd := send("b", "c", root.ID, "fwd") // b→c: hidden from a
	cr := send("c", "", fwd.ID, "cr")     // c→b: hidden from a
	late := send("b", "a", cr.ID, "late") // b→a: linked to root only via hidden ones
	x := send("a", "c", "", "to-c")       // a→c
	send("c", "", x.ID, "c-reply")        // c→a
	send("c", "a", "", "fyi")             // c→a, no thread
	for range 250 {                       // push old matches past the newest 200
		send("a", "b", "", "noise")
	}

	// -with: filter before the limit; names resolved like send.
	h, err := b.History("a", 200, HistoryFilter{With: "cname"})
	if err != nil || ids(h) != "to-c,c-reply,fyi" {
		t.Fatalf("with: %q %v", ids(h), err)
	}
	// -thread from any member: hidden intermediates link, their bodies never show.
	for _, anchor := range []string{root.ID, r1.ID, late.ID} {
		h, err = b.History("a", 200, HistoryFilter{Thread: anchor})
		if err != nil || ids(h) != "root,r1,late" {
			t.Fatalf("thread %s: %q %v", anchor, ids(h), err)
		}
	}
	// Combined filters, then the limit.
	if h, _ = b.History("a", 1, HistoryFilter{With: "c", Thread: x.ID}); ids(h) != "c-reply" {
		t.Fatalf("with+thread+n: %q", ids(h))
	}
	if h, _ = b.History("a", 200, HistoryFilter{With: "c", Thread: root.ID}); len(h) != 0 {
		t.Fatalf("with c in root thread: %q", ids(h))
	}
	// Foreign or unknown anchors are refused alike; unknown peers too.
	for _, anchor := range []string{fwd.ID, cr.ID, "0000000000000000"} {
		if _, err := b.History("a", 20, HistoryFilter{Thread: anchor}); code(err) != CodeUnknownMsg {
			t.Fatalf("foreign anchor: %v", err)
		}
	}
	if _, err := b.History("a", 20, HistoryFilter{With: "nobody"}); code(err) != CodeUnknownTarget {
		t.Fatalf("unknown peer: %v", err)
	}
	// A peer that is gone: its exact id from your history still works.
	delete(b.sessions, "c")
	if h, err := b.History("a", 200, HistoryFilter{With: "c"}); err != nil || ids(h) != "to-c,c-reply,fyi" {
		t.Fatalf("gone peer: %q %v", ids(h), err)
	}
	// ...but only when the name is unknown: an ambiguous one stays an error.
	hello(t, b, "n1", "c")
	hello(t, b, "n2", "c")
	if _, err := b.History("a", 200, HistoryFilter{With: "c"}); code(err) != CodeAmbiguous {
		t.Fatalf("ambiguous peer picked silently: %v", err)
	}
}

func TestThreadEvictedParent(t *testing.T) {
	b := newBroker(t, nil)
	b.lim.SendPerMin, b.lim.SendBurst = 1e6, 1e6
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	q, _ := b.Send("a", SendReq{To: "b", Text: "q"}, nil)
	r1, _ := b.Send("b", SendReq{ReplyTo: q.ID, Text: "r1"}, nil)
	r2, _ := b.Send("b", SendReq{ReplyTo: q.ID, Text: "r2"}, nil)
	b.history = b.history[1:] // q evicted
	h, err := b.History("a", 20, HistoryFilter{Thread: r2.ID})
	if err != nil || ids(h) != "r1,r2" || h[0].ReplyTo != q.ID {
		t.Fatalf("siblings of an evicted parent: %q %v", ids(h), err)
	}
	if _, err := b.History("a", 20, HistoryFilter{Thread: q.ID}); code(err) != CodeUnknownMsg {
		t.Fatalf("evicted anchor: %v", err)
	}
	_ = r1
}

func TestAckSelected(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	m1, _ := b.Send("a", SendReq{To: "b", Text: "1"}, nil)
	m2, _ := b.Send("a", SendReq{To: "b", Text: "2"}, nil)
	mine, _ := b.Send("b", SendReq{To: "a", Text: "for a"}, nil)
	got, err := b.Ack("b", []string{m2.ID, mine.ID, "0000000000000000"})
	if err != nil || len(got) != 1 || got[0] != m2.ID {
		t.Fatalf("ack: %v %v", got, err)
	}
	if in, _ := b.Inbox("b"); len(in) != 1 || in[0].ID != m1.ID {
		t.Fatalf("wrong messages removed: %v", in)
	}
	if in, _ := b.Inbox("a"); len(in) != 1 {
		t.Fatal("another mailbox was touched")
	}
	if got, _ := b.Ack("b", []string{m2.ID}); len(got) != 0 {
		t.Fatalf("not idempotent: %v", got)
	}
}

// TestHistoryRefTrim: the frame-size accounting in History is still reachable: ref
// names are peer-controlled (16 refs of 80 '<' escape to ~7.7 KiB of JSON per
// summary), so a long-enough history returns fewer summaries than asked for, and the
// response fits one frame.
func TestHistoryRefTrim(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "bob")
	atts := make([]Attachment, MaxRefs)
	for i := range atts {
		atts[i] = Attachment{Type: "ref", Name: strings.Repeat("<", 80), Path: "/x"}
	}
	for range 150 { // ~8.8 KiB per summary: past MaxMessage well before the newest 150
		if _, err := b.Send("a", SendReq{To: "b", Text: strings.Repeat("<", 160), Attachments: atts}, nil); err != nil {
			t.Fatal(err)
		}
	}
	h, _ := b.History("b", 200, HistoryFilter{})
	data, _ := json.Marshal(Response{ID: 1, Result: h})
	if len(h) >= 150 || len(data) >= MaxFrame {
		t.Fatalf("%d summaries, %d bytes", len(h), len(data))
	}
	if len(h) == 0 {
		t.Fatal("trim dropped everything")
	}
}
