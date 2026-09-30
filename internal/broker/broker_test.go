package broker

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type sink struct {
	mu   sync.Mutex
	msgs []*Message
	dead bool
}

func (s *sink) Push(m *Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return false
	}
	s.msgs = append(s.msgs, m)
	return true
}

func (s *sink) Close() { s.mu.Lock(); s.dead = true; s.mu.Unlock() }

func (s *sink) len() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.msgs) }

func newBroker(t *testing.T, mut func(*Limits)) *Broker {
	t.Helper()
	lim := DefaultLimits()
	lim.SendBurst = 1000
	if mut != nil {
		mut(&lim)
	}
	b, err := New(lim, "")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func hello(t *testing.T, b *Broker, id, name string) *sink {
	t.Helper()
	s := &sink{}
	if err := b.Hello(SessionInfo{ID: id, Name: name}, s, true, false); err != nil {
		t.Fatal(err)
	}
	return s
}

func code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func TestSendDeliverAck(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a1", "alice")
	bob := hello(t, b, "b1", "bob")

	m, err := b.Send("a1", SendReq{To: "bob", Text: "hi"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bob.len() != 1 {
		t.Fatalf("pushed %d, want 1", bob.len())
	}
	if in, _ := b.Inbox("b1"); len(in) != 1 {
		t.Fatalf("inbox %d before ack, want 1", len(in))
	}
	if _, err := b.Ack("b1", []string{m.ID}); err != nil {
		t.Fatal(err)
	}
	if in, _ := b.Inbox("b1"); len(in) != 0 {
		t.Fatalf("inbox %d after ack, want 0", len(in))
	}
	// Late subscriber gets queued mail replayed.
	b.Send("a1", SendReq{To: "b1", Text: "again"}, nil)
	late := hello(t, b, "b1", "")
	if late.len() != 1 {
		t.Fatalf("replayed %d, want 1", late.len())
	}
}

func TestResolve(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a1", "")
	hello(t, b, "abc", "x")
	hello(t, b, "abd", "x")
	if _, err := b.Send("a1", SendReq{To: "x", Text: "t"}, nil); code(err) != CodeAmbiguous {
		t.Fatalf("got %v, want ambiguous", err)
	}
	if _, err := b.Send("a1", SendReq{To: "abc", Text: "t"}, nil); err != nil {
		t.Fatalf("exact id: %v", err)
	}
	if _, err := b.Send("a1", SendReq{To: "zz", Text: "t"}, nil); code(err) != CodeUnknownTarget {
		t.Fatalf("got %v, want unknown", err)
	}
}

func TestMailboxFull(t *testing.T) {
	b := newBroker(t, func(l *Limits) { l.MailboxCap = 2 })
	hello(t, b, "a", "")
	hello(t, b, "b", "")
	for range 2 {
		if _, err := b.Send("a", SendReq{To: "b", Text: "x"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Send("a", SendReq{To: "b", Text: "x"}, nil); code(err) != CodeMailboxFull {
		t.Fatalf("got %v, want mailbox_full", err)
	}
}

func TestRateLimit(t *testing.T) {
	b := newBroker(t, func(l *Limits) { l.SendBurst = 2 })
	now := time.Now()
	b.now = func() time.Time { return now }
	hello(t, b, "a", "")
	hello(t, b, "b", "")
	b.Send("a", SendReq{To: "b", Text: "1"}, nil)
	b.Send("a", SendReq{To: "b", Text: "2"}, nil)
	if _, err := b.Send("a", SendReq{To: "b", Text: "3"}, nil); code(err) != CodeRateLimited {
		t.Fatalf("got %v, want rate_limited", err)
	}
	now = now.Add(3 * time.Second) // 20/min → 1 token
	if _, err := b.Send("a", SendReq{To: "b", Text: "4"}, nil); err != nil {
		t.Fatalf("after refill: %v", err)
	}
}

func TestAskReplyGoesToAsker(t *testing.T) {
	b := newBroker(t, nil)
	aSub := hello(t, b, "a", "")
	bSub := hello(t, b, "b", "")
	asker := &sink{} // e.g. a transient `agm ask` connection
	ask, err := b.Send("a", SendReq{To: "b", Text: "q?", ExpectsReply: true}, asker)
	if err != nil {
		t.Fatal(err)
	}
	if bSub.len() != 1 {
		t.Fatal("ask not delivered")
	}
	if _, err := b.Send("b", SendReq{ReplyTo: ask.ID, Text: "answer"}, nil); err != nil {
		t.Fatal(err)
	}
	if asker.len() != 1 || aSub.len() != 0 {
		t.Fatalf("asker=%d subscriber=%d, want 1/0", asker.len(), aSub.len())
	}
	if len(b.asks) != 0 {
		t.Fatal("ask not resolved")
	}

	// Asker gone → reply falls back to the mailbox.
	ask2, _ := b.Send("a", SendReq{To: "b", Text: "q2?", ExpectsReply: true}, asker)
	b.Detach("a", asker)
	b.Send("b", SendReq{ReplyTo: ask2.ID, Text: "late"}, nil)
	if in, _ := b.Inbox("a"); len(in) != 1 || in[0].Text != "late" {
		t.Fatalf("mailbox fallback failed: %v", in)
	}
}

func TestDeadlock(t *testing.T) {
	b := newBroker(t, nil)
	for _, id := range []string{"a", "b", "c"} {
		hello(t, b, id, "")
	}
	mustSend := func(from, to string) {
		t.Helper()
		if _, err := b.Send(from, SendReq{To: to, Text: "?", ExpectsReply: true}, nil); err != nil {
			t.Fatal(err)
		}
	}
	mustSend("a", "b")
	mustSend("b", "c")
	if _, err := b.Send("c", SendReq{To: "a", Text: "?", ExpectsReply: true}, nil); code(err) != CodeDeadlock {
		t.Fatalf("got %v, want would_deadlock", err)
	}
	// Plain send along the cycle is fine.
	if _, err := b.Send("c", SendReq{To: "a", Text: "fyi"}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAskExpires(t *testing.T) {
	b := newBroker(t, func(l *Limits) { l.AskTimeout = 20 * time.Millisecond })
	hello(t, b, "a", "")
	hello(t, b, "b", "")
	b.Send("a", SendReq{To: "b", Text: "?", ExpectsReply: true}, nil)
	time.Sleep(60 * time.Millisecond)
	if _, err := b.Send("b", SendReq{To: "a", Text: "?", ExpectsReply: true}, nil); err != nil {
		t.Fatalf("expired ask still blocks: %v", err)
	}
}

func TestHopLimit(t *testing.T) {
	b := newBroker(t, func(l *Limits) { l.MaxHops = 2 })
	hello(t, b, "a", "")
	hello(t, b, "b", "")
	m, _ := b.Send("a", SendReq{To: "b", Text: "0"}, nil)
	from := []string{"b", "a"}
	for i := range 2 {
		var err error
		if m, err = b.Send(from[i%2], SendReq{ReplyTo: m.ID, Text: "r"}, nil); err != nil {
			t.Fatalf("hop %d: %v", i+1, err)
		}
	}
	if _, err := b.Send("b", SendReq{ReplyTo: m.ID, Text: "r"}, nil); code(err) != CodeHopLimit {
		t.Fatalf("got %v, want hop_limit", err)
	}
}

func TestSpoolRoundtrip(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, _ := New(DefaultLimits(), spool)
	hello(t, b, "a", "alice")
	hello(t, b, "b", "")
	ask, _ := b.Send("a", SendReq{To: "b", Text: "persist me", ExpectsReply: true}, nil)

	b2, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	in, _ := b2.Inbox("b")
	if len(in) != 1 || in[0].Text != "persist me" {
		t.Fatalf("mailbox not restored: %v", in)
	}
	if _, ok := b2.asks[ask.ID]; !ok {
		t.Fatal("pending ask not restored")
	}
	if _, err := b2.Send("b", SendReq{ReplyTo: ask.ID, Text: "ok"}, nil); err != nil {
		t.Fatalf("reply after restart: %v", err)
	}
	if in, _ := b2.Inbox("a"); len(in) != 1 {
		t.Fatalf("reply not queued for a: %v", in)
	}
}

func TestSweep(t *testing.T) {
	b := newBroker(t, nil)
	now := time.Now()
	b.now = func() time.Time { return now }
	alive := map[int]bool{100: true}
	b.alive = func(pid int) bool { return alive[pid] }

	hello(t, b, "sub", "")                                           // subscribed: kept
	b.Hello(SessionInfo{ID: "pidlive", PID: 100}, nil, false, false) // idle but process alive: kept
	b.Hello(SessionInfo{ID: "piddead", PID: 200}, nil, false, false) // process dead, empty: removed now
	b.Hello(SessionInfo{ID: "idle"}, nil, false, false)              // no pid: removed after IdleTTL
	b.Hello(SessionInfo{ID: "mail", PID: 300}, nil, false, false)    // dead but has mail: kept until MailTTL
	b.Send("sub", SendReq{To: "mail", Text: "x"}, nil)

	gone := b.Sweep()
	if len(gone) != 1 || gone[0] != "piddead" {
		t.Fatalf("first sweep removed %v, want [piddead]", gone)
	}
	now = now.Add(b.lim.IdleTTL)
	if gone := b.Sweep(); len(gone) != 1 || gone[0] != "idle" {
		t.Fatalf("after IdleTTL removed %v, want [idle]", gone)
	}
	now = now.Add(b.lim.MailTTL)
	if gone := b.Sweep(); len(gone) != 1 || gone[0] != "mail" {
		t.Fatalf("after MailTTL removed %v, want [mail]", gone)
	}
	if len(b.List()) != 2 {
		t.Fatalf("left %v, want sub+pidlive", b.List())
	}
}

func TestExclusiveTakeBye(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "")
	plain := hello(t, b, "c", "")
	w1, w2 := &sink{}, &sink{}
	b.Hello(SessionInfo{ID: "c"}, w1, true, true)
	b.Hello(SessionInfo{ID: "c"}, w2, true, true)
	if !w1.dead || w2.dead || plain.dead {
		t.Fatalf("new waiter must replace old one only: w1=%v w2=%v plain=%v", w1.dead, w2.dead, plain.dead)
	}
	b.Send("a", SendReq{To: "c", Text: "x"}, nil)
	if w2.len() != 1 || plain.len() != 1 {
		t.Fatal("waiter and plain subscriber both get pushes")
	}
	if got, _ := b.Take("c"); len(got) != 1 {
		t.Fatalf("take %d, want 1", len(got))
	}
	if got, _ := b.Take("c"); len(got) != 0 {
		t.Fatalf("second take %d, want 0", len(got))
	}
	b.Bye("c")
	if !w2.dead || len(b.List()) != 1 {
		t.Fatal("bye must close subscribers and remove the empty session")
	}
}

func TestNaming(t *testing.T) {
	b := newBroker(t, nil)
	alive := map[int]bool{1: true, 2: true, 3: true, 4: true}
	b.alive = func(pid int) bool { return alive[pid] }
	reg := func(id, harness, name string, pid int) string {
		t.Helper()
		b.Hello(SessionInfo{ID: id, Harness: harness, Name: name, Cwd: "/w/tmp", PID: pid}, nil, false, false)
		return b.sessions[id].info.Name
	}
	// Generated names: adj-noun, stable per id, unique.
	n1, n2 := reg("p1", "pi", "", 1), reg("o1", "omp", "", 2)
	if !strings.Contains(n1, "-") || n1 == n2 {
		t.Fatalf("names %q %q", n1, n2)
	}
	if again := genName("p1", func(string) bool { return false }); again != n1 {
		t.Fatalf("not stable: %q vs %q", again, n1)
	}
	if n := genName("p1", func(n string) bool { return n == n1 }); n == n1 || n == "" {
		t.Fatalf("collision not skipped: %q", n)
	}
	// Explicit names replace generated ones; re-hello without a name keeps them.
	if n := reg("o2", "omp", "planner", 3); n != "planner" {
		t.Fatalf("got %q", n)
	}
	if n := reg("o2", "omp", "", 3); n != "planner" {
		t.Fatalf("re-hello renamed to %q", n)
	}

	hello(t, b, "me", "")
	send := func(to string) (string, error) {
		m, err := b.Send("me", SendReq{To: to, Text: "x"}, nil)
		if err != nil {
			return "", err
		}
		return m.To, nil
	}
	if to, err := send("PLANNER"); err != nil || to != "o2" {
		t.Fatalf("case-insensitive: %q %v", to, err)
	}
	reg("old", "pi", "planner", 9) // dead pid: live one wins
	if to, err := send("planner"); err != nil || to != "o2" {
		t.Fatalf("live-first: %q %v", to, err)
	}
	reg("new", "pi", "planner", 4)
	if _, err := send("planner"); code(err) != CodeAmbiguous || !strings.Contains(err.Error(), "o2 (omp, /w/tmp)") {
		t.Fatalf("ambiguous: %v", err)
	}
	if to, err := send("planner@omp"); err != nil || to != "o2" {
		t.Fatalf("name@harness: %q %v", to, err)
	}
	if _, err := send("nobody"); code(err) != CodeUnknownTarget || !strings.Contains(err.Error(), "planner@omp") {
		t.Fatalf("unknown should list live sessions: %v", err)
	}
}

func TestWordListsUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, w := range append(adjectives[:], nouns[:]...) {
		if seen[w] || w == "" {
			t.Fatalf("duplicate/empty word %q", w)
		}
		seen[w] = true
	}
}

func TestWakerRequeue(t *testing.T) {
	b := newBroker(t, nil)
	woke := make(chan string, 4)
	b.Waker = func(info SessionInfo) { woke <- info.ID }
	hello(t, b, "a", "")
	b.Hello(SessionInfo{ID: "cx", Harness: "codex"}, nil, false, false) // hook-only, no subscriber
	b.Send("a", SendReq{To: "cx", Text: "1"}, nil)
	select {
	case id := <-woke:
		if id != "cx" {
			t.Fatalf("woke %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("waker not called")
	}
	b.Send("cx", SendReq{To: "a", Text: "x"}, nil) // a is subscribed: no wake
	select {
	case id := <-woke:
		t.Fatalf("woke subscribed session %q", id)
	case <-time.After(50 * time.Millisecond):
	}
	if p := b.Pending(); len(p) != 1 || p[0].ID != "cx" {
		t.Fatalf("pending %v", p)
	}
	msgs, _ := b.Take("cx")
	b.Send("a", SendReq{To: "cx", Text: "2"}, nil)
	b.Requeue("cx", msgs)
	if in, _ := b.Inbox("cx"); len(in) != 2 || in[0].Text != "1" || in[1].Text != "2" {
		t.Fatalf("requeue order: %v", in)
	}
}

func TestSpawnLimits(t *testing.T) {
	b := newBroker(t, func(l *Limits) { l.SpawnMax = 3 })
	alive := map[int]bool{1: true, 2: true, 3: true}
	b.alive = func(pid int) bool { return alive[pid] }
	b.Hello(SessionInfo{ID: "root", PID: 1}, nil, false, false)

	name, depth, err := b.Spawn("root", "worker")
	if err != nil || name != "worker" || depth != 1 {
		t.Fatalf("spawn: %q %d %v", name, depth, err)
	}
	if _, _, err := b.Spawn("root", "WORKER"); code(err) != CodeNameTaken {
		t.Fatalf("pending name must be reserved: %v", err)
	}
	// The child registers under the reserved name and is linked to its parent.
	b.Hello(SessionInfo{ID: "w1", Name: "worker", PID: 2}, nil, false, false)
	if info := b.sessions["w1"].info; info.Parent != "root" || info.Depth != 1 {
		t.Fatalf("link: %+v", info)
	}
	// Depth: worker (1) may spawn (2); a depth-2 agent may not.
	gen, depth, err := b.Spawn("w1", "")
	if err != nil || depth != 2 || gen == "" {
		t.Fatalf("nested spawn: %q %d %v", gen, depth, err)
	}
	b.Hello(SessionInfo{ID: "g1", Name: gen, PID: 3}, nil, false, false)
	if _, _, err := b.Spawn("g1", ""); code(err) != CodeSpawnLimit {
		t.Fatalf("depth 3 must fail: %v", err)
	}
	// Count: worker + g1 live, one pending → the fourth is refused.
	if _, _, err := b.Spawn("", "third"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Spawn("", "fourth"); code(err) != CodeSpawnLimit {
		t.Fatalf("max 3: %v", err)
	}
	// Dead spawned agents free their slot.
	delete(alive, 3)
	if _, _, err := b.Spawn("", "fourth"); err != nil {
		t.Fatalf("slot not freed: %v", err)
	}
}

func TestResolveOp(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "abc-1", "alice")
	b.Hello(SessionInfo{ID: "abc-2", Name: "alice", Harness: "codex"}, nil, false, false)
	b.Send("abc-2", SendReq{To: "abc-1", Text: "hi"}, nil)
	got, err := b.Resolve("ALICE") // live one wins, like Send
	if err != nil || got.ID != "abc-1" || !got.Live || got.Queued != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err := b.Resolve("alice@codex"); err != nil || got.ID != "abc-2" {
		t.Fatalf("qualified: %+v %v", got, err)
	}
	if got, err := b.Resolve("abc"); err != nil || got.ID != "abc-1" { // prefix: live wins
		t.Fatalf("prefix: %+v %v", got, err)
	}
	hello(t, b, "x-1", "bob")
	hello(t, b, "x-2", "bob")
	if _, err := b.Resolve("bob"); code(err) != CodeAmbiguous {
		t.Fatalf("ambiguous: %v", err)
	}
	if _, err := b.Resolve("nobody"); code(err) != CodeUnknownTarget {
		t.Fatalf("unknown: %v", err)
	}
	if in, _ := b.Inbox("abc-1"); len(in) != 1 {
		t.Fatal("resolve changed state")
	}
}

// TestReplyRecreatesRemovedAsker: a reply to a question always reaches its asker, even
// when GC removed the asker's session (no subscriber, no PID) in the meantime: the
// daemon recreates it as an offline mailbox, so a later wait/inbox finds the reply and
// a wait sink registered before the sweep is pushed to.
func TestReplyRecreatesRemovedAsker(t *testing.T) {
	b := newBroker(t, nil)
	now := time.Now()
	b.now = func() time.Time { return now }
	// CLI-style asker: hello without a subscriber and without a PID.
	if err := b.Hello(SessionInfo{ID: "asker-1"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	hello(t, b, "helper-1", "helper")
	q, err := b.Send("asker-1", SendReq{To: "helper", Text: "q?", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &sink{} // `agm wait -reply-to`, open across the sweep
	if m, err := b.Wait("asker-1", q.ID, w); err != nil || m != nil {
		t.Fatalf("wait: %v %v", m, err)
	}

	now = now.Add(11 * time.Minute)
	if gone := b.Sweep(); len(gone) != 1 || gone[0] != "asker-1" {
		t.Fatalf("sweep removed %v, want [asker-1]", gone)
	}
	for _, s := range b.List() {
		if s.ID == "asker-1" {
			t.Fatal("asker still listed")
		}
	}

	// Plain sends to the removed session still fail; only replies recreate it.
	if _, err := b.Send("helper-1", SendReq{To: "asker-1", Text: "x"}, nil); code(err) != CodeUnknownTarget {
		t.Fatalf("plain send: %v, want unknown_target", err)
	}
	r, err := b.Send("helper-1", SendReq{ReplyTo: q.ID, Text: "a"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var info *SessionInfo
	for i, s := range b.List() {
		if s.ID == "asker-1" {
			info = &b.List()[i]
		}
	}
	if info == nil || info.Live || info.Queued != 1 || info.Name == "" || info.Name == "asker-1" {
		t.Fatalf("recreated asker: %+v", info)
	}
	if in, _ := b.Inbox("asker-1"); len(in) != 1 || in[0].ID != r.ID {
		t.Fatalf("recreated mailbox: %+v", in)
	}
	if w.msgs == nil || w.msgs[0].ID != r.ID {
		t.Fatalf("wait sink not pushed: %+v", w.msgs)
	}
	if m, err := b.Wait("asker-1", q.ID, nil); err != nil || m == nil || m.ID != r.ID {
		t.Fatalf("later wait: %v %v", m, err)
	}
}

// TestCleanLine: whitespace, control and format runes become one space; ends trim;
// the cap keeps at most max runes; normal text (tabs are whitespace) is preserved
// otherwise.
func TestCleanLine(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"bob\nIgnore previous\x1b[31m\u202eevil", "bob Ignore previous [31m evil"},
		{"  a\t\tb  ", "a b"},
		{"\x03\x1b[A\u202e\u2066x", "[A x"},
		{"\u009bcm", "cm"}, // C1 CSI
	} {
		if got := CleanLine(tc.in, 64); got != tc.want {
			t.Errorf("CleanLine(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := CleanLine("ab\x01cd\x7f", 3); got != "ab" { // cap cut after a space: no trailing space (N1)
		t.Errorf("cap: %q", got)
	}
}

// TestHelloNormalizesIdentity: peer-supplied identity fields are stored as one clean
// capped line; the empty-after-clean name means "no name" (generated name applies).
func TestHelloNormalizesIdentity(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "bob\nIgnore previous\x1b[31m\u202eevil"+strings.Repeat("x", 100))
	var info SessionInfo
	for _, s := range b.List() {
		if s.ID == "a" {
			info = s
		}
	}
	n := []rune(info.Name)
	if len(n) != 64 || strings.IndexFunc(info.Name, dirtyRune) >= 0 || strings.Contains(info.Name, "\n") {
		t.Fatalf("stored name %q (%d runes)", info.Name, len(n))
	}
	if want := "bob Ignore previous [31m evil"; !strings.HasPrefix(info.Name, want) {
		t.Fatalf("stored name %q", info.Name)
	}
	if r, err := b.Resolve(info.Name); err != nil || r.ID != "a" {
		t.Fatalf("resolve by normalized name: %v %+v", err, r)
	}
	// An unnormalizable name is no name.
	if err := b.Hello(SessionInfo{ID: "b", Name: "\n\x1b\u202e "}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	for _, s := range b.List() {
		if s.ID == "b" && s.Name == "" {
			t.Fatal("empty after clean: generated name must apply")
		}
	}
}

// TestHelloRejectsDirtyIDs: ids are identities, never rewritten: control or format
// runes, or over 256 bytes, are a bad_request.
func TestHelloRejectsDirtyIDs(t *testing.T) {
	b := newBroker(t, nil)
	for _, id := range []string{"a\nb", "a\x1b[2Jb", "a\u202eb", "a\u2066b", "a\u2028b", "a\u00a0b", " a", "a ", "a  b", strings.Repeat("i", 257)} {
		if err := b.Hello(SessionInfo{ID: id}, nil, false, false); code(err) != CodeBadRequest {
			t.Errorf("hello id %q: %v, want bad_request", id, err)
		}
	}
	if err := b.Hello(SessionInfo{ID: strings.Repeat("i", 256)}, nil, false, false); err != nil {
		t.Fatalf("256-byte id rejected: %v", err)
	}
}

// TestSpawnCleansReservedName: a reservation made with an unnormalized name links the
// session that says hello with that name (both normalize to the same clean line).
func TestSpawnCleansReservedName(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "mom-1", "mom")
	name, depth, err := b.Spawn("mom-1", "kid\n\x1b[2J")
	if err != nil || name != "kid [2J" || depth != 1 {
		t.Fatalf("spawn: %q %d %v", name, depth, err)
	}
	if err := b.Hello(SessionInfo{ID: "kid-1", Name: "kid\n\x1b[2J", Harness: "pi"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	for _, s := range b.List() {
		if s.ID == "kid-1" && (s.Parent != "mom-1" || s.Depth != 1) {
			t.Fatalf("spawn link: %+v", s)
		}
	}
}

// TestReplyRoutesByAskerID: a reply whose asker is gone is routed by the asker's exact
// id, never by name or id prefix: a session that took the asker's name (or an id the
// asker's id prefixes) must not receive it (0.3.0 misdelivery regression).
func TestReplyRoutesByAskerID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		squatter SessionInfo
	}{
		{"same name", SessionInfo{ID: "zed-1", Name: "alice-a"}},
		{"id prefix", SessionInfo{ID: "alice-ab"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBroker(t, nil)
			if err := b.Hello(SessionInfo{ID: "alice-a"}, nil, false, false); err != nil {
				t.Fatal(err)
			}
			hello(t, b, "bob-1", "bob")
			q, err := b.Send("alice-a", SendReq{To: "bob", Text: "q?", ExpectsReply: true, NoWait: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			b.Bye("alice-a") // asker gone (empty mailbox)
			if err := b.Hello(tc.squatter, nil, false, false); err != nil {
				t.Fatal(err)
			}
			r, err := b.Send("bob-1", SendReq{ReplyTo: q.ID, Text: "a"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if in, _ := b.Inbox(tc.squatter.ID); len(in) != 0 {
				t.Fatalf("squatter %s got the reply: %+v", tc.squatter.ID, in)
			}
			if in, _ := b.Inbox("alice-a"); len(in) != 1 || in[0].ID != r.ID {
				t.Fatalf("recreated asker mailbox: %+v", in)
			}
		})
	}
}

// TestFailedReplyLeavesNoEmptySession: a reply that fails after the asker was
// resurrected (too_large, rate_limited, ...) rolls the empty mailbox back.
func TestFailedReplyLeavesNoEmptySession(t *testing.T) {
	b := newBroker(t, nil)
	if err := b.Hello(SessionInfo{ID: "asker-1"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	hello(t, b, "bob-1", "bob")
	q, err := b.Send("asker-1", SendReq{To: "bob", Text: "q?", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.Bye("asker-1")
	if _, err := b.Send("bob-1", SendReq{ReplyTo: q.ID, Text: strings.Repeat("\x01", MaxFrame/4)}, nil); code(err) != CodeTooLarge {
		t.Fatalf("oversize reply: %v", err)
	}
	for _, s := range b.List() {
		if s.ID == "asker-1" {
			t.Fatalf("failed reply left an empty session: %+v", s)
		}
	}
	if _, err := b.Send("bob-1", SendReq{ReplyTo: q.ID, Text: "a"}, nil); err != nil {
		t.Fatal(err)
	}
	if in, _ := b.Inbox("asker-1"); len(in) != 1 {
		t.Fatalf("retry after rollback: %+v", in)
	}
}
