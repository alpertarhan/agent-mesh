package main

// In-process bridge tests (3c.1): two brokers on two temp sockets, a Bridge
// between them, no ssh. Ticks are driven by hand (tickOnce) so the scenarios
// are deterministic.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// bridgeRig owns the two daemons and the bridge-under-test.
type bridgeRig struct {
	t       *testing.T
	dir     string
	socks   [2]string
	brokers [2]*broker.Broker
	b       *Bridge
	logs    *lockedStrings
	ctx     context.CancelFunc
}

// brokerAt starts a daemon on an explicit socket with the given limits.
func brokerAt(t *testing.T, sock string, lim broker.Limits) *broker.Broker {
	b, _, _ := brokerAtS(t, sock, lim)
	return b
}

// brokerAtS also returns the cancel and done channel, so a test can stop one
// daemon mid-run and start a fresh one on the same socket (T6).
func brokerAtS(t *testing.T, sock string, lim broker.Limits) (*broker.Broker, context.CancelFunc, <-chan error) {
	t.Helper()
	ln, err := broker.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := broker.New(lim, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
	return b, cancel, done
}

type lockedStrings struct {
	mu sync.Mutex
	v  []string
}

func (l *lockedStrings) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.v = append(l.v, s)
}

func (l *lockedStrings) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.v...)
}

func newBridgeRig(t *testing.T, lim broker.Limits) *bridgeRig {
	return newBridgeRig2(t, lim, lim)
}

// newBridgeRig2 allows different limits per daemon (e.g. a stricter far side).
func newBridgeRig2(t *testing.T, lim0, lim1 broker.Limits) *bridgeRig {
	return newBridgeRig2h(t, lim0, lim1, nil)
}

// newBridgeRig2h also arms something on the bridge before Run starts (the
// restart test's crash-window hook: written exactly once, before any reader).
func newBridgeRig2h(t *testing.T, lim0, lim1 broker.Limits, beforeStart func(*Bridge)) *bridgeRig {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "agmbridge")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := &bridgeRig{t: t, dir: dir, logs: &lockedStrings{}}
	r.socks = [2]string{filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")}
	r.brokers[0] = brokerAt(t, r.socks[0], lim0)
	r.brokers[1] = brokerAt(t, r.socks[1], lim1)
	// t.Logf is only safe from the test goroutine while it runs: bridge
	// goroutines outlive the test body, so log after the end is dropped.
	var done atomic.Bool
	t.Cleanup(func() { done.Store(true) })
	r.b = NewBridge(r.socks[0], r.socks[1], "laptop/", "srv/", filepath.Join(dir, "link.map"), func(f string, a ...any) {
		r.logs.add(fmt.Sprintf(f, a...))
		if !done.Load() {
			t.Logf(f, a...)
		}
	})
	r.b.tick = 50 * time.Millisecond
	if beforeStart != nil {
		beforeStart(r.b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = cancel
	go r.b.Run(ctx)
	return r
}

// newBridgeRigManual is a rig whose bridge does not tick on its own: the test
// calls tickOnce by hand, so mirror and bye timing is deterministic. The
// bridge is built directly (Run is never started, so b.ctx is written once,
// before any reader exists).
func newBridgeRigManual(t *testing.T, lim broker.Limits) *bridgeRig {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "agmbridge")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	r := &bridgeRig{t: t, dir: dir, logs: &lockedStrings{}}
	r.socks = [2]string{filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")}
	r.brokers[0] = brokerAt(t, r.socks[0], lim)
	r.brokers[1] = brokerAt(t, r.socks[1], lim)
	var done atomic.Bool
	t.Cleanup(func() { done.Store(true) })
	r.b = NewBridge(r.socks[0], r.socks[1], "laptop/", "srv/", filepath.Join(dir, "link.map"), func(f string, a ...any) {
		r.logs.add(fmt.Sprintf(f, a...))
		if !done.Load() {
			t.Logf(f, a...)
		}
	})
	r.b.tick = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.ctx = cancel
	r.b.startCtx(ctx)
	return r
}

// bsession is a test session on one of the daemons: a connection with hello,
// collecting pushed messages.
type bsession struct {
	t    *testing.T
	conn *bconn
	id   string
	got  *lockedStrings // JSON of pushed messages
	mu   sync.Mutex
	msgs []*broker.Message
}

func dialSession(t *testing.T, sock, id, name, harness string, subscribe bool) *bsession {
	t.Helper()
	s := &bsession{t: t, id: id, got: &lockedStrings{}}
	var err error
	s.conn, err = dialBConn(sock, func(m *broker.Message) {
		s.mu.Lock()
		s.msgs = append(s.msgs, m)
		s.mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.conn.close)
	info := broker.SessionInfo{ID: id, Name: name, Harness: harness}
	if subscribe {
		info.PID = 0 // pid-less: live only while subscribed
	}
	if err := s.conn.callDL(context.Background(), broker.Request{Op: "hello", Session: &info, Subscribe: subscribe}, nil, fixedDL(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *bsession) call(op string, req broker.Request, out any) error {
	s.t.Helper()
	req.Op = op
	return s.conn.callDL(context.Background(), req, out, fixedDL(74*time.Second))
}

func (s *bsession) send(r broker.SendReq) *broker.Message {
	s.t.Helper()
	var m broker.Message
	if err := s.call("send", broker.Request{SendReq: r}, &m); err != nil {
		s.t.Fatal(err)
	}
	return &m
}

// waitMsg waits for a pushed message matching pred.
func (s *bsession) waitMsg(pred func(*broker.Message) bool, what string) *broker.Message {
	s.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for _, m := range s.msgs {
			if pred(m) {
				s.mu.Unlock()
				return m
			}
		}
		s.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	s.t.Fatalf("timed out waiting for %s", what)
	return nil
}

func (s *bsession) msgsFrom(sub string) []*broker.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*broker.Message
	for _, m := range s.msgs {
		if strings.HasPrefix(m.From, sub) {
			out = append(out, m)
		}
	}
	return out
}

// listOn returns the session list of a daemon.
func listOn(t *testing.T, sock string) []broker.SessionInfo {
	t.Helper()
	c, err := dialBConn(sock, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	var list []broker.SessionInfo
	if err := c.callDL(context.Background(), broker.Request{Op: "list"}, &list, fixedDL(74*time.Second)); err != nil {
		t.Fatal(err)
	}
	return list
}

func findSession(list []broker.SessionInfo, pred func(broker.SessionInfo) bool) *broker.SessionInfo {
	for i := range list {
		if pred(list[i]) {
			return &list[i]
		}
	}
	return nil
}

// tickN lets the bridge run a few ticks.
func (r *bridgeRig) tickN(n int) {
	time.Sleep(time.Duration(n)*r.b.tick + 30*time.Millisecond)
}

// T1: mirroring, both directions, never for "/" ids; field changes re-hello;
// live follows the real session; gone sessions lose their proxy; the cap holds;
// repeated ticks are stable.
func TestBridgeMirroring(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	_ = alice
	r.tickN(3)

	// Both directions mirrored with the prefix.
	l := listOn(t, r.socks[0])
	px := findSession(l, func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" })
	if px == nil {
		t.Fatalf("no srv/bob-1 proxy on the local daemon: %+v", l)
	}
	if px.Name != "srv/bob" || px.Harness != "pi" || px.PID != 0 {
		t.Fatalf("proxy fields wrong: %+v", px)
	}
	l = listOn(t, r.socks[1])
	if findSession(l, func(s broker.SessionInfo) bool { return s.ID == "laptop/alice-1" }) == nil {
		t.Fatalf("no laptop/alice-1 proxy on the remote daemon: %+v", l)
	}

	// Gate-like and prefixed ids are never mirrored.
	gated := dialSession(t, r.socks[1], "gated/x", "gated", "openclaw", true)
	r.tickN(3)
	for _, s := range listOn(t, r.socks[0]) {
		if strings.HasPrefix(s.ID, "srv/gated") {
			t.Fatalf("a / session was mirrored: %+v", s)
		}
	}
	_ = gated

	// Field changes reach the proxy by re-hello (addition c).
	info := broker.SessionInfo{ID: "bob-1", Name: "bobby", Harness: "pi"}
	if err := bob.conn.callDL(context.Background(), broker.Request{Op: "hello", Session: &info, Subscribe: true}, nil, fixedDL(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	r.tickN(3)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if px = findSession(listOn(t, r.socks[0]), func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" }); px != nil && px.Name == "srv/bobby" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if px == nil || px.Name != "srv/bobby" {
		t.Fatalf("proxy name did not follow: %+v", px)
	}

	// Live follows the real session: unsubscribe by closing, then re-hello.
	bob.conn.close()
	waitFor(t, 3*time.Second, func() bool {
		l := listOn(t, r.socks[0])
		px := findSession(l, func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" })
		t.Logf("DEBUG poll srv/bob-1: %+v", px)
		return px != nil && !px.Live
	}, "proxy stayed live after the real session went offline")
	nb := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	waitFor(t, 3*time.Second, func() bool {
		return findSession(listOn(t, r.socks[0]), func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" && s.Live }) != nil
	}, "proxy did not return to live")
	_ = nb

	// Gone: the real session byes; the proxy drains and disappears. Bye
	// closes the caller's subscription, so a transport error is expected.
	_ = nb.call("bye", broker.Request{}, nil)
	waitFor(t, 3*time.Second, func() bool {
		return findSession(listOn(t, r.socks[0]), func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" }) == nil
	}, "proxy survived its real session")

	// Stability: several ticks with sessions on both sides, no churn (h).
	before := idsWith(listOn(t, r.socks[1]), "laptop/")
	r.tickN(5)
	after := idsWith(listOn(t, r.socks[1]), "laptop/")
	if strings.Join(before, ",") != strings.Join(after, ",") {
		t.Fatalf("proxy churn across ticks: %v -> %v", before, after)
	}
}

// T1b: the mirror cap is 64, deterministic: live first, then newest - the
// ten newest sessions are offline, so live-first must keep older live ones
// over newer offline ones (M7).
func TestBridgeMirrorCap(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	var live []*bsession
	for i := 0; i < 60; i++ { // s00..s59: live
		live = append(live, dialSession(t, r.socks[0], fmt.Sprintf("s%02d", i), fmt.Sprintf("s%02d", i), "pi", true))
	}
	r.tickN(3)                 // mirror the live set first
	for i := 60; i < 70; i++ { // s60..s69: offline (registered, not subscribed)
		s := dialSession(t, r.socks[0], fmt.Sprintf("s%02d", i), fmt.Sprintf("s%02d", i), "pi", true)
		s.conn.close()
	}
	r.tickN(5)
	proxies := idsWith(listOn(t, r.socks[1]), "laptop/")
	if len(proxies) != bridgeMirrorCap {
		t.Fatalf("%d proxies, want %d", len(proxies), bridgeMirrorCap)
	}
	// All 60 live sessions are mirrored; the remaining four slots go to the
	// newest offline ones (s66..s69), not to s60..s63.
	for i := 0; i < 60; i++ {
		if !contains(proxies, fmt.Sprintf("laptop/s%02d", i)) {
			t.Fatalf("live session s%02d fell out of the cap", i)
		}
	}
	for _, want := range []string{"laptop/s66", "laptop/s67", "laptop/s68", "laptop/s69"} {
		if !contains(proxies, want) {
			t.Fatalf("newest offline %s not mirrored: %v", want, proxies)
		}
	}
	for _, out := range []string{"laptop/s60", "laptop/s61", "laptop/s62", "laptop/s63"} {
		if contains(proxies, out) {
			t.Fatalf("older offline %s mirrored over a live session", out)
		}
	}
	_ = live
}

// T2: sends both ways through the proxies; the receiver sees the proxy as the
// sender; attachments are relayed and refs are dropped with a note; replies
// map reply_to back to the original question id.
func TestBridgeSendBothWays(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	q := alice.send(broker.SendReq{To: "srv/bob", Text: "hello across",
		Attachments: []broker.Attachment{
			{Type: "snippet", Name: "s", Content: "x"},
			{Type: "ref", Name: "notes", Path: "/home/bob/notes.txt"},
		}})
	m := bob.waitMsg(func(m *broker.Message) bool { return strings.HasPrefix(m.Text, "hello across") }, "relayed message")
	if !strings.HasPrefix(m.From, "laptop/") || m.From != "laptop/alice-1" {
		t.Fatalf("sender is not the proxy: %+v", m)
	}
	if len(m.Attachments) != 1 || m.Attachments[0].Type != "snippet" {
		t.Fatalf("attachments not relayed: %+v", m.Attachments)
	}
	if !strings.Contains(m.Text, "dropped reference to notes (/home/bob/notes.txt is on laptop)") {
		t.Fatalf("ref note missing: %q", m.Text)
	}

	// The reply maps back to alice's question id.
	bob.send(broker.SendReq{To: m.From, Text: "hi back", ReplyTo: m.ID})
	reply := alice.waitMsg(func(m *broker.Message) bool { return m.Text == "hi back" }, "relayed reply")
	if reply.From != "srv/bob-1" {
		t.Fatalf("reply sender: %+v", reply)
	}
	if reply.ReplyTo != q.ID {
		t.Fatalf("reply_to not mapped: %q want %q", reply.ReplyTo, q.ID)
	}

	// Server-started: bob messages alice first.
	bob.send(broker.SendReq{To: "laptop/alice", Text: "knock knock"})
	first := alice.waitMsg(func(m *broker.Message) bool { return m.Text == "knock knock" }, "server-started message")
	if first.From != "srv/bob-1" {
		t.Fatalf("server-started sender: %+v", first)
	}

	// M10: every relay acked at its source - the proxy mailboxes are empty
	// (without acks they would fill to MailboxCap and reject further mail).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		l0 := listOn(t, r.socks[0])
		l1 := listOn(t, r.socks[1])
		p0 := findSession(l0, func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" })
		p1 := findSession(l1, func(s broker.SessionInfo) bool { return s.ID == "laptop/alice-1" })
		if p0 != nil && p1 != nil && p0.Queued == 0 && p1.Queued == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("proxy mailboxes did not drain to zero after the relays")
}

// T3: a no_wait ask across, answered with wait -reply-to; history -thread shows
// the mirrored thread on both sides.
func TestBridgeAskAndWait(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	q := alice.send(broker.SendReq{To: "srv/bob", Text: "what is 2+2", ExpectsReply: true, NoWait: true})
	m := bob.waitMsg(func(m *broker.Message) bool { return m.Text == "what is 2+2" }, "the ask")
	if !m.ExpectsReply {
		t.Fatalf("expects_reply lost: %+v", m)
	}
	bob.send(broker.SendReq{To: m.From, Text: "four", ReplyTo: m.ID})

	// wait -reply-to on alice's side: the call registers a watcher and returns
	// empty; the reply is pushed with the mapped reply_to.
	var waited broker.Message
	done := make(chan error, 1)
	go func() {
		done <- alice.call("wait", broker.Request{IDs: []string{q.ID}}, &waited)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait -reply-to did not resolve")
	}
	pushed := alice.waitMsg(func(m *broker.Message) bool { return m.Text == "four" && m.ReplyTo == q.ID }, "the waited reply")
	if pushed.From != "srv/bob-1" {
		t.Fatalf("waited reply sender: %+v", pushed)
	}

	// Threads on both sides (history returns Summary records).
	var th []broker.Summary
	if err := alice.call("history", broker.Request{Thread: q.ID, Limit: 50}, &th); err != nil {
		t.Fatal(err)
	}
	if len(th) != 2 || th[0].ID != q.ID || th[0].Dir != "out" || th[1].ReplyTo != q.ID {
		t.Fatalf("local thread: %+v", th)
	}
	if err := bob.call("history", broker.Request{Thread: m.ID, Limit: 50}, &th); err != nil {
		t.Fatal(err)
	}
	if len(th) != 2 || th[0].ID != m.ID || th[0].Dir != "in" || th[0].From != "laptop/alice-1" || th[1].ReplyTo != m.ID || th[1].Dir != "out" {
		t.Fatalf("remote thread: %+v", th)
	}
}

// T4: a reply chain across the bridge hits hop_limit on both daemons at the
// same depth: each side derives hop from its own mapped reply_to chain.
func TestBridgeHopLimit(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	// Alternate reply-asks; each crossing maps reply_to, so each daemon's own
	// chain grows by one per round, in lockstep. Track each side's ids: a
	// relayed copy's reply_to is the mapped id of the message it answers.
	aliceQ := alice.send(broker.SendReq{To: "srv/bob", Text: "q0", ExpectsReply: true, NoWait: true})
	cur := bob.waitMsg(func(m *broker.Message) bool { return m.Text == "q0" }, "q0")
	var aliceErr, bobErr error
	rounds := 0
	for ; rounds < 30; rounds++ {
		bm, err := sendReq(bob, broker.SendReq{To: cur.From, Text: fmt.Sprintf("b%d", rounds), ReplyTo: cur.ID, ExpectsReply: true, NoWait: true})
		if err != nil {
			bobErr = err
			break
		}
		am := alice.waitMsg(func(m *broker.Message) bool { return m.ReplyTo == aliceQ.ID && m.Text == fmt.Sprintf("b%d", rounds) }, fmt.Sprintf("b%d", rounds))
		am2, err := sendReq(alice, broker.SendReq{To: "srv/bob", Text: fmt.Sprintf("a%d", rounds), ReplyTo: am.ID, ExpectsReply: true, NoWait: true})
		if err != nil {
			aliceErr = err
			break
		}
		aliceQ = am2
		cur = bob.waitMsg(func(m *broker.Message) bool { return m.ReplyTo == bm.ID && m.Text == fmt.Sprintf("a%d", rounds) }, fmt.Sprintf("a%d", rounds))
	}
	if rounds >= 30 {
		t.Fatal("hop limit never hit")
	}
	// Whichever side hit it first, the other side is at the same depth: its
	// next reply-ask must also fail with hop_limit, not succeed.
	first := aliceErr
	if first == nil {
		first = bobErr
	}
	if be, ok := first.(*broker.Error); !ok || be.Code != broker.CodeHopLimit {
		t.Fatalf("chain stopped for another reason: %v", first)
	}
	var other error
	if aliceErr != nil {
		_, other = sendReq(bob, broker.SendReq{To: cur.From, Text: "probe", ReplyTo: cur.ID, ExpectsReply: true, NoWait: true})
	} else {
		_, other = sendReq(alice, broker.SendReq{To: "srv/bob", Text: "probe", ReplyTo: aliceQ.ID, ExpectsReply: true, NoWait: true})
	}
	if be, ok := other.(*broker.Error); !ok || be.Code != broker.CodeHopLimit {
		t.Fatalf("the other side is not at the limit: %v", other)
	}
	if rounds+1 < 4 { // sanity: the chain ran a few crossings at least
		t.Fatalf("limit hit too early: %d rounds", rounds)
	}
}

// T5: a stop between the far send and the near ack (after the map write) sends
// no duplicate after the restart, and replies to earlier messages still map.
func TestBridgeRestartNoDuplicate(t *testing.T) {
	// Crash window: stop the bridge after the far send, before the near ack
	// (the map pair is already recorded in memory and on disk). The hook is
	// armed before Run starts (written exactly once) and fires on its second
	// call: the "first" relay passes, the "second" relay stops.
	stop := make(chan struct{})
	var hookCalls atomic.Int64
	r := newBridgeRig2h(t, broker.DefaultLimits(), broker.DefaultLimits(), func(b *Bridge) {
		b.testHookPostSend = func() {
			if hookCalls.Add(1) == 2 {
				close(stop)
				<-make(chan struct{}) // block the relay goroutine forever
			}
		}
	})
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	// Establish a mapped pair.
	q1 := alice.send(broker.SendReq{To: "srv/bob", Text: "first", ExpectsReply: true, NoWait: true})
	m1 := bob.waitMsg(func(m *broker.Message) bool { return m.Text == "first" }, "first")

	q2 := alice.send(broker.SendReq{To: "srv/bob", Text: "second", ExpectsReply: true, NoWait: true})
	<-stop
	r.ctx() // cancel the bridge's context
	time.Sleep(100 * time.Millisecond)

	// Restart with the same map file.
	b2 := NewBridge(r.socks[0], r.socks[1], "laptop/", "srv/", filepath.Join(r.dir, "link.map"), func(f string, a ...any) { t.Logf(f, a...) })
	b2.tick = 50 * time.Millisecond
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go b2.Run(ctx2)
	defer b2.Stop()

	// Both sides of the restarted bridge must be up before replying (a push
	// that races the reverse mirror bounces as unmirrored).
	mirrored(t, r.socks[1], "laptop/alice-1")
	mirrored(t, r.socks[0], "srv/bob-1")

	// The replayed "second" is deduped (mapped before the stop): no
	// duplicate. Count only after a marker has crossed the same serial path,
	// so the replay has certainly been processed (M2's test gap).
	alice.send(broker.SendReq{To: "srv/bob", Text: "post-restart marker"})
	bob.waitMsg(func(m *broker.Message) bool { return m.Text == "post-restart marker" }, "the marker")
	count := 0
	for _, m := range bob.msgsFrom("laptop/") {
		if m.Text == "second" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("second delivered %d times, want 1", count)
	}

	// Replies to the pre-restart exchange still map.
	bob.send(broker.SendReq{To: m1.From, Text: "late answer", ReplyTo: m1.ID})
	reply := alice.waitMsg(func(m *broker.Message) bool { return m.Text == "late answer" }, "late answer")
	if reply.ReplyTo != q1.ID {
		t.Fatalf("pre-restart mapping lost: %q want %q", reply.ReplyTo, q1.ID)
	}
	_ = q2
}

// T7: transient errors retry in order; permanent errors bounce (an ask's
// bounce is a reply); an unmapped reply_to goes out with the unlinked note.
func TestBridgeErrors(t *testing.T) {
	// A stricter far daemon: alice's six sends fit her local budget, but the
	// laptop/alice proxy's sends on the far side exceed its burst, rate_limit
	// and are retried in order (the realistic case: a replay burst after a
	// reconnect, or a tighter far-side config).
	near := broker.DefaultLimits()
	far := broker.DefaultLimits()
	far.SendBurst = 3
	far.SendPerMin = 600 // refill fast enough that the retry lands
	r := newBridgeRig2(t, near, far)
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	for i := 0; i < 6; i++ {
		alice.send(broker.SendReq{To: "srv/bob", Text: fmt.Sprintf("burst %d", i)})
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(bob.msgsFrom("laptop/")) >= 6 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	var texts []string
	for _, m := range bob.msgsFrom("laptop/") {
		if strings.HasPrefix(m.Text, "burst") {
			texts = append(texts, m.Text)
		}
	}
	if len(texts) != 6 {
		t.Fatalf("delivered %d of 6: %v", len(texts), texts)
	}
	for i, tx := range texts {
		if tx != fmt.Sprintf("burst %d", i) {
			t.Fatalf("order broken at %d: %v", i, texts)
		}
	}

	// (The permanent-bounce path is TestBridgeBounceUnknownTarget: a frame
	// the CLI cannot even send never reaches the broker to bounce.)

	// Unmapped reply_to: restart the bridge with a fresh map, then reply to an
	// old mapped message: the relay sends it unlinked, with the note.
	r.ctx()
	time.Sleep(50 * time.Millisecond)
	b2 := NewBridge(r.socks[0], r.socks[1], "laptop/", "srv/", filepath.Join(r.dir, "fresh.map"), func(f string, a ...any) { t.Logf(f, a...) })
	b2.tick = 50 * time.Millisecond
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go b2.Run(ctx2)
	mirrored(t, r.socks[1], "laptop/alice-1")
	mirrored(t, r.socks[0], "srv/bob-1")
	bob.send(broker.SendReq{To: "laptop/alice", Text: "orphan reply", ReplyTo: m1id(t, bob, "burst 0")})
	orphan := alice.waitMsg(func(m *broker.Message) bool { return strings.HasPrefix(m.Text, "orphan reply") }, "the orphan reply")
	if orphan.ReplyTo != "" {
		t.Fatalf("orphan reply kept a reply_to: %q", orphan.ReplyTo)
	}
	if !strings.Contains(orphan.Text, "no longer mapped") && !strings.Contains(orphan.Text, "no longer known") {
		t.Fatalf("unlinked note missing: %q", orphan.Text)
	}
}

// T7b: a permanent relay error (unknown_target: the real session is gone
// before its proxy) bounces as a reply, so a blocking ask returns at once.
// Manual ticks keep the proxy alive past the send.
func TestBridgeBounceUnknownTarget(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	carol := dialSession(t, r.socks[1], "carol-1", "carol", "pi", true)
	r.b.tickOnce()
	r.b.tickOnce() // both sides mirrored; relays started

	// The real session goes away; its proxy survives until the next tick.
	_ = carol.call("bye", broker.Request{}, nil) // bye closes the caller's sub
	q := alice.send(broker.SendReq{To: "srv/carol-1", Text: "anyone?", ExpectsReply: true, NoWait: true})
	bounce := alice.waitMsg(func(m *broker.Message) bool {
		return strings.Contains(m.Text, "[agent-mesh link] not delivered to srv/carol-1")
	}, "the bounce")
	if !strings.Contains(bounce.Text, "unknown_target") {
		t.Fatalf("bounce reason: %q", bounce.Text)
	}
	if bounce.ReplyTo != q.ID {
		t.Fatalf("the bounce is not a reply to the ask: %+v", bounce)
	}
	// The failed ask is acked away at the source proxy.
	r.b.tickOnce()
	waitFor(t, 3*time.Second, func() bool {
		return findSession(listOn(t, r.socks[0]), func(s broker.SessionInfo) bool { return s.ID == "srv/carol-1" }) == nil
	}, "the dead proxy was not byed")
}

// M9: Stop tears the connections down: no proxy row stays live on either
// daemon (a stopped bridge must not keep sessions looking subscribed).
func TestBridgeStopTearsDown(t *testing.T) {
	r := newBridgeRig(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)
	_ = alice
	_ = bob
	r.b.Stop()
	waitFor(t, 3*time.Second, func() bool {
		l0 := listOn(t, r.socks[0])
		l1 := listOn(t, r.socks[1])
		p0 := findSession(l0, func(s broker.SessionInfo) bool { return s.ID == "srv/bob-1" })
		p1 := findSession(l1, func(s broker.SessionInfo) bool { return s.ID == "laptop/alice-1" })
		return (p0 == nil || !p0.Live) && (p1 == nil || !p1.Live)
	}, "a proxy stayed live after Stop")
}

// M12: mailbox_full on the target is retried in place, in order, and lands
// once the mailbox drains (the far side caps mailboxes at 2).
func TestBridgeMailboxFullRetried(t *testing.T) {
	near := broker.DefaultLimits()
	far := broker.DefaultLimits()
	far.MailboxCap = 2
	r := newBridgeRig2(t, near, far)
	alice := dialSession(t, r.socks[0], "alice-1", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob-1", "bob", "pi", true)
	r.tickN(3)

	// bob never acks: the first two fill his mailbox, the third gets
	// mailbox_full and must wait, not bounce.
	for i := 0; i < 3; i++ {
		alice.send(broker.SendReq{To: "srv/bob", Text: fmt.Sprintf("mf %d", i)})
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(bob.msgsFrom("laptop/")) < 2 {
		time.Sleep(50 * time.Millisecond)
	}
	if len(bob.msgsFrom("laptop/")) != 2 {
		t.Fatalf("expected the first two only, got %d", len(bob.msgsFrom("laptop/")))
	}
	time.Sleep(500 * time.Millisecond) // the third retries; it must not bounce
	for _, m := range alice.msgsFrom("srv/") {
		if strings.Contains(m.Text, "not delivered") {
			t.Fatalf("mailbox_full bounced instead of retrying: %q", m.Text)
		}
	}
	// Drain: the third lands, in order.
	if err := bob.call("ack", broker.Request{IDs: func() []string {
		var ids []string
		for _, m := range bob.msgsFrom("laptop/") {
			ids = append(ids, m.ID)
		}
		return ids
	}()}, nil); err != nil {
		t.Fatal(err)
	}
	bob.waitMsg(func(m *broker.Message) bool { return m.Text == "mf 2" }, "the third after the drain")
	var texts []string
	for _, m := range bob.msgsFrom("laptop/") {
		if strings.HasPrefix(m.Text, "mf ") {
			texts = append(texts, m.Text)
		}
	}
	if len(texts) != 3 || texts[2] != "mf 2" {
		t.Fatalf("order broken: %v", texts)
	}
}

// W8: one failed fetch only skips a tick; a second consecutive miss declares
// the side down and logs exactly once, and further misses stay silent until a
// successful fetch flips it back.
func TestBridgeMissDeclaresDownAfterTwo(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	r.b.mu.Lock()
	r.b.sideUp[1] = true
	r.b.sideLogged[1] = true // a previous tick saw the side up
	r.b.mu.Unlock()
	downs := func() int {
		n := 0
		for _, line := range r.logs.all() {
			if strings.Contains(line, "is down") {
				n++
			}
		}
		return n
	}
	r.b.miss(1)
	r.b.mu.Lock()
	up := r.b.sideUp[1]
	r.b.mu.Unlock()
	if !up || downs() != 0 {
		t.Fatalf("one miss declared the side down (up=%v, %d down lines)", up, downs())
	}
	r.b.miss(1)
	r.b.mu.Lock()
	up = r.b.sideUp[1]
	r.b.mu.Unlock()
	if up || downs() != 1 {
		t.Fatalf("second miss should declare down once (up=%v, %d down lines)", up, downs())
	}
	r.b.miss(1)
	if downs() != 1 {
		t.Fatalf("a third miss logged again: %d down lines", downs())
	}
}

func m1id(t *testing.T, s *bsession, text string) string {
	t.Helper()
	m := s.waitMsg(func(m *broker.Message) bool { return m.Text == text }, "message "+text)
	return m.ID
}

// Addition a: a far socket that does not exist is reported, not auto-started,
// and the near side keeps working.
func TestBridgeFarDownNoAutoStart(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agmbridge")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	local := filepath.Join(dir, "l.sock")
	brokerAt(t, local, broker.DefaultLimits())
	alice := dialSession(t, local, "alice-1", "alice", "pi", true)

	missing := filepath.Join(dir, "missing", "far.sock")
	logs := &lockedStrings{}
	b := NewBridge(local, missing, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("the far socket path was created: %v", err)
	}
	// The near side keeps working: list still answers, alice is undisturbed.
	if findSession(listOn(t, local), func(s broker.SessionInfo) bool { return s.ID == "alice-1" }) == nil {
		t.Fatal("near side disturbed while the far side was down")
	}
	_ = alice
}

// sendReq sends and returns the created message or the broker error.
func sendReq(s *bsession, r broker.SendReq) (*broker.Message, error) {
	var m broker.Message
	err := s.call("send", broker.Request{SendReq: r}, &m)
	return &m, err
}

// mirrored waits until the ids appear in each daemon's list (both sides of a
// restarted bridge are up before the test continues).
func mirrored(t *testing.T, sock, id string) {
	t.Helper()
	waitFor(t, 3*time.Second, func() bool {
		return findSession(listOn(t, sock), func(s broker.SessionInfo) bool { return s.ID == id }) != nil
	}, "proxy "+id+" on "+sock)
}

// T1 stability helper.
func idsWith(list []broker.SessionInfo, prefix string) []string {
	var out []string
	for _, s := range list {
		if strings.HasPrefix(s.ID, prefix) {
			out = append(out, s.ID)
		}
	}
	return out
}

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}
