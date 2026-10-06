package main

// Review probes from the FREEZE-3c.1 and 3c.1b reviews (silent-raven), kept
// as regression tests. P5 was adapted to the fixed id-map API (record()) and
// loops the burst; P8's tick bound is the relaxed 15 s; P13 is scaled down;
// P12-P19 come from the 3c.1b review (lagForward mimics ssh -L: per-chunk
// latency, bandwidth limit, and delivery after close).

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func probeWait(d time.Duration, cond func() bool) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

func probeHas(s *bsession, text string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.msgs {
		if strings.Contains(m.Text, text) {
			return true
		}
	}
	return false
}

func probeInbox(t *testing.T, sock, id string) []string {
	c, err := dialBConn(sock, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	info := broker.SessionInfo{ID: id}
	if err := c.callDL(t.Context(), broker.Request{Op: "hello", Session: &info}, nil, fixedDL(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	var msgs []*broker.Message
	if err := c.callDL(t.Context(), broker.Request{Op: "inbox"}, &msgs, fixedDL(74*time.Second)); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range msgs {
		out = append(out, m.Text)
	}
	return out
}

// P1: the bridge's connections to side 1 die (tunnel drop with a quick
// reconnect, or `agm restart` on the server) while both daemons stay up.
func TestBridgeProbeP1ReconnectAfterConnLoss(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	for _, p := range r.b.proxies[0] { // proxies of side-0 sessions live on side 1
		p.mu.Lock()
		p.conn.close()
		p.mu.Unlock()
	}
	r.b.ctrl[1].close()
	for i := 0; i < 3; i++ {
		r.b.tickOnce()
		time.Sleep(60 * time.Millisecond)
	}
	px := findSession(listOn(t, r.socks[1]), func(s broker.SessionInfo) bool { return s.ID == "laptop/alice" })
	t.Logf("P1a laptop/alice on side 1 after 3 ticks: present=%v live=%v", px != nil, px != nil && px.Live)
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p1b bob to alice"})
	alice.send(broker.SendReq{To: "srv/bob", Text: "p1c alice to bob"})
	for i := 0; i < 3; i++ {
		r.b.tickOnce()
	}
	gotA := probeWait(3*time.Second, func() bool { return probeHas(alice, "p1b bob to alice") })
	gotB := probeWait(3*time.Second, func() bool { return probeHas(bob, "p1c alice to bob") })
	t.Logf("P1b side1->side0 delivered=%v  P1c side0->side1 delivered=%v", gotA, gotB)
	if px == nil || !px.Live || !gotA || !gotB {
		t.Errorf("bridge did not recover its side-1 proxy connections: live=%v b->a=%v a->b=%v", px != nil && px.Live, gotA, gotB)
	}
}

// P2: mail to an offline proxy must reach the real (offline) session's mailbox.
func TestBridgeProbeP2OfflineProxyDrain(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	alice.conn.close() // alice goes offline but stays registered (Detach)
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce() // the proxy follows: offline
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p2 for offline alice"})
	for i := 0; i < 4; i++ {
		r.b.tickOnce()
		time.Sleep(60 * time.Millisecond)
	}
	got := probeInbox(t, r.socks[0], "alice")
	t.Logf("P2 alice's real mailbox on side 0: %q", got)
	if len(got) == 0 {
		t.Errorf("mail to an offline proxy was never relayed to the real session")
	}
}

// P3: a new session's first message, sent before the next tick mirrors it.
func TestBridgeProbeP3FirstMessageOfNewSession(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce() // relays started
	carol := dialSession(t, r.socks[0], "carol", "carol", "pi", true)
	carol.send(broker.SendReq{To: "srv/bob", Text: "p3 hello from a fresh session"})
	bounced := probeWait(2*time.Second, func() bool { return probeHas(carol, "not delivered") })
	delivered := probeHas(bob, "p3 hello from a fresh session")
	t.Logf("P3 bounced=%v delivered=%v", bounced, delivered)
	if bounced || !probeWait(2*time.Second, func() bool { r.b.tickOnce(); return probeHas(bob, "p3 hello") }) {
		t.Errorf("a fresh session's first message bounced (bounced=%v) or never arrived", bounced)
	}
}

// P4: run with -race. Sessions churn on side 0 (the tick writes b.proxies)
// while mirrored senders relay (workers read b.proxies in senderConn).
func TestBridgeProbeP4ProxyMapRace(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.SendPerMin, lim.SendBurst = 100000, 10000
	r := newBridgeRig(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.tickN(4)
	for i := 0; i < 40; i++ {
		dialSession(t, r.socks[0], fmt.Sprintf("churn%02d", i), fmt.Sprintf("churn%02d", i), "pi", true)
		alice.send(broker.SendReq{To: "srv/bob", Text: fmt.Sprintf("p4 #%d", i)})
		time.Sleep(15 * time.Millisecond)
	}
	if !probeWait(5*time.Second, func() bool { return probeHas(bob, "p4 #39") }) {
		t.Errorf("relay stalled")
	}
}

// P5: concurrent persists of the id map must not lose pairs on disk.
func TestBridgeProbeP5ConcurrentPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "link.map")
	m := loadBridgeMap(path, nil)
	var wg sync.WaitGroup
	const rounds = 10 // one burst catches an unlocked persist only sometimes
	for round := 0; round < rounds; round++ {
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// the production path: record() snapshots and persists under
				// the persist lock (adapted from the 3c.1 probe, which
				// reproduced the old unlocked snapshot+persist interleaving)
				m.record([2]string{fmt.Sprintf("a%02d%02d", round, i), fmt.Sprintf("b%02d%02d", round, i)})
			}()
		}
		wg.Wait()
	}
	data, err := os.ReadFile(path)
	var pairs [][2]string
	perr := json.Unmarshal(data, &pairs)
	t.Logf("P5 on disk: read err=%v parse err=%v pairs=%d of %d", err, perr, len(pairs), 64*rounds)
	if err != nil || perr != nil || len(pairs) != 64*rounds {
		t.Errorf("persisted map lost pairs or is corrupt: %d of %d (parse err %v)", len(pairs), 64*rounds, perr)
	}
}

// P6: a gate session (an id with "/" that is not a bridge proxy) asks a bridged
// session. Today: logged and acked, no bounce, so a blocking ask waits for its timeout.
func TestBridgeProbeP6GateSenderGetsNoAnswer(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	gate := dialSession(t, r.socks[0], "srv/openclaw", "srv/openclaw", "openclaw", true)
	r.b.tickOnce()
	gate.send(broker.SendReq{To: "srv/bob", Text: "p6 question from a gate session", ExpectsReply: true, NoWait: true})
	answered := probeWait(2*time.Second, func() bool { return probeHas(gate, "not delivered") })
	t.Logf("P6 gate sender told=%v; logs: %q", answered, r.logs.all())
	if !answered {
		t.Errorf("the gate session's question was dropped without any answer")
	}
}

// P7: workers of proxies whose real session left must exit.
func TestBridgeProbeP7WorkerLeak(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()
	var ss []*bsession
	for i := 0; i < 30; i++ {
		ss = append(ss, dialSession(t, r.socks[0], fmt.Sprintf("tmp%02d", i), fmt.Sprintf("tmp%02d", i), "pi", true))
	}
	r.b.tickOnce() // 30 proxies, 30 workers
	for _, s := range ss {
		s.call("bye", broker.Request{}, nil)
		s.conn.close()
	}
	time.Sleep(100 * time.Millisecond)
	r.b.tickOnce() // all 30 gone: byed
	r.b.tickOnce()
	time.Sleep(300 * time.Millisecond)
	after := runtime.NumGoroutine()
	r.b.mu.Lock()
	proxiesLeft := len(r.b.proxies[0]) // read under the bridge lock: removals race this line
	r.b.mu.Unlock()
	t.Logf("P7 goroutines before=%d after 30 sessions came and went=%d (proxies left: %d)", before, after, proxiesLeft)
	if after-before >= 30 {
		t.Errorf("relay workers of byed proxies did not exit: +%d goroutines", after-before)
	}
}

// P8: a far daemon that accepts but never answers (wedged daemon, half-open tunnel).
func TestBridgeProbeP8WedgedFarBlocksTick(t *testing.T) {
	dir, _ := os.MkdirTemp("/tmp", "agmzz")
	t.Cleanup(func() { os.RemoveAll(dir) })
	local := filepath.Join(dir, "l.sock")
	brokerAt(t, local, broker.DefaultLimits())
	dialSession(t, local, "alice", "alice", "pi", true)
	far := filepath.Join(dir, "far.sock")
	ln, err := net.Listen("unix", far)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var held []net.Conn // keep accepted conns referenced: never answered, never closed
	var hmu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			hmu.Lock()
			held = append(held, c)
			hmu.Unlock()
		}
	}()
	b := NewBridge(local, far, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	done := make(chan struct{})
	go func() { b.tickOnce(); close(done) }()
	select {
	case <-done:
		t.Logf("P8 tick returned")
	case <-time.After(15 * time.Second):
		t.Errorf("P8 one tick against a far daemon that never answers is still blocked after 15 s")
	}
}

// P9: an offline proxy whose registered connection the bridge holds is swept at
// MailTTL (LastSeen is not refreshed by a held connection), and never recreated.
func TestBridgeProbeP9HeldOfflineProxySwept(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailTTL = 300 * time.Millisecond
	r := newBridgeRigManual(t, lim)
	hello := func() { // alice: offline but active (hello without subscribe, then gone)
		s := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
		s.conn.close()
	}
	hello()
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	for i := 0; i < 4; i++ {
		time.Sleep(150 * time.Millisecond)
		hello()
		r.brokers[0].Sweep()
		r.brokers[1].Sweep()
		r.b.tickOnce()
	}
	real := findSession(listOn(t, r.socks[0]), func(s broker.SessionInfo) bool { return s.ID == "alice" })
	px := findSession(listOn(t, r.socks[1]), func(s broker.SessionInfo) bool { return s.ID == "laptop/alice" })
	_, err := sendReq(bob, broker.SendReq{To: "laptop/alice", Text: "p9"})
	t.Logf("P9 real alice present=%v; proxy laptop/alice present=%v; bob -> laptop/alice: err=%v", real != nil, px != nil, err)
	if real != nil && px == nil {
		t.Errorf("the held offline proxy was swept and not recreated while its real session is present")
	}
}

// P10: log volume while the far side is down, with 5 local sessions.
func TestBridgeProbeP10LogsWhileFarDown(t *testing.T) {
	dir, _ := os.MkdirTemp("/tmp", "agmzz")
	t.Cleanup(func() { os.RemoveAll(dir) })
	local := filepath.Join(dir, "l.sock")
	brokerAt(t, local, broker.DefaultLimits())
	for i := 0; i < 5; i++ {
		dialSession(t, local, fmt.Sprintf("s%d", i), fmt.Sprintf("s%d", i), "pi", true)
	}
	logs := &lockedStrings{}
	b := NewBridge(local, filepath.Join(dir, "missing.sock"), "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	for i := 0; i < 10; i++ {
		b.tickOnce()
	}
	all := logs.all()
	first := ""
	if len(all) > 0 {
		first = all[0]
	}
	t.Logf("P10 10 ticks, far side down, 5 local sessions: %d log lines; first: %q", len(all), first)
	if len(all) > 3 {
		t.Errorf("log lines grow per tick and per session while the far side is down: %d", len(all))
	}
}

// P11 (the test proposed for mutation M3): a relayed ask must be no_wait on the
// target daemon, so a blocking ask back from the target is not refused as a deadlock.
func TestBridgeProbeP11RelayedAskNoDeadlock(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	r.b.tickOnce()
	alice.send(broker.SendReq{To: "srv/bob", Text: "p11 question", ExpectsReply: true, NoWait: true})
	bob.waitMsg(func(m *broker.Message) bool { return m.Text == "p11 question" }, "the relayed ask")
	_, err := sendReq(bob, broker.SendReq{To: "laptop/alice", Text: "p11 counter-question", ExpectsReply: true})
	t.Logf("P11 bob's blocking ask back to laptop/alice: err=%v", err)
	if err != nil {
		t.Errorf("a blocking ask back across the bridge was refused: %v", err)
	}
}

// lagForward listens on path and forwards each connection to target, adding
// oneWay latency to every chunk in both directions and limiting the
// client-to-daemon direction to bps bytes per second (0: unlimited). Like
// ssh -L, data it already accepted is still delivered after the client closes.
func lagForward(t *testing.T, path, target string, oneWay time.Duration, bps int) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			d, err := net.Dial("unix", target)
			if err != nil {
				c.Close()
				continue
			}
			go lagPipe(c, d, oneWay, bps)
			go lagPipe(d, c, oneWay, 0)
		}
	}()
}

func lagPipe(src, dst net.Conn, oneWay time.Duration, bps int) {
	type chunk struct {
		at time.Time
		b  []byte
	}
	ch := make(chan chunk, 1<<16)
	go func() {
		defer close(ch)
		buf := make([]byte, 16<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				ch <- chunk{time.Now().Add(oneWay), append([]byte(nil), buf[:n]...)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range ch {
		if d := time.Until(c.at); d > 0 {
			time.Sleep(d)
		}
		if bps > 0 {
			time.Sleep(time.Duration(len(c.b)) * time.Second / time.Duration(bps))
		}
		if _, err := dst.Write(c.b); err != nil {
			break
		}
	}
	if uc, ok := dst.(*net.UnixConn); ok {
		uc.CloseWrite()
	} else {
		dst.Close()
	}
}

func probeDir(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "agmzz")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func probeHasID(list []broker.SessionInfo, id string) bool {
	return findSession(list, func(s broker.SessionInfo) bool { return s.ID == id }) != nil
}

// P12: a new session's first message over a tunnel with ~80 ms RTT. (Kept
// from the FREEZE-3c.1b review; P13 is scaled down as suggested there.)
func TestBridgeProbeP12FirstMessageOverLaggyTunnel(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 40*time.Millisecond, 0)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool { return probeHasID(listOn(t, l), "srv/bob") }, "srv/bob mirrored")
	bounced, delivered := 0, 0
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("new%d", i)
		ns := dialSession(t, l, id, id, "pi", true)
		ns.send(broker.SendReq{To: "srv/bob", Text: "p12 " + id})
		probeWait(5*time.Second, func() bool { return probeHas(ns, "not delivered") || probeHas(bob, "p12 "+id) })
		if probeHas(ns, "not delivered") {
			bounced++
		} else if probeHas(bob, "p12 "+id) {
			delivered++
		}
	}
	t.Logf("P12 over an ~80 ms RTT tunnel: %d of 8 first messages bounced as unmirrored, %d delivered", bounced, delivered)
	if bounced > 0 {
		t.Errorf("first messages still bounce over a tunnel with latency")
	}
}

// P13: a 900 KiB message over a 64 KiB/s laptop-to-server link (about 14 s).
func TestBridgeProbeP13LargeMessageSlowLink(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 20*time.Millisecond, 24<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	b.baseDL = 2 * time.Second // the ~4 s transfer exceeds the base: only the
	// per-16-KiB scaling keeps the deadline ahead of the frame in transit (B3)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool { return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice") }, "mirrored")
	// scaled down from the review's 900 KiB / 64 KiB/s (same shape, seconds
	// not minutes): 96 KiB over 24 KiB/s is a ~4 s transfer against a 16 s
	// scaled deadline
	big := strings.Repeat("x", 96<<10)
	alice.send(broker.SendReq{To: "srv/bob", Text: big})
	time.Sleep(14 * time.Second) // 14 s: a mutation's re-sent copy lands past 8 s
	n := 0
	bob.mu.Lock()
	for _, m := range bob.msgs {
		if len(m.Text) == len(big) {
			n++
		}
	}
	bob.mu.Unlock()
	timeouts := 0
	for _, line := range logs.all() {
		if strings.Contains(line, "no answer within") {
			timeouts++
		}
	}
	t.Logf("P13 96 KiB over 24 KiB/s: bob received it %d times in 14 s; %d timeout log lines; log tail: %q", n, timeouts, probeTail(logs.all(), 3))
	if n != 1 {
		t.Errorf("a large message over a slow link was delivered %d times, want exactly 1", n)
	}
}

func probeTail(v []string, n int) []string {
	if len(v) > n {
		return v[len(v)-n:]
	}
	return v
}

// P14: a foreign subscriber on a proxy the bridge wants offline.
func TestBridgeProbeP14ForeignSubscriberLeak(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a.conn.close() // alice: registered, offline
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	r.b.tickOnce()
	dialSession(t, r.socks[1], "laptop/alice", "laptop/alice", "pi", true) // the foreign subscriber
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		r.b.tickOnce()
	}
	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()
	foreign := 0
	for _, line := range r.logs.all() {
		if strings.Contains(line, "subscribed by someone else") {
			foreign++
		}
	}
	t.Logf("P14 foreign subscriber on laptop/alice: goroutines %d -> %d over 20 ticks; %d foreign-line(s)", before, after, foreign)
	if after-before >= 10 {
		t.Errorf("each tick reopens the proxy and leaks the replaced connection: +%d goroutines in 20 ticks", after-before)
	}
	if foreign != 1 {
		t.Errorf("the foreign-subscriber line fired %d times, want exactly 1", foreign)
	}
}

// P15: the cwd cap must cut on a rune boundary.
func TestBridgeProbeP15CwdCapRuneBoundary(t *testing.T) {
	b := NewBridge("x", "y", "laptop/", "srv/", filepath.Join(t.TempDir(), "m.map"), nil)
	p := &bproxy{b: b, home: 0, realID: "alice", info: broker.SessionInfo{Name: "alice", Cwd: strings.Repeat("a", cwdCap-1) + "ğ"}}
	got := b.proxyInfo(p).Cwd
	t.Logf("P15 capped cwd: %d bytes, valid UTF-8 = %v, last bytes %q", len(got), utf8.ValidString(got), got[len(got)-2:])
	if len(got) > cwdCap {
		t.Errorf("cwd not capped: %d bytes", len(got))
	}
	if !utf8.ValidString(got) {
		t.Errorf("the cwd cap split a multi-byte rune")
	}
}

// P16: the offline drain re-queues every unacked message on every tick while
// the relay of the first one is stuck (the real mailbox is full).
func TestBridgeProbeP16DrainRequeuesWhileStuck(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	carol := dialSession(t, r.socks[0], "carol", "carol", "pi", true)
	carol.send(broker.SendReq{To: "alice", Text: "fill 1"})
	carol.send(broker.SendReq{To: "alice", Text: "fill 2"}) // alice's real mailbox is full
	a.conn.close()
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p16 one"})
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p16 two"})
	for i := 0; i < 10; i++ {
		r.b.tickOnce()
		time.Sleep(20 * time.Millisecond)
	}
	r.b.mu.Lock()
	p := r.b.proxies[0]["alice"]
	r.b.mu.Unlock()
	q := len(p.queue)
	t.Logf("P16 after 10 ticks with 2 unacked messages and the relay stuck on mailbox_full: %d queued copies", q)
	if q > 4 {
		t.Errorf("the drain piles up duplicates while the worker is busy: %d queued", q)
	}
}

// P17: a real far-daemon restart (Serve stops, its connections and in-memory
// state are gone; a fresh daemon starts on the same path).
func TestBridgeProbeP17RealFarRestart(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	lim := broker.DefaultLimits()
	brokerAt(t, l, lim)
	_, cancel1, _ := brokerAtS(t, s, lim)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.tick = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool { return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice") }, "mirrored")
	alice.send(broker.SendReq{To: "srv/bob", Text: "before"})
	bob.waitMsg(func(m *broker.Message) bool { return m.Text == "before" }, "before")

	cancel1() // the far daemon stops for real
	waitFor(t, 3*time.Second, func() bool {
		c, err := net.Dial("unix", s)
		if err == nil {
			c.Close()
		}
		return err != nil
	}, "far daemon stopped")
	alice.send(broker.SendReq{To: "srv/bob", Text: "during"})
	time.Sleep(300 * time.Millisecond)
	serveFresh(t, s, lim) // a fresh daemon on the same path, no state (a new process takes the lock; in-process we bypass it)
	bob = dialSession(t, s, "bob", "bob", "pi", true)
	start := time.Now()
	alice.send(broker.SendReq{To: "srv/bob", Text: "after"})
	got := bob.waitMsg(func(m *broker.Message) bool { return m.Text == "after" }, "after the restart")
	bob.send(broker.SendReq{To: got.From, Text: "back", ReplyTo: got.ID})
	alice.waitMsg(func(m *broker.Message) bool { return m.Text == "back" }, "the reverse direction")
	during := 0
	bob.mu.Lock()
	for _, m := range bob.msgs {
		if m.Text == "during" {
			during++
		}
	}
	bob.mu.Unlock()
	t.Logf("P17 real far restart: both directions back after %v; 'during' delivered %d time(s)", time.Since(start).Round(10*time.Millisecond), during)
	if during != 1 {
		t.Errorf("the message sent while the far daemon was down arrived %d times, want 1", during)
	}
}

// serveFresh serves a fresh, stateless broker on path without broker.Listen's
// process-lifetime lock (in-process stand-in for a restarted daemon process).
func serveFresh(t *testing.T, path string, lim broker.Limits) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := broker.New(lim, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
}

// P18 (run with -race): proxyInfo reads p.info without p.mu. The worker reaches it
// through ackOn's not_registered fallback (rehello), while the tick writes p.info
// under p.mu when the real session's fields change. This drives both paths at once.
func TestBridgeProbeP18ProxyInfoRace(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	r.b.mu.Lock()
	p := r.b.proxies[0]["alice"]
	r.b.mu.Unlock()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // what ackOn's fallback does on the worker goroutine
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				p.rehello()
			}
		}
	}()
	for i := 0; i < 20; i++ {
		c := dialSession(t, r.socks[0], "alice", fmt.Sprintf("alice%d", i), "pi", false) // a rename
		c.conn.close()
		r.b.tickOnce()
	}
	close(stop)
	<-done
}

// P19: a high-latency link (~1.1 s RTT: in-flight Wi-Fi, a loaded mobile link).
func TestBridgeProbeP19HighLatencyLink(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 550*time.Millisecond, 0)
	dialSession(t, s, "bob", "bob", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	mirrored := probeWait(15*time.Second, func() bool { return probeHasID(listOn(t, l), "srv/bob") })
	all := strings.Join(logs.all(), "\n")
	upL, upFar := strings.Contains(all, l+" is up"), strings.Contains(all, lag+" is up")
	t.Logf("P19 ~1.1 s RTT: srv/bob mirrored within 15 s = %v; initial states logged: near=%v far=%v; log: %q", mirrored, upL, upFar, probeTail(logs.all(), 4))
	if !mirrored {
		t.Errorf("the bridge never comes up over a link with RTT above the 1 s ping deadline")
	}
	if !upL || !upFar {
		t.Errorf("initial side state not logged: near=%v far=%v", upL, upFar)
	}
}

// P20-P22 from the FREEZE-3c.1c review (silent-raven), kept as regression
// tests. P21b is the production-budget attachment case (40 s).

// P20: a session leaves while its offline proxy holds a message from a sender
// that is not mirrored yet. byeProxy's last drain queues it, and the hold needs
// the next tick's apply while byeProxy blocks the tick waiting for the drain.
func TestBridgeProbeP20ByeWithHeldMessage(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a.conn.close() // alice: registered, offline
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()                                                    // laptop/alice (offline) on side 1
	carol := dialSession(t, r.socks[1], "carol", "carol", "pi", true) // new: not mirrored yet
	carol.send(broker.SendReq{To: "laptop/alice", Text: "p20 for alice", ExpectsReply: true, NoWait: true})
	a2 := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a2.call("bye", broker.Request{}, nil) // alice leaves for good
	a2.conn.close()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	r.b.tickOnce() // alice gone: byeProxy's last drain queues carol's message
	first := time.Since(start)
	r.b.tickOnce() // carol is mirrored and applied by now
	told := probeWait(6*time.Second, func() bool { return probeHas(carol, "not delivered") })
	t.Logf("P20 the tick with the bye took %v; carol told within 6 s after the next tick: %v; log tail: %q",
		first.Round(100*time.Millisecond), told, probeTail(r.logs.all(), 2))
	if first > 5*time.Second {
		t.Errorf("byeProxy blocked the tick for %v", first.Round(100*time.Millisecond))
	}
	if !told {
		t.Errorf("the held message was neither delivered nor bounced after the bye")
	}
}

// P21: the relay deadline scales with the text only. The same 96 KiB payload,
// once as text and once as a file attachment, over 24 KiB/s (about 4 s), with
// baseDL shrunk to 2 s through the Bridge field.
func TestBridgeProbeP21AttachmentDeadline(t *testing.T) {
	for _, inText := range []bool{true, false} {
		name := map[bool]string{true: "in text", false: "in a file attachment"}[inText]
		t.Run(strings.ReplaceAll(name, " ", "_"), func(t *testing.T) {
			dir := probeDir(t)
			l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
			brokerAt(t, l, broker.DefaultLimits())
			brokerAt(t, s, broker.DefaultLimits())
			lagForward(t, lag, s, 20*time.Millisecond, 24<<10)
			alice := dialSession(t, l, "alice", "alice", "pi", true)
			bob := dialSession(t, s, "bob", "bob", "pi", true)
			b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
			b.tick = 500 * time.Millisecond
			b.baseDL = 2 * time.Second
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			go b.Run(ctx)
			waitFor(t, 5*time.Second, func() bool {
				return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice")
			}, "mirrored")
			payload := strings.Repeat("x", 96<<10)
			req := broker.SendReq{To: "srv/bob", Text: payload}
			if !inText {
				req = broker.SendReq{To: "srv/bob", Text: "see attached",
					Attachments: []broker.Attachment{{Type: "file", Name: "big.txt", Content: payload}}}
			}
			alice.send(req)
			time.Sleep(14 * time.Second)
			n := 0
			bob.mu.Lock()
			for _, m := range bob.msgs {
				if m.Text == req.Text {
					n++
				}
			}
			bob.mu.Unlock()
			t.Logf("P21 96 KiB %s over 24 KiB/s (baseDL 2 s): delivered %d time(s) in 14 s", name, n)
			if n != 1 {
				t.Errorf("delivered %d times, want 1", n)
			}
		})
	}
}

// countText counts the messages a session holds whose text contains sub.
func countText(c *bsession, sub string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.msgs {
		if strings.Contains(m.Text, sub) {
			n++
		}
	}
	return n
}

// P20b: as P20, but the sender is already mirrored (the common case): the
// relay to the gone session fails at once and the bounce goes out on the
// connection byeProxy has just closed.
func TestBridgeProbeP20bByeMirroredSender(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a.conn.close() // alice: registered, offline
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce() // laptop/alice (offline) on side 1, srv/bob on side 0
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p20b for alice", ExpectsReply: true, NoWait: true})
	a2 := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a2.call("bye", broker.Request{}, nil)
	a2.conn.close()
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce() // alice gone: byeProxy's last drain queues bob's message
	r.b.tickOnce()
	told := probeWait(6*time.Second, func() bool { return countText(bob, "not delivered") > 0 })
	t.Logf("P20b mirrored sender bob told within 6 s: %v", told)
	if !told {
		t.Errorf("bob's message to a session that left was neither delivered nor bounced")
	}
}

// P20c: two messages from a not-yet-mirrored sender. The first goes straight
// to the worker, which holds it for the next tick's apply; the second sits in
// the queue, so byeProxy waits - on the tick goroutine, the one that would
// apply the list the hold needs.
func TestBridgeProbeP20cByeStallsTick(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a.conn.close()
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	carol := dialSession(t, r.socks[1], "carol", "carol", "pi", true) // new: not mirrored yet
	carol.send(broker.SendReq{To: "laptop/alice", Text: "p20c one", ExpectsReply: true, NoWait: true})
	carol.send(broker.SendReq{To: "laptop/alice", Text: "p20c two", ExpectsReply: true, NoWait: true})
	a2 := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a2.call("bye", broker.Request{}, nil)
	a2.conn.close()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	r.b.tickOnce()
	first := time.Since(start)
	r.b.tickOnce()
	probeWait(6*time.Second, func() bool { return countText(carol, "not delivered") == 2 })
	t.Logf("P20c the bye tick took %v; carol told %d of 2; log tail: %q",
		first.Round(100*time.Millisecond), countText(carol, "not delivered"), probeTail(r.logs.all(), 2))
	if first > 5*time.Second {
		t.Errorf("byeProxy blocked the tick for %v", first.Round(100*time.Millisecond))
	}
	if n := countText(carol, "not delivered"); n != 2 {
		t.Errorf("carol told %d of 2", n)
	}
}

// P22: a relay in transit on the sender proxy's connection when the sender
// goes offline. The transition's openConn closes the replaced connection
// under the relay; the tunnel still delivers the frame it accepted, and the
// retry on the new connection sends it again.
func TestBridgeProbeP22TransitionCutsRelay(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 20*time.Millisecond, 24<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	logs := &lockedStrings{}
	start := time.Now()
	stamp := func() string { return fmt.Sprintf("[%6.1fs]", float64(time.Since(start).Milliseconds())/1e3) }
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) {
		logs.add(stamp() + " " + fmt.Sprintf(f, a...))
	})
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice")
	}, "mirrored")
	big := strings.Repeat("y", 96<<10) // ~4 s at 24 KiB/s, inside the scaled budget
	alice.send(broker.SendReq{To: "srv/bob", Text: big})
	time.Sleep(time.Second) // the relay is in transit
	logs.add(stamp() + " ALICE CLOSES")
	alice.conn.close() // alice goes offline: the next tick transitions laptop/alice
	time.Sleep(14 * time.Second)
	n := countText(bob, big)
	foreign := 0
	for _, line := range logs.all() {
		if strings.Contains(line, "subscribed by someone else") {
			foreign++
		}
	}
	t.Logf("P22 96 KiB relay in transit when its sender went offline: delivered %d time(s); %d false foreign line(s); log: %q", n, foreign, probeTail(logs.all(), 3))
	if n != 1 {
		t.Errorf("delivered %d times, want 1", n)
	}
	if foreign != 0 {
		t.Errorf("the draining retired subscription was misread as a foreign subscriber %d times", foreign)
	}
}

// P21b: P21's attachment case at production budgets (default baseDL), the
// full-size P13 shape: 900 KiB in a file attachment over 64 KiB/s.
func TestBridgeProbeP21bAttachmentProduction(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 20*time.Millisecond, 64<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice")
	}, "mirrored")
	alice.send(broker.SendReq{To: "srv/bob", Text: "p21b see attached",
		Attachments: []broker.Attachment{{Type: "file", Name: "big.txt", Content: strings.Repeat("x", 900<<10)}}})
	time.Sleep(40 * time.Second)
	n := countText(bob, "p21b see attached")
	t.Logf("P21b 900 KiB file attachment over 64 KiB/s, default budgets: delivered %d time(s) in 40 s", n)
	if n != 1 {
		t.Errorf("delivered %d times, want 1", n)
	}
}

// The A7 regression test proposed in the review: proxyInfo against a locked
// writer, no network, so -race sees the interleaving every run.
func TestProxyInfoSnapshotUnderLock(t *testing.T) {
	b := NewBridge("x", "y", "laptop/", "srv/", filepath.Join(t.TempDir(), "m.map"), nil)
	p := &bproxy{b: b, home: 0, realID: "alice", info: broker.SessionInfo{Name: "alice"}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			p.mu.Lock()
			p.info = broker.SessionInfo{Name: fmt.Sprintf("alice%d", i), Cwd: "/w"}
			p.mu.Unlock()
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
			_ = b.proxyInfo(p)
		}
	}
}

// P23-P27 and P25b from the FREEZE-3c.1d review (silent-raven), kept as
// regression tests, with the cutForward helper (a tunnel that drops and
// comes back while the far daemon keeps its state).

// cutForward is a tunnel that can drop and come back while the far daemon
// keeps running with its state (the laptop sleeps, the ssh link drops).
type cutTunnel struct {
	mu    sync.Mutex
	down  bool
	conns []net.Conn
}

func cutForward(t *testing.T, path, target string) *cutTunnel {
	ct := &cutTunnel{}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			ct.mu.Lock()
			down := ct.down
			ct.mu.Unlock()
			if down {
				c.Close()
				continue
			}
			d, err := net.Dial("unix", target)
			if err != nil {
				c.Close()
				continue
			}
			ct.mu.Lock()
			ct.conns = append(ct.conns, c, d)
			ct.mu.Unlock()
			go func() {
				buf := make([]byte, 64<<10)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						d.Write(buf[:n])
					}
					if err != nil {
						d.Close()
						return
					}
				}
			}()
			go func() {
				buf := make([]byte, 64<<10)
				for {
					n, err := d.Read(buf)
					if n > 0 {
						c.Write(buf[:n])
					}
					if err != nil {
						c.Close()
						return
					}
				}
			}()
		}
	}()
	return ct
}

func (ct *cutTunnel) cut() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.down = true
	for _, c := range ct.conns {
		c.Close()
	}
	ct.conns = nil
}

func (ct *cutTunnel) restore() {
	ct.mu.Lock()
	ct.down = false
	ct.mu.Unlock()
}

// P23: a session leaves and comes back while its proxy is still finishing
// mail (a bounce stuck on mailbox_full). gone is never reset, so the worker
// byes and removes the proxy of a session that is back.
func TestBridgeProbeP23ReturnBeforeBye(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 1
	r := newBridgeRigManual(t, lim)
	a := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a.conn.close() // alice: registered, offline
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	fill := dave.send(broker.SendReq{To: "bob", Text: "fill"})     // bob's mailbox is full
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p23 note"}) // a plain send: its bounce is no reply, so the mailbox cap applies
	a2 := dialSession(t, r.socks[0], "alice", "alice", "pi", false)
	a2.call("bye", broker.Request{}, nil) // alice leaves
	a2.conn.close()
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce() // gone: the worker drains, the relay bounces, the bounce sticks on mailbox_full
	time.Sleep(200 * time.Millisecond)
	a3 := dialSession(t, r.socks[0], "alice", "alice", "pi", false) // alice is back
	a3.conn.close()
	r.b.tickOnce()                                               // alice is wanted again
	bob.call("ack", broker.Request{IDs: []string{fill.ID}}, nil) // the bounce can land now
	time.Sleep(3 * time.Second)
	r.b.mu.Lock()
	_, held := r.b.proxies[0]["alice"]
	r.b.mu.Unlock()
	row := probeHasID(listOn(t, r.socks[1]), "laptop/alice")
	sendErr := bob.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p23 after return"}}, nil)
	t.Logf("P23 alice back before the bye: proxy held=%v, far row present=%v, bob's next send: %v; log tail: %q",
		held, row, sendErr, probeTail(r.logs.all(), 3))
	if !held || !row || sendErr != nil {
		t.Errorf("the proxy of a session that came back was byed and removed")
	}
}

// P24: a session leaves while the tunnel is down; mail for it sits in the far
// row. When the tunnel returns, its sender must be told.
func TestBridgeProbeP24LeaveWhileTunnelDown(t *testing.T) {
	dir := probeDir(t)
	l, s, cutp := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "cut.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	tun := cutForward(t, cutp, s)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, cutp, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		return probeHasID(listOn(t, l), "srv/bob") && probeHasID(listOn(t, s), "laptop/alice")
	}, "mirrored")
	tun.cut()
	time.Sleep(500 * time.Millisecond) // the far side is declared down
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p24 for alice", ExpectsReply: true, NoWait: true})
	alice.call("bye", broker.Request{}, nil) // alice leaves during the outage
	time.Sleep(time.Second)
	tun.restore()
	told := probeWait(8*time.Second, func() bool { return countText(bob, "not delivered") > 0 })
	t.Logf("P24 alice left while the tunnel was down: bob told within 8 s of the tunnel's return: %v; far row still holds: %q; log: %q",
		told, probeInbox(t, s, "laptop/alice"), probeTail(logs.all(), 4))
	if !told {
		t.Errorf("mail for a session that left during an outage is stranded in the far row (MailTTL 24 h), its sender never told")
	}
}

// P25: a small relay queued behind a big frame on the same sender connection
// (alice sends a big file to bob, then a note to carol). The small call's
// budget runs out while the big frame is still in transit; its timeout closes
// the shared connection under the big relay. Scaled: baseDL 2 s, 96 KiB over
// 24 KiB/s (production: 10 s against 900 KiB at 64 KiB/s).
func TestBridgeProbeP25SmallCallBehindBigFrame(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 20*time.Millisecond, 24<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	carol := dialSession(t, s, "carol", "carol", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	b.baseDL = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		l1, l2 := listOn(t, l), listOn(t, s)
		return probeHasID(l1, "srv/bob") && probeHasID(l1, "srv/carol") && probeHasID(l2, "laptop/alice")
	}, "mirrored")
	time.Sleep(time.Second) // let the mirroring hellos drain through the throttle
	big := strings.Repeat("z", 96<<10)
	alice.send(broker.SendReq{To: "srv/bob", Text: big})
	alice.send(broker.SendReq{To: "srv/carol", Text: "p25 quick note"})
	time.Sleep(16 * time.Second)
	nb, nc := countText(bob, big), countText(carol, "p25 quick note")
	t.Logf("P25 big file to bob, then a note to carol on the same sender conn: bob got it %d time(s), carol %d; log: %q", nb, nc, probeTail(logs.all(), 4))
	if nb != 1 || nc != 1 {
		t.Errorf("duplicates: bob %d, carol %d (want 1 and 1)", nb, nc)
	}
}

// P26: the worker byes and removes a gone proxy while a tick that snapshotted
// it before the removal reaches its gone branch, finds the connection dead
// and reopens it: an orphan connection and a far row nobody tracks.
func TestBridgeProbeP26ReopenAfterRemove(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	orphans := 0
	var ids []string
	const rounds = 150
	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("a%d", i)
		a := dialSession(t, r.socks[0], id, id, "pi", false)
		a.conn.close()
		r.b.tickOnce() // laptop/aN mirrored
		a2 := dialSession(t, r.socks[0], id, id, "pi", false)
		a2.call("bye", broker.Request{}, nil)
		a2.conn.close()
		r.b.tickOnce() // gone: nudge; the worker byes right away ...
		r.b.tickOnce() // ... racing this tick's gone branch
		time.Sleep(30 * time.Millisecond)
		r.b.tickOnce()
		r.b.mu.Lock()
		_, held := r.b.proxies[0][id]
		r.b.mu.Unlock()
		if !held && probeHasID(listOn(t, r.socks[1]), "laptop/"+id) {
			orphans++
			ids = append(ids, id)
		}
	}
	for i := 0; i < 5; i++ {
		r.b.tickOnce()
		time.Sleep(50 * time.Millisecond)
	}
	still := 0
	for _, id := range ids {
		if probeHasID(listOn(t, r.socks[1]), "laptop/"+id) {
			still++
		}
	}
	var lines []string
	if len(ids) > 0 {
		for _, l := range r.logs.all() {
			if strings.Contains(l, "laptop/"+ids[0]+" ") || strings.Contains(l, ids[0]+" left") {
				lines = append(lines, l)
			}
		}
	}
	t.Logf("P26 %d of %d departures left a far row the bridge no longer tracks; %d still there 5 ticks later; log for %v: %q", orphans, rounds, still, ids, lines)
	if orphans > 0 {
		t.Errorf("reopened after removal: %d orphan rows", orphans)
	}
}

// P27: a session's last message waits in a busy target queue (bob's mailbox
// is full) while the session leaves. The bye removes the sender proxy first;
// the relay then finds its sender unmirrored and bounces to a session that is
// gone - the message is lost and nobody is told.
func TestBridgeProbeP27LastWordsOfALeavingSender(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	carol := dialSession(t, r.socks[0], "carol", "carol", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", false) // registered: no pushes, nothing acked
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	f1 := dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	f2 := dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	r.b.tickOnce()
	carol.send(broker.SendReq{To: "srv/bob", Text: "p27 first"}) // srv/bob's worker sticks on mailbox_full
	time.Sleep(100 * time.Millisecond)
	alice.send(broker.SendReq{To: "srv/bob", Text: "p27 alice's last words"}) // queued behind it
	alice.call("bye", broker.Request{}, nil)                                  // alice leaves
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce() // alice gone: her proxy is byed and removed
	time.Sleep(300 * time.Millisecond)
	bob.call("ack", broker.Request{IDs: []string{f1.ID, f2.ID}}, nil) // bob's mailbox frees up
	for i := 0; i < 6; i++ {
		time.Sleep(500 * time.Millisecond)
		r.b.tickOnce()
	}
	inbox := probeInbox(t, r.socks[1], "bob")
	got := false
	for _, x := range inbox {
		if strings.Contains(x, "alice's last words") {
			got = true
		}
	}
	t.Logf("P27 bob's mailbox: %q; alice's last words delivered: %v; log tail: %q", inbox, got, probeTail(r.logs.all(), 3))
	if !got {
		t.Errorf("a message sent before its sender left was dropped")
	}
}

// P25b: P25 at production budgets: a 900 KiB file to bob over 64 KiB/s (about
// 14 s), then a note to carol (10 s budget) on the same sender connection.
func TestBridgeProbeP25bProduction(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 20*time.Millisecond, 64<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	carol := dialSession(t, s, "carol", "carol", "pi", true)
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		l1, l2 := listOn(t, l), listOn(t, s)
		return probeHasID(l1, "srv/bob") && probeHasID(l1, "srv/carol") && probeHasID(l2, "laptop/alice")
	}, "mirrored")
	time.Sleep(time.Second)
	alice.send(broker.SendReq{To: "srv/bob", Text: "p25b see attached",
		Attachments: []broker.Attachment{{Type: "file", Name: "big.txt", Content: strings.Repeat("x", 900<<10)}}})
	alice.send(broker.SendReq{To: "srv/carol", Text: "p25b quick note"})
	time.Sleep(45 * time.Second)
	nb, nc := countText(bob, "p25b see attached"), countText(carol, "p25b quick note")
	t.Logf("P25b 900 KiB file to bob then a note to carol, 64 KiB/s, default budgets: bob %d, carol %d", nb, nc)
	if nb != 1 || nc != 1 {
		t.Errorf("duplicates: bob %d, carol %d (want 1 and 1)", nb, nc)
	}
}

// P28-P36 and P31b from the FREEZE-3c.1e review (silent-raven), kept as
// regression tests, with the symForward/halfForward helpers (both
// directions throttled; a half-open ssh -L), stackCount and ackAllBut.
// P29 is adapted to the refcounted transient registry (acquire/release
// instead of two dialTransient calls), as noted in the freeze.

// symForward is lagForward with both directions throttled (a slow downlink:
// hotel or in-flight Wi-Fi, tethering), per connection.
func symForward(t *testing.T, path, target string, oneWay time.Duration, bps int) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			d, err := net.Dial("unix", target)
			if err != nil {
				c.Close()
				continue
			}
			go lagPipe(c, d, oneWay, bps)
			go lagPipe(d, c, oneWay, bps)
		}
	}()
}

// ackAllBut acks everything in a session's mailbox except texts containing keep.
func ackAllBut(t *testing.T, s *bsession, keep string) {
	var msgs []*broker.Message
	if err := s.call("inbox", broker.Request{}, &msgs); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range msgs {
		if !strings.Contains(m.Text, keep) {
			ids = append(ids, m.ID)
		}
	}
	if len(ids) > 0 {
		s.call("ack", broker.Request{IDs: ids}, nil)
	}
}

// P28: C2's departed path covers mail still queued, not mail in retry. alice's
// own message is the one retrying mailbox_full when she leaves: the retry
// re-resolves the sender, finds her proxy removed, and bounces to the gone
// alice - acked away, never delivered.
func TestBridgeProbeP28SenderLeavesMidRetry(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	r.b.tickOnce()
	alice.send(broker.SendReq{To: "srv/bob", Text: "p28 alice's last words"}) // retries mailbox_full
	time.Sleep(300 * time.Millisecond)
	alice.call("bye", broker.Request{}, nil) // alice leaves mid-retry
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce() // her proxy is byed and removed
	time.Sleep(300 * time.Millisecond)
	ackAllBut(t, bob, "last words") // bob's mailbox frees up
	for i := 0; i < 6; i++ {
		time.Sleep(500 * time.Millisecond)
		r.b.tickOnce()
	}
	got := countText(bob, "alice's last words")
	t.Logf("P28 alice's last words delivered %d time(s); log tail: %q", got, probeTail(r.logs.all(), 3))
	if got != 1 {
		t.Errorf("a message in retry when its sender left was dropped")
	}
}

// P29: two transient connections for one departed sender (two target workers
// relaying her last words at once, or one worker's per-round re-dial next to
// another's). Releasing one byes the shared proxy id and deregisters the
// other: its send fails not_registered, which bounces to the gone sender.
func TestBridgeProbeP29SharedTransientId(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	alice.call("bye", broker.Request{}, nil)
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce()
	waitFor(t, 3*time.Second, func() bool { _, ok := r.b.departedInfo(0, "alice"); return ok }, "alice departed")
	// adapted to the refcounted registry (3c.1f): two workers acquire the
	// shared transient; one releases; the other must still send.
	t1, err1 := r.b.acquireTransient(0, "alice")
	t2, err2 := r.b.acquireTransient(0, "alice")
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if t1.conn != t2.conn {
		t.Fatal("acquire did not share one connection")
	}
	r.b.releaseTransient(t1) // worker A's relay landed
	err := t2.conn.callDL(context.Background(), broker.Request{Op: "send", SendReq: broker.SendReq{To: "bob", Text: "p29 via the second transient"}}, nil, fixedDL(5*time.Second))
	t.Logf("P29 send through a second transient after the first was released: %v", err)
	if err != nil {
		t.Errorf("a concurrent transient's release deregistered this one: %v", err)
	}
}

// P30: a departed sender's message retrying mailbox_full through its transient
// when the tunnel drops. Each retry round byes and re-dials the transient; the
// re-dial fails while the tunnel is down, and the "last resort" bounce goes to
// the gone sender - acked away, never delivered.
func TestBridgeProbeP30TransientRetryDuringOutage(t *testing.T) {
	dir := probeDir(t)
	l, s, cutp := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "cut.sock")
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, lim)
	tun := cutForward(t, cutp, s)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	carol := dialSession(t, l, "carol", "carol", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	dave := dialSession(t, s, "dave", "dave", "pi", true)
	f1 := dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	logs := &lockedStrings{}
	b := NewBridge(l, cutp, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 5*time.Second, func() bool {
		l2 := listOn(t, s)
		return probeHasID(listOn(t, l), "srv/bob") && probeHasID(l2, "laptop/alice") && probeHasID(l2, "laptop/carol")
	}, "mirrored")
	carol.send(broker.SendReq{To: "srv/bob", Text: "p30 blocker"}) // bob's worker sticks on mailbox_full
	time.Sleep(200 * time.Millisecond)
	alice.send(broker.SendReq{To: "srv/bob", Text: "p30 alice's last words"}) // queued behind it
	alice.call("bye", broker.Request{}, nil)                                  // alice leaves: her proxy is byed and removed
	time.Sleep(500 * time.Millisecond)
	bob.call("ack", broker.Request{IDs: []string{f1.ID}}, nil) // one slot: the blocker lands, alice's message sticks in its transient retry
	time.Sleep(3 * time.Second)
	tun.cut()
	time.Sleep(3 * time.Second) // the next transient round runs while the tunnel is down
	tun.restore()
	time.Sleep(time.Second)
	ackAllBut(t, bob, "last words")
	probeWait(8*time.Second, func() bool { return countText(bob, "alice's last words") > 0 })
	got := countText(bob, "alice's last words")
	var hits []string
	for _, x := range logs.all() {
		if strings.Contains(x, "alice") {
			hits = append(hits, x)
		}
	}
	t.Logf("P30 alice's last words delivered %d time(s) after the tunnel came back; log lines about alice: %q", got, hits)
	if got != 1 {
		t.Errorf("a departed sender's message was dropped by a tunnel blip during its retry")
	}
}

// P31: a live proxy reopens on a row that queued mail while it was
// unsubscribed (the laptop slept, the bridge was away). Its hello with
// subscribe replays the mailbox AHEAD of the hello's answer, and the hello's
// budget counts only its own bytes: a replay longer than 10 s on the link
// times out every attempt, on the tick goroutine.
func TestBridgeProbeP31ReplayOutlastsHello(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	row := func() *broker.SessionInfo {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	}
	b1 := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b1.tick = 100 * time.Millisecond
	ctx1, cancel1 := context.WithCancel(context.Background())
	go b1.Run(ctx1)
	waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
	cancel1()
	b1.Stop() // the laptop sleeps: the proxy row stays, unsubscribed
	waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && !r.Live }, "laptop/alice offline")
	for i := 0; i < 6; i++ { // ~960 KiB: ~15 s at 64 KiB/s
		bob.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p31 #%d ", i) + strings.Repeat("m", 160<<10)})
	}
	symForward(t, lag, s, 20*time.Millisecond, 64<<10)
	logs := &lockedStrings{}
	b2 := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b2.tick = 500 * time.Millisecond
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	go b2.Run(ctx2)
	time.Sleep(45 * time.Second)
	n := countText(alice, "p31 #")
	q := -1
	if r := row(); r != nil {
		q = r.Queued
	}
	t.Logf("P31 6 x 160 KiB queued while the bridge was away, then a 64 KiB/s link: alice got %d of 6 in 45 s; far row still queues %d; log: %q", n, q, probeTail(logs.all(), 4))
	if n != 6 {
		t.Errorf("the live proxy never came up: alice got %d of 6", n)
	}
}

// P32: the departed record outlives the session's return (a resumed session
// reuses its id). The resumed alice's first message, sent before a tick has
// mirrored her again, takes the transient path instead of the A1 hold; the
// transient retries mailbox_full, and each round's release byes laptop/alice -
// Bye closes every subscription of the id, including the NEW proxy's.
func TestBridgeProbeP32ResumedSenderTransient(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	alice.call("bye", broker.Request{}, nil)
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce()
	waitFor(t, 3*time.Second, func() bool { _, ok := r.b.departedInfo(0, "alice"); return ok }, "alice departed")
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"})               // bob's mailbox is full
	alice2 := dialSession(t, r.socks[0], "alice", "alice", "pi", true) // alice resumes, same id
	alice2.send(broker.SendReq{To: "srv/bob", Text: "p32 first words after resume"})
	time.Sleep(200 * time.Millisecond) // bob's worker: unmirrored + departed -> transient, mailbox_full, retrying
	r.b.tickOnce()                     // the new proxy for the resumed alice: laptop/alice, live
	row := func() string {
		x := findSession(listOn(t, r.socks[1]), func(s broker.SessionInfo) bool { return s.ID == "laptop/alice" })
		if x == nil {
			return "missing"
		}
		return fmt.Sprintf("live=%v", x.Live)
	}
	before := row()
	time.Sleep(2500 * time.Millisecond) // one transient retry round: release (bye) + re-dial
	after := row()
	err := dave.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p32 to the resumed alice"}}, nil)
	t.Logf("P32 laptop/alice after the new proxy came up: %s; after one transient round: %s; a far send to it: %v", before, after, err)
	if after != "live=true" {
		t.Errorf("an old transient's bye knocked out the resumed session's new proxy (%s)", after)
	}
}

// P31b: P31 on the reconcile path - the bridge keeps running (launchd), the
// tunnel drops while the laptop sleeps, mail queues for the live session, and
// on wake the reconcile reopens the subscribed proxy over a slow downlink.
func TestBridgeProbeP31bReplayOnReconnect(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	slow, cutp := filepath.Join(dir, "slow.sock"), filepath.Join(dir, "cut.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	symForward(t, slow, s, 20*time.Millisecond, 64<<10)
	tun := cutForward(t, cutp, slow)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	row := func() *broker.SessionInfo {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	}
	logs := &lockedStrings{}
	b := NewBridge(l, cutp, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 10*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
	// the far row goes live before the hello answer is back: wait until the
	// bridge has installed the proxy, or the cut lands on its creation (P31)
	waitFor(t, 10*time.Second, func() bool {
		b.mu.Lock()
		p := b.proxies[0]["alice"]
		b.mu.Unlock()
		c := (*bconn)(nil)
		if p != nil {
			c = p.connSnapshot()
		}
		return c != nil && !c.isDead()
	}, "alice's proxy installed")
	time.Sleep(time.Second)
	tun.cut() // the laptop sleeps
	waitFor(t, 10*time.Second, func() bool { r := row(); return r != nil && !r.Live }, "laptop/alice offline")
	for i := 0; i < 6; i++ { // ~960 KiB: ~15 s at 64 KiB/s
		bob.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p31b #%d ", i) + strings.Repeat("m", 160<<10)})
	}
	tun.restore() // the laptop wakes on a slow link
	time.Sleep(45 * time.Second)
	n := countText(alice, "p31b #")
	q := -1
	if r := row(); r != nil {
		q = r.Queued
	}
	t.Logf("P31b reconnect after a sleep, 6 x 160 KiB queued, 64 KiB/s: alice got %d of 6 in 45 s; far row still queues %d; log: %q", n, q, probeTail(logs.all(), 4))
	if n != 6 {
		t.Errorf("the live proxy never came back: alice got %d of 6", n)
	}
}

// P33: no sleep, no restart: two big messages to a LIVE laptop session over a
// slow downlink. The ack of the first queues behind the second's push; its
// budget counts only request-side bytes, so it times out, the close cuts the
// push mid-frame, and the reopen's replay of the second never fits the
// hello's budget (P31): the live proxy wedges.
func TestBridgeProbeP33AckBehindPush(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	symForward(t, lag, s, 20*time.Millisecond, 64<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 10*time.Second, func() bool {
		x := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
		return x != nil && x.Live
	}, "laptop/alice live")
	t0 := time.Now()
	for i := 0; i < 2; i++ { // 2 x 800 KiB: ~12.5 s each at 64 KiB/s
		bob.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p33 #%d ", i) + strings.Repeat("m", 800<<10)})
	}
	probeWait(60*time.Second, func() bool { return countText(alice, "p33 #") >= 2 })
	n := countText(alice, "p33 #")
	q := -1
	if x := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" }); x != nil {
		q = x.Queued
	}
	t.Logf("P33 2 x 800 KiB to a live session at 64 KiB/s: alice got %d of 2 after %.0f s; far row still queues %d; log: %q", n, time.Since(t0).Seconds(), q, probeTail(logs.all(), 4))
	if n != 2 {
		t.Errorf("the live proxy wedged: alice got %d of 2", n)
	}
}

// stackCount counts goroutines whose stack contains fn.
func stackCount(fn string) int {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	c := 0
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, fn) {
			c++
		}
	}
	return c
}

// halfTunnel models a half-open ssh -L: halfOpen drops every live connection,
// and from then on the local end still accepts but nothing gets through.
type halfTunnel struct {
	mu     sync.Mutex
	frozen bool
	conns  []net.Conn
}

func halfForward(t *testing.T, path, target string) *halfTunnel {
	ht := &halfTunnel{}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close(); ht.halfOpen() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			ht.mu.Lock()
			frozen := ht.frozen
			ht.conns = append(ht.conns, c)
			ht.mu.Unlock()
			if frozen {
				go io.Copy(io.Discard, c) // accepted, never answered
				continue
			}
			d, err := net.Dial("unix", target)
			if err != nil {
				c.Close()
				continue
			}
			ht.mu.Lock()
			ht.conns = append(ht.conns, d)
			ht.mu.Unlock()
			go func() { io.Copy(d, c); d.Close() }()
			go func() { io.Copy(c, d); c.Close() }()
		}
	}()
	return ht
}

func (ht *halfTunnel) halfOpen() {
	ht.mu.Lock()
	ht.frozen = true
	cs := ht.conns
	ht.conns = nil
	ht.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

// P35: C1's farOK guard, which no committed test exercises (mutation Y2
// survives). A session leaves while the tunnel is half-open: the ping fails,
// so farOK is false, and the gone branch must not dial. Without the guard,
// every tick also waits out a hello on the accepted-but-dead connection.
func TestBridgeProbeP35GoneProxyBehindHalfOpenTunnel(t *testing.T) {
	dir := probeDir(t)
	l, s, hp := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "half.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	ht := halfForward(t, hp, s)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	b := NewBridge(l, hp, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.pingDL = 500 * time.Millisecond
	b.baseDL = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool { return probeHasID(listOn(t, s), "laptop/alice") }, "laptop/alice mirrored")
	ht.halfOpen()                            // the remote end is gone; the local end still accepts
	alice.call("bye", broker.Request{}, nil) // alice leaves during the outage
	time.Sleep(100 * time.Millisecond)
	var worst time.Duration
	for i := 0; i < 3; i++ {
		t0 := time.Now()
		b.tickOnce()
		if d := time.Since(t0); d > worst {
			worst = d
		}
	}
	t.Logf("P35 tickOnce with a gone proxy behind a half-open tunnel: worst %.1f s (pingDL 0.5 s, baseDL 3 s)", worst.Seconds())
	if worst > 2*time.Second {
		t.Errorf("the gone proxy's reopen stalled the tick for %.1f s", worst.Seconds())
	}
}

// P36: the echo at the floor. A send's answer is the stored message, so a
// relay's payload crosses the link twice; callDL doubles only the bytes
// AHEAD (budget(2*ahead + own)), not the call's own. On a link at the floor
// the budgets assume (chunkDL: 1 s per 16 KiB), slow both ways, a relay
// bigger than baseDL's worth times out on its own echo and is re-sent, and
// the note queued behind it dies with the connection. P25 and P25b cannot
// see this: lagForward throttles the uplink only.
func TestBridgeProbeP36EchoAtTheFloor(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	symForward(t, lag, s, 20*time.Millisecond, 16<<10)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	carol := dialSession(t, s, "carol", "carol", "pi", true)
	logs := &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	b.baseDL = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 10*time.Second, func() bool {
		l1, l2 := listOn(t, l), listOn(t, s)
		return probeHasID(l1, "srv/bob") && probeHasID(l1, "srv/carol") && probeHasID(l2, "laptop/alice")
	}, "mirrored")
	time.Sleep(2 * time.Second)        // let the mirroring hellos drain through the throttle
	big := strings.Repeat("z", 96<<10) // 6 s up, then 6 s of echo down; its budget is 3 + 6 s
	alice.send(broker.SendReq{To: "srv/bob", Text: big})
	time.Sleep(200 * time.Millisecond) // the big relay's call goes out first
	alice.send(broker.SendReq{To: "srv/carol", Text: "p36 quick note"})
	time.Sleep(25 * time.Second)
	nb, nc := countText(bob, big), countText(carol, "p36 quick note")
	t.Logf("P36 96 KiB relay then a note, 16 KiB/s both ways, baseDL 3 s: bob got it %d time(s), carol %d; log: %q", nb, nc, probeTail(logs.all(), 3))
	if nb != 1 || nc != 1 {
		t.Errorf("duplicates at the floor: bob %d, carol %d (want 1 and 1)", nb, nc)
	}
}

// P37-P43 and P40b from the FREEZE-3c.1f review (silent-raven), kept as
// regression tests, with the departAlice helper. P40/P40b pin the
// registered-while-queued rule (ticks never wait out a replay); P41
// and P42 pin the progress rule (an answer queued behind a push, and a
// retired connection still receiving); P43 pins the no-bye release.

// departAlice mirrors alice, then has her leave so her proxy is byed and she
// is recorded as departed.
func departAlice(t *testing.T, r *bridgeRig, alice *bsession) {
	t.Helper()
	alice.call("bye", broker.Request{}, nil)
	time.Sleep(50 * time.Millisecond)
	r.b.tickOnce()
	waitFor(t, 3*time.Second, func() bool { _, ok := r.b.departedInfo(0, "alice"); return ok }, "alice departed")
}

// P37: D4's registry is check-then-dial: two FIRST acquires of the same
// departed id (two workers relaying her last words at once) both find no
// entry, both dial, and the second overwrites the first in the map. The
// first's release then byes the id - and the second's row with it.
func TestBridgeProbeP37ConcurrentFirstAcquire(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	r.b.tickOnce()
	departAlice(t, r, alice)
	var t1, t2 *btransient
	var e1, e2 error
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() { defer wg.Done(); <-start; t1, e1 = r.b.acquireTransient(0, "alice") }()
	go func() { defer wg.Done(); <-start; t2, e2 = r.b.acquireTransient(0, "alice") }()
	close(start)
	wg.Wait()
	if e1 != nil || e2 != nil {
		t.Fatal(e1, e2)
	}
	r.b.releaseTransient(t1) // worker A's relay landed
	err := t2.conn.callDL(context.Background(), broker.Request{Op: "send", SendReq: broker.SendReq{To: "bob", Text: "p37 via the second acquire"}}, nil, fixedDL(5*time.Second))
	t.Logf("P37 two concurrent first acquires: shared=%v; a send through the second after the first's release: %v", t1 == t2, err)
	if err != nil {
		t.Errorf("concurrent first acquires dialed two transients; the first release deregistered the second: %v", err)
	}
}

// P38: while a departed sender's transient holds the row, the far side can
// message the id: the send is accepted and queued in the transient's row.
// The last release byes it, Bye keeps a row with mail (PID 0), and nothing
// drains or bounces it - the sender is never told.
func TestBridgeProbeP38MailToATransientRow(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	alice.send(broker.SendReq{To: "srv/bob", Text: "p38 alice's last words"})
	time.Sleep(300 * time.Millisecond) // the relay retries mailbox_full
	departAlice(t, r, alice)           // her proxy is byed; the retry moves to a transient
	time.Sleep(2500 * time.Millisecond)
	var sendErr error
	err := dave.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p38 for alice"}}, nil)
	sendErr = err
	ackAllBut(t, bob, "last words") // bob's mailbox frees: the relay lands, the transient is released
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "alice's last words") == 1 }, "alice's last words")
	time.Sleep(time.Second)
	row := findSession(listOn(t, r.socks[1]), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	rowState := "gone"
	if row != nil {
		rowState = fmt.Sprintf("kept, live=%v, queued=%d", row.Live, row.Queued)
	}
	bounced := 0
	dave.mu.Lock()
	for _, m := range dave.msgs {
		if strings.Contains(m.Text, "p38 for alice") || strings.Contains(m.From, "laptop/alice") {
			bounced++
		}
	}
	dave.mu.Unlock()
	t.Logf("P38 dave's send to laptop/alice during the transient: %v; after the release the row is %s; dave heard back %d time(s)", sendErr, rowState, bounced)
	if sendErr == nil && bounced == 0 {
		t.Errorf("a message to a departed id was accepted into the transient's row and stranded without a word to its sender")
	}
}

// P39: P35's stall through the wanted branch. The farOK guard covers the gone
// branch, but transition (and rehello) still call the far side on the tick
// while it is down: behind a half-open tunnel, every tick waits out a hello
// for each laptop session whose liveness changed.
func TestBridgeProbeP39TransitionBehindHalfOpenTunnel(t *testing.T) {
	dir := probeDir(t)
	l, s, hp := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "half.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	ht := halfForward(t, hp, s)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	b := NewBridge(l, hp, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.pingDL = 500 * time.Millisecond
	b.baseDL = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		x := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
		return x != nil && x.Live
	}, "laptop/alice live")
	ht.halfOpen()      // the remote end is gone; the local end still accepts
	alice.conn.close() // alice goes offline on the laptop during the outage
	time.Sleep(100 * time.Millisecond)
	var worst time.Duration
	for i := 0; i < 3; i++ {
		t0 := time.Now()
		b.tickOnce()
		if d := time.Since(t0); d > worst {
			worst = d
		}
	}
	t.Logf("P39 tickOnce with a live->offline transition behind a half-open tunnel: worst %.1f s (pingDL 0.5 s, baseDL 3 s)", worst.Seconds())
	if worst > 2*time.Second {
		t.Errorf("the transition stalled the tick for %.1f s while the far side was down", worst.Seconds())
	}
}

// P40: D2's second part, pinned. P31/P31b assert delivery only, and with the
// progress rule a subscribed hello rides out a replay and delivers - on the
// tick. Here the ticks are driven by hand and timed while a reconnect drains
// a 960 KiB backlog over 64 KiB/s: no tick may wait out the replay.
func TestBridgeProbeP40ReplayStaysOffTheTick(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	slow, cutp := filepath.Join(dir, "slow.sock"), filepath.Join(dir, "cut.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	symForward(t, slow, s, 20*time.Millisecond, 64<<10)
	tun := cutForward(t, cutp, slow)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	row := func() *broker.SessionInfo {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	}
	b := NewBridge(l, cutp, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 10*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
	waitFor(t, 10*time.Second, func() bool {
		b.mu.Lock()
		p := b.proxies[0]["alice"]
		b.mu.Unlock()
		return p != nil && p.connSnapshot() != nil && !p.connSnapshot().isDead()
	}, "alice's proxy installed")
	time.Sleep(time.Second)
	tun.cut() // the laptop sleeps
	waitFor(t, 10*time.Second, func() bool { r := row(); return r != nil && !r.Live }, "laptop/alice offline")
	for i := 0; i < 6; i++ { // ~960 KiB: ~15 s at 64 KiB/s
		bob.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p40 #%d ", i) + strings.Repeat("m", 160<<10)})
	}
	tun.restore()
	var worst time.Duration
	ticks := 0
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && countText(alice, "p40 #") < 6 {
		t0 := time.Now()
		b.tickOnce()
		ticks++
		if d := time.Since(t0); d > worst {
			worst = d
		}
		time.Sleep(500 * time.Millisecond)
	}
	n := countText(alice, "p40 #")
	t.Logf("P40 reconnect with 6 x 160 KiB queued, 64 KiB/s both ways: alice got %d of 6 over %d ticks; the slowest tick took %.1f s", n, ticks, worst.Seconds())
	if n != 6 {
		t.Errorf("alice got %d of 6", n)
	}
	if worst > 3*time.Second {
		t.Errorf("a tick waited out the replay (%.1f s)", worst.Seconds())
	}
}

// P40b: X10's witness, the creation path of P40. P31 asserts delivery only,
// and with the progress rule a fresh bridge's subscribed hello rides out the
// replay and delivers - on the tick. Here a fresh bridge meets a 960 KiB
// backlog over 64 KiB/s, and its ticks are driven by hand and timed.
func TestBridgeProbeP40bCreationReplayStaysOffTheTick(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	row := func() *broker.SessionInfo {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	}
	b1 := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b1.tick = 100 * time.Millisecond
	ctx1, cancel1 := context.WithCancel(context.Background())
	go b1.Run(ctx1)
	waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
	cancel1()
	b1.Stop() // the bridge goes away: the proxy row stays, unsubscribed
	waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && !r.Live }, "laptop/alice offline")
	for i := 0; i < 6; i++ { // ~960 KiB: ~15 s at 64 KiB/s
		bob.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p40b #%d ", i) + strings.Repeat("m", 160<<10)})
	}
	symForward(t, lag, s, 20*time.Millisecond, 64<<10)
	b2 := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	b2.startCtx(ctx2)
	t.Cleanup(b2.Stop)
	var worst time.Duration
	ticks := 0
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && countText(alice, "p40b #") < 6 {
		t0 := time.Now()
		b2.tickOnce()
		ticks++
		if d := time.Since(t0); d > worst {
			worst = d
		}
		time.Sleep(500 * time.Millisecond)
	}
	n := countText(alice, "p40b #")
	t.Logf("P40b fresh bridge, 6 x 160 KiB queued, 64 KiB/s both ways: alice got %d of 6 over %d ticks; the slowest tick took %.1f s", n, ticks, worst.Seconds())
	if n != 6 {
		t.Errorf("alice got %d of 6", n)
	}
	if worst > 3*time.Second {
		t.Errorf("a tick waited out the replay (%.1f s)", worst.Seconds())
	}
}

// p41rig: alice live on the laptop, bob live on the server, 64 KiB/s both
// ways, baseDL 3 s. bob's 640 KiB message to alice is coming down her
// subscribed proxy connection (~10 s) when her note to bob is relayed through
// the same connection: the note's answer queues behind the push.
func p41rig(t *testing.T) (alice, bob *bsession, logs *lockedStrings) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	symForward(t, lag, s, 20*time.Millisecond, 64<<10)
	alice = dialSession(t, l, "alice", "alice", "pi", true)
	bob = dialSession(t, s, "bob", "bob", "pi", true)
	logs = &lockedStrings{}
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), func(f string, a ...any) { logs.add(fmt.Sprintf(f, a...)) })
	b.tick = 500 * time.Millisecond
	b.baseDL = 3 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go b.Run(ctx)
	waitFor(t, 10*time.Second, func() bool {
		x := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
		return probeHasID(listOn(t, l), "srv/bob") && x != nil && x.Live
	}, "mirrored")
	time.Sleep(2 * time.Second) // let the mirroring hellos drain through the throttle
	bob.send(broker.SendReq{To: "laptop/alice", Text: "p41 big " + strings.Repeat("b", 640<<10)})
	time.Sleep(300 * time.Millisecond) // its push is on the way down alice's proxy connection
	alice.send(broker.SendReq{To: "srv/bob", Text: "p41 quick note"})
	return alice, bob, logs
}

// P41: D2(ii)'s own case. P33, P31b and P40 pass without the progress rule,
// because (i) drains through the inbox; nothing committed has an answer
// queued behind a push. Without the rule, the note's call times out at its
// 3 s budget, the close cuts the push, and the retry stores the note again.
func TestBridgeProbeP41AnswerBehindAPush(t *testing.T) {
	alice, bob, logs := p41rig(t)
	time.Sleep(20 * time.Second)
	nb, na := countText(bob, "p41 quick note"), countText(alice, "p41 big ")
	t.Logf("P41 note relayed behind a 640 KiB push, 64 KiB/s, baseDL 3 s: bob got the note %d time(s), alice the big one %d; log: %q", nb, na, probeTail(logs.all(), 3))
	if nb != 1 || na != 1 {
		t.Errorf("bob %d, alice %d (want 1 and 1)", nb, na)
	}
}

// P42: X5's witness, P41 plus a transition. alice goes offline while the
// note's answer is still behind the push, so the tick retires her subscribed
// connection with the call in flight. The grace (newest deadline + slack)
// runs out long before the push ends; only the progress rule keeps the
// retired connection open until the answer arrives.
func TestBridgeProbeP42RetiredWhileReceiving(t *testing.T) {
	alice, bob, logs := p41rig(t)
	time.Sleep(time.Second)
	alice.conn.close() // alice goes offline: the next tick retires her subscribed connection
	time.Sleep(20 * time.Second)
	nb := countText(bob, "p41 quick note")
	t.Logf("P42 the same, and alice goes offline 1 s later: bob got the note %d time(s); log: %q", nb, probeTail(logs.all(), 3))
	if nb != 1 {
		t.Errorf("bob got the note %d time(s) (want 1)", nb)
	}
}

// P43: X17's witness. P32 passes without D5's no-bye rule: since D1 the
// transient lives until its relay lands, and P32 never lets it land. Here the
// old relay lands after the resumed alice's new proxy is up, and the last
// release must not bye the row her proxy now owns.
func TestBridgeProbeP43OldRelayLandsAfterResume(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	departAlice(t, r, alice)
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"})               // bob's mailbox is full
	alice2 := dialSession(t, r.socks[0], "alice", "alice", "pi", true) // alice resumes, same id
	alice2.send(broker.SendReq{To: "srv/bob", Text: "p43 first words after resume"})
	time.Sleep(200 * time.Millisecond) // unmirrored + departed -> transient, mailbox_full, retrying
	r.b.tickOnce()                     // the resumed alice's new proxy: laptop/alice, live
	row := func() string {
		x := findSession(listOn(t, r.socks[1]), func(s broker.SessionInfo) bool { return s.ID == "laptop/alice" })
		if x == nil {
			return "missing"
		}
		return fmt.Sprintf("live=%v", x.Live)
	}
	waitFor(t, 3*time.Second, func() bool { return row() == "live=true" }, "the new proxy is live")
	ackAllBut(t, bob, "first words") // bob has room: the old relay lands on its next round
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "p43 first words") == 1 }, "the old relay landed")
	time.Sleep(500 * time.Millisecond) // the last release follows the landing
	after := row()
	err := dave.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p43 to the resumed alice"}}, nil)
	time.Sleep(time.Second)
	got := countText(alice2, "p43 to the resumed alice")
	t.Logf("P43 the old relay landed after the resume: laptop/alice is %s; a far send to it: %v; alice got it %d time(s)", after, err, got)
	if after != "live=true" || got != 1 {
		t.Errorf("the old transient's last release knocked out the resumed alice's proxy (%s, got %d)", after, got)
	}
}

// P44-P46 and P34b from the FREEZE-3c.1g review (silent-raven), kept as
// regression tests. P34b replaces P34 (same scenario, evidence scoped to
// this bridge only: pendingCount, and a sentinel that must stay queued after
// Stop - process-wide stack counts drifted under -count runs and load).
// P44: N2 depends on acquireTransient returning the dial's broker error. A
// failed dial deletes the placeholder, and every acquirer (the dialer too)
// re-reads the map, finds nothing, and returns a generic "unavailable": the
// refusal is lost, so N2's bounce never fires. The id here is valid on its
// own daemon and over 256 bytes with the prefix, so the far hello refuses it.
func TestBridgeProbeP44AcquireKeepsTheRefusal(t *testing.T) {
	r := newBridgeRigManual(t, broker.DefaultLimits())
	id := strings.Repeat("x", 252)
	r.b.mu.Lock()
	r.b.departed[0][id] = broker.SessionInfo{ID: id, Name: "x", Harness: "pi"}
	r.b.mu.Unlock()
	_, err := r.b.acquireTransient(0, id)
	var be *broker.Error
	isBE := errors.As(err, &be)
	t.Logf("P44 acquire of a departed id whose transient hello is refused: %v (a broker error: %v)", err, isBE)
	if !isBE {
		t.Errorf("acquireTransient replaced the refusal with %q, so N2's bounce never fires", err)
	}
}

// P45: E2 runs the inbox (and any bounces and the ack) between removing the
// map entry and the bye. A relay that acquires in that window dials a new
// transient on the same id, and the old bye then deregisters it: P29's loss
// again. Before E2 the bye went out at once, so the window was nil.
func TestBridgeProbeP45ReleaseDrainRacesANewAcquire(t *testing.T) {
	dir := probeDir(t)
	l, s, lag := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	lagForward(t, lag, s, 50*time.Millisecond, 0) // 100 ms round trips, no throttle
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, lag, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool { return probeHasID(listOn(t, s), "laptop/alice") }, "laptop/alice mirrored")
	alice.call("bye", broker.Request{}, nil)
	time.Sleep(50 * time.Millisecond)
	b.tickOnce() // gone: the worker drains and byes the proxy
	waitFor(t, 5*time.Second, func() bool { _, ok := b.departedInfo(0, "alice"); return ok }, "alice departed")
	t1, err := b.acquireTransient(0, "alice") // relay A's transient
	if err != nil {
		t.Fatal(err)
	}
	go b.releaseTransient(t1) // relay A landed: the last release drains, then byes
	time.Sleep(20 * time.Millisecond)
	t2, err := b.acquireTransient(0, "alice") // relay B, a moment later
	if err != nil {
		t.Fatal(err)
	}
	defer b.releaseTransient(t2)
	time.Sleep(500 * time.Millisecond) // the first release's bye has landed
	err = t2.conn.callDL(context.Background(), broker.Request{Op: "send", SendReq: broker.SendReq{To: "bob", Text: "p45 relay B"}}, nil, fixedDL(5*time.Second))
	t.Logf("P45 a relay acquires 20 ms into the previous transient's last release (100 ms round trips): shared=%v; its send after that release's bye: %v", t1 == t2, err)
	if err != nil {
		t.Errorf("the previous transient's bye deregistered the new one: %v", err)
	}
}

// P46: E2's ask path and its cleanup, which P38 does not assert. dave asks
// laptop/alice (no_wait) while her transient holds the row. The last release
// must answer the ask (reply_to), and ack it, so that the bye removes the row.
func TestBridgeProbeP46AskToATransientRow(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	alice.send(broker.SendReq{To: "srv/bob", Text: "p46 alice's last words"})
	time.Sleep(300 * time.Millisecond) // the relay retries mailbox_full
	departAlice(t, r, alice)           // her proxy is byed; the retry moves to a transient
	time.Sleep(2500 * time.Millisecond)
	var ask broker.Message
	if err := dave.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p46 a question for alice", ExpectsReply: true, NoWait: true}}, &ask); err != nil {
		t.Fatal(err)
	}
	ackAllBut(t, bob, "last words") // bob has room: the relay lands, and the transient is released
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "alice's last words") == 1 }, "alice's last words")
	time.Sleep(time.Second)
	var answer *broker.Message
	dave.mu.Lock()
	for _, m := range dave.msgs {
		if m.ReplyTo == ask.ID {
			answer = m
		}
	}
	dave.mu.Unlock()
	row := findSession(listOn(t, r.socks[1]), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	rowState := "gone"
	if row != nil {
		rowState = fmt.Sprintf("kept, live=%v, queued=%d", row.Live, row.Queued)
	}
	text := "<none>"
	if answer != nil {
		text = answer.Text
	}
	t.Logf("P46 dave's async ask to laptop/alice during the transient: answered=%v (%q); after the release the row is %s", answer != nil, text, rowState)
	if answer == nil || rowState != "gone" {
		t.Errorf("the ask got no answer, or the row outlived the release (answered=%v, row %s)", answer != nil, rowState)
	}
}

// P34b: P34 per bridge. P34 compares process-wide stack counts with a
// baseline taken at its start. A worker from an earlier test that is still
// winding down (its context is cancelled, but under -race and load it has not
// run yet) is in that baseline and can leave mid-test, so the exact equality
// fails: one failure in a -race -count=2 family run. Here the evidence
// belongs to this bridge only:
//   - parked: the target proxy holds one pending message and its queue is
//     empty, so the worker has it;
//   - after Stop: pending drops to 0, so relayOne returned;
//   - and a sentinel pushed into the queue stays there, so relay() has
//     exited and nobody consumes it.
func TestBridgeProbeP34bStopEndsParkedWorkers(t *testing.T) {
	proxyOf := func(b *Bridge, home int, id string) *bproxy {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.proxies[home][id]
	}
	stopped := func(p *bproxy, what string) {
		t.Helper()
		if !probeWait(3*time.Second, func() bool { return p.pendingCount() == 0 }) {
			t.Errorf("a worker parked in %s did not return from relayOne after Stop", what)
			return
		}
		p.enqueue(&broker.Message{ID: "p34b-sentinel", From: "nobody", To: p.realID, Text: "sentinel"})
		time.Sleep(300 * time.Millisecond)
		if len(p.queue) != 1 {
			t.Errorf("a worker parked in %s kept consuming its queue after Stop", what)
		}
	}
	// Parked in a hold: alice live, a brand-new sender messages her.
	r := newBridgeRigManual(t, broker.DefaultLimits())
	dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	r.b.tickOnce()
	pa := proxyOf(r.b, 0, "alice")
	n := dialSession(t, r.socks[1], "nina", "nina", "pi", true) // not mirrored yet
	n.send(broker.SendReq{To: "laptop/alice", Text: "hold me"})
	waitFor(t, 3*time.Second, func() bool { return pa.pendingCount() == 1 && len(pa.queue) == 0 }, "alice's worker holds the message")
	time.Sleep(300 * time.Millisecond) // polling in the hold
	r.b.Stop()
	stopped(pa, "the hold")

	// Parked in a retry: bob live with a full mailbox; carol's message sticks.
	lim := broker.DefaultLimits()
	lim.MailboxCap = 1
	r2 := newBridgeRigManual(t, lim)
	carol := dialSession(t, r2.socks[0], "carol", "carol", "pi", true)
	dialSession(t, r2.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r2.socks[1], "dave", "dave", "pi", true)
	r2.b.tickOnce()
	pb := proxyOf(r2.b, 1, "bob")
	dave.send(broker.SendReq{To: "bob", Text: "fill"})
	carol.send(broker.SendReq{To: "srv/bob", Text: "stuck"})
	waitFor(t, 3*time.Second, func() bool { return pb.pendingCount() == 1 && len(pb.queue) == 0 }, "bob's worker holds the message")
	time.Sleep(300 * time.Millisecond) // mailbox_full: waiting out the backoff
	r2.b.Stop()
	stopped(pb, "the retry")
}

// P47, P48 and P50 from the FREEZE-3c.1h review (silent-raven), kept as
// regression tests, with the flakyForward helper (a far side that fails
// every other connection). P47 pins the persisted row set and its reaper
// (G2); P48 pins the acquire race (G1, under -race); P50 pins N6's inbox
// loop.
// P47: orphaned proxy rows after a bridge restart. A stopped bridge leaves
// its rows registered on purpose (P31: mail queued meanwhile is drained when
// it returns). But a session that ends while the bridge is away is never in
// the new bridge's list, so nothing ever finishes its row: Sweep keeps
// prefixed rows for MailTTL (24 h), and a send to it is accepted and never
// answered. A dead transient's release (it cannot bye) leaves the same row.
func TestBridgeProbeP47OrphanRowAfterRestart(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	row := func() *broker.SessionInfo {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
	}
	b1 := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	b1.startCtx(ctx1)
	b1.tickOnce()
	waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
	cancel1()
	b1.Stop()                                // the bridge goes away: a reboot, an upgrade, a crash
	alice.call("bye", broker.Request{}, nil) // alice's session ends meanwhile
	b2 := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	b2.startCtx(ctx2)
	t.Cleanup(b2.Stop)
	for i := 0; i < 3; i++ {
		b2.tickOnce()
		time.Sleep(200 * time.Millisecond)
	}
	var m broker.Message
	err := bob.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p47 for alice", ExpectsReply: true, NoWait: true}}, &m)
	for i := 0; i < 5; i++ {
		b2.tickOnce()
		time.Sleep(200 * time.Millisecond)
	}
	state := "gone"
	if r := row(); r != nil {
		state = fmt.Sprintf("kept, live=%v, queued=%d", r.Live, r.Queued)
	}
	heard := 0
	bob.mu.Lock()
	for _, x := range bob.msgs {
		if x.ReplyTo == m.ID {
			heard++
		}
	}
	bob.mu.Unlock()
	t.Logf("P47 alice ended while the bridge was away; after the restart laptop/alice is %s; bob's ask to it: %v; bob heard back %d time(s)", state, err, heard)
	if err == nil && heard == 0 {
		t.Errorf("an ask to an orphaned proxy row was accepted and never answered (row %s)", state)
	}
}

// flakyForward fails every other connection (it accepts, then closes after
// failAfter, so the hello sees "connection closed"); the rest pass through
// with oneWay latency each way. A far side that flaps.
func flakyForward(t *testing.T, path, target string, oneWay, failAfter time.Duration) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		n := 0
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n++
			if n%2 == 1 {
				go func() { time.Sleep(failAfter); c.Close() }()
				continue
			}
			d, err := net.Dial("unix", target)
			if err != nil {
				c.Close()
				continue
			}
			go lagPipe(c, d, oneWay, 0)
			go lagPipe(d, c, oneWay, 0)
		}
	}()
}

// P48: the dialer writes t.conn without b.mu, and every acquirer re-reads
// the map and reads cur.conn under b.mu. When an acquirer whose own dial
// failed re-reads while another goroutine's placeholder is still dialing, it
// reads that placeholder's conn with no happens-before to the dialer's write:
// a data race. Run under -race; the far side flaps, as after an outage.
func TestBridgeProbeP48AcquireUnderAFlappingFarSide(t *testing.T) {
	dir := probeDir(t)
	l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "flaky.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	flakyForward(t, f, s, 10*time.Millisecond, 30*time.Millisecond) // slow failures: waiters pile up
	b := NewBridge(l, f, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.mu.Lock()
	b.departed[0]["alice"] = broker.SessionInfo{ID: "alice", Name: "alice", Harness: "pi"}
	b.mu.Unlock()
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, fail := 0, 0
	stop := time.Now().Add(4 * time.Second)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(seed))
			for time.Now().Before(stop) {
				tr, err := b.acquireTransient(0, "alice")
				mu.Lock()
				if err == nil {
					ok++
				} else {
					fail++
				}
				mu.Unlock()
				if err == nil {
					b.releaseTransient(tr) // refs drop to 0 often: frequent closes and re-dials
				}
				time.Sleep(time.Duration(rnd.Intn(3)) * time.Millisecond)
			}
		}(int64(g))
	}
	wg.Wait()
	b.mu.Lock()
	left := len(b.transients)
	b.mu.Unlock()
	t.Logf("P48 16 relays acquiring for 4 s against a flapping far side: %d acquired, %d failed; registry entries left: %d", ok, fail, left)
	if left != 0 {
		t.Errorf("%d transient entries outlived every release", left)
	}
}

// P50: N6's loop, pinned. dave sends 2 x 600 KiB to laptop/alice while her
// transient holds the row; together they exceed one frame, so the inbox needs
// two batches. The last release must bounce both and leave no row behind.
func TestBridgeProbeP50DrainNeedsTwoBatches(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.MailboxCap = 2
	r := newBridgeRigManual(t, lim)
	alice := dialSession(t, r.socks[0], "alice", "alice", "pi", true)
	bob := dialSession(t, r.socks[1], "bob", "bob", "pi", true)
	dave := dialSession(t, r.socks[1], "dave", "dave", "pi", true)
	r.b.tickOnce()
	dave.send(broker.SendReq{To: "bob", Text: "fill 1"})
	dave.send(broker.SendReq{To: "bob", Text: "fill 2"}) // bob's mailbox is full
	alice.send(broker.SendReq{To: "srv/bob", Text: "p50 alice's last words"})
	time.Sleep(300 * time.Millisecond) // the relay retries mailbox_full
	departAlice(t, r, alice)           // her proxy is byed; the retry moves to a transient
	time.Sleep(2500 * time.Millisecond)
	ids := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := dave.send(broker.SendReq{To: "laptop/alice", Text: fmt.Sprintf("p50 #%d ", i) + strings.Repeat("d", 600<<10)})
		ids[m.ID] = true
	}
	ackAllBut(t, bob, "last words") // bob has room: the relay lands, and the transient is released
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "alice's last words") == 1 }, "alice's last words")
	time.Sleep(time.Second)
	bounced := 0
	dave.mu.Lock()
	for _, m := range dave.msgs {
		if strings.Contains(m.Text, "not delivered") {
			bounced++
		}
	}
	dave.mu.Unlock()
	state := "gone"
	if x := findSession(listOn(t, r.socks[1]), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" }); x != nil {
		state = fmt.Sprintf("kept, live=%v, queued=%d", x.Live, x.Queued)
	}
	t.Logf("P50 2 x 600 KiB to laptop/alice during the transient: dave got %d bounce(s); after the release the row is %s", bounced, state)
	if bounced != 2 || state != "gone" {
		t.Errorf("the release drained one batch only (bounces %d, row %s)", bounced, state)
	}
}

// P51, P52 and P53 from the FREEZE-3c.1i review (silent-raven), kept as
// regression tests; the P51t timeline probe was diagnostic and is dropped.
// P51 pins H1b (a session resuming while its row is reaped must not lose
// tracking); P52 and P53 pin H1a (mail that lands between the last inbox and
// the bye keeps the row; the set must not drop the id on the bye alone).
// P51: a reap racing a session that comes back under the same id. Proxy
// creation says hello on the far row (openConn) BEFORE it publishes the proxy
// in b.proxies, so for one round trip the row is the new proxy's while
// drainRowBye's ownership re-check cannot see it. A re-check in that window
// misses the proxy: the bye goes out, and after it returns rows.remove drops
// the id although the proxy registered it meanwhile. The set has lost a row it
// owns, so the next restart gap orphans it again (G2's symptom). Each offset is
// a fresh setup with 200 ms each way on the far side; the offset is how long
// after the reaping tick the session returns and the next tick runs. One
// message waits in the orphaned row: its bounce is a round trip that moves the
// re-check into the next tick's creation (with an empty row the next tick's
// ping and list take longer than the whole reap).
func TestBridgeProbeP51ReapRacesAResumedSession(t *testing.T) {
	lost := 0
	for _, off := range []time.Duration{900 * time.Millisecond, 1000 * time.Millisecond, 1100 * time.Millisecond} {
		t.Run(fmt.Sprintf("offset_%dms", off.Milliseconds()), func(t *testing.T) {
			dir := probeDir(t)
			l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
			brokerAt(t, l, broker.DefaultLimits())
			brokerAt(t, s, broker.DefaultLimits())
			lagForward(t, f, s, 200*time.Millisecond, 0)
			mapPath := filepath.Join(dir, "m.map")
			alice := dialSession(t, l, "alice", "alice", "pi", true)
			bob := dialSession(t, s, "bob", "bob", "pi", true)
			row := func() *broker.SessionInfo {
				return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
			}
			// b1 mirrors alice, then goes away; alice ends meanwhile.
			b1 := NewBridge(l, s, "laptop/", "srv/", mapPath, nil)
			ctx1, cancel1 := context.WithCancel(context.Background())
			b1.startCtx(ctx1)
			b1.tickOnce()
			waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
			cancel1()
			b1.Stop()
			alice.call("bye", broker.Request{}, nil)
			bob.send(broker.SendReq{To: "laptop/alice", Text: "p51 queued while no bridge runs"}) // the reap bounces it
			// b2 starts behind the slow tunnel: its first tick reaps laptop/alice.
			b2 := NewBridge(l, f, "laptop/", "srv/", mapPath, nil)
			ctx2, cancel2 := context.WithCancel(context.Background())
			b2.startCtx(ctx2)
			b2.tickOnce()
			b2.mu.Lock()
			reaping := b2.reaping[transientKey(0, "alice")]
			b2.mu.Unlock()
			// alice comes back under the same id; the next tick creates her proxy
			// while the reap is still in flight.
			time.Sleep(off)
			dialSession(t, l, "alice", "alice", "pi", true)
			b2.tickOnce()
			time.Sleep(2 * time.Second) // the reap finishes
			b2.mu.Lock()
			_, hasProxy := b2.proxies[0]["alice"]
			b2.mu.Unlock()
			tracked := slices.Contains(b2.rows.snapshot(0), "alice")
			after := "gone"
			if r := row(); r != nil {
				after = fmt.Sprintf("live=%v", r.Live)
			}
			for i := 0; i < 2; i++ { // let the proxy recover its row
				b2.tickOnce()
				time.Sleep(300 * time.Millisecond)
			}
			recovered := "gone"
			if r := row(); r != nil {
				recovered = fmt.Sprintf("live=%v", r.Live)
			}
			// The consequence: b2 goes away, alice ends, b3 starts.
			cancel2()
			b2.Stop()
			b3 := NewBridge(l, s, "laptop/", "srv/", mapPath, nil)
			ctx3, cancel3 := context.WithCancel(context.Background())
			t.Cleanup(cancel3)
			b3.startCtx(ctx3)
			t.Cleanup(b3.Stop)
			// alice's second session ends while no bridge runs
			sess := findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "alice" })
			if sess != nil {
				a2 := dialSession(t, l, "alice", "alice", "pi", false)
				a2.call("bye", broker.Request{}, nil)
			}
			for i := 0; i < 4; i++ {
				b3.tickOnce()
				time.Sleep(300 * time.Millisecond)
			}
			var m broker.Message
			err := bob.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p51 for alice", ExpectsReply: true, NoWait: true}}, &m)
			for i := 0; i < 4; i++ {
				b3.tickOnce()
				time.Sleep(300 * time.Millisecond)
			}
			heard := 0
			bob.mu.Lock()
			for _, x := range bob.msgs {
				if err == nil && x.ReplyTo == m.ID {
					heard++
				}
			}
			bob.mu.Unlock()
			t.Logf("P51 offset %v: reap in flight after tick 1: %v; after the reap: proxy=%v, tracked=%v, far row %s (after 2 more ticks: %s); after the next restart gap, bob's ask: err=%v, heard back %d",
				off, reaping, hasProxy, tracked, after, recovered, err, heard)
			if hasProxy && !tracked {
				lost++
				t.Errorf("the reap dropped an id that the resumed session's proxy registered meanwhile")
			}
		})
	}
	t.Logf("P51 offsets where the set lost a live proxy's row: %d of 3", lost)
}

// P52: mail that lands between a reap's last inbox and its bye. broker.Bye
// keeps a row that holds mail (it only clears the PID), but drainRowBye
// removes the id from the set on any successful bye. The row then lingers,
// untracked, holding mail that nobody answers until MailTTL - in a running
// bridge, no restart needed. drainAndBye's gone path removes the same way.
// Each offset is a fresh setup; bob's message lands that long after the
// reaping tick, inside the reap's last round trip (200 ms each way).
func TestBridgeProbeP52MailBetweenLastInboxAndBye(t *testing.T) {
	lingering := 0
	for _, off := range []time.Duration{1850 * time.Millisecond, 2000 * time.Millisecond, 2150 * time.Millisecond} {
		t.Run(fmt.Sprintf("offset_%dms", off.Milliseconds()), func(t *testing.T) {
			dir := probeDir(t)
			l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
			brokerAt(t, l, broker.DefaultLimits())
			brokerAt(t, s, broker.DefaultLimits())
			lagForward(t, f, s, 200*time.Millisecond, 0)
			mapPath := filepath.Join(dir, "m.map")
			alice := dialSession(t, l, "alice", "alice", "pi", true)
			bob := dialSession(t, s, "bob", "bob", "pi", true)
			row := func() *broker.SessionInfo {
				return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
			}
			b1 := NewBridge(l, s, "laptop/", "srv/", mapPath, nil)
			ctx1, cancel1 := context.WithCancel(context.Background())
			b1.startCtx(ctx1)
			b1.tickOnce()
			waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live }, "laptop/alice live")
			cancel1()
			b1.Stop()
			alice.call("bye", broker.Request{}, nil)
			bob.send(broker.SendReq{To: "laptop/alice", Text: "p52 queued while no bridge runs"})
			b2 := NewBridge(l, f, "laptop/", "srv/", mapPath, nil)
			ctx2, cancel2 := context.WithCancel(context.Background())
			t.Cleanup(cancel2)
			b2.startCtx(ctx2)
			t.Cleanup(b2.Stop)
			b2.tickOnce() // reaps laptop/alice
			time.Sleep(off)
			late := bob.send(broker.SendReq{To: "laptop/alice", Text: "p52 late", ExpectsReply: true, NoWait: true})
			time.Sleep(1500 * time.Millisecond) // the reap finishes
			for i := 0; i < 4; i++ {            // the running bridge keeps ticking
				b2.tickOnce()
				time.Sleep(300 * time.Millisecond)
			}
			state := "gone"
			if r := row(); r != nil {
				state = fmt.Sprintf("kept, live=%v, queued=%d", r.Live, r.Queued)
			}
			tracked := slices.Contains(b2.rows.snapshot(0), "alice")
			heard := 0
			bob.mu.Lock()
			for _, x := range bob.msgs {
				if x.ReplyTo == late.ID {
					heard++
				}
			}
			bob.mu.Unlock()
			t.Logf("P52 offset %v: after the reap and 4 more ticks, laptop/alice is %s, tracked=%v; bob heard back on the late ask %d time(s)", off, state, tracked, heard)
			if state != "gone" && !tracked && heard == 0 {
				lingering++
				t.Errorf("a row kept by its bye (it held late mail) was dropped from the set: it lingers untracked")
			}
		})
	}
	t.Logf("P52 offsets that left an untracked row holding mail: %d of 3", lingering)
}

// P53: P52's window on the gone path of a running bridge. alice leaves while
// the bridge runs; her proxy's drainAndBye reads the inbox, then byes. Mail
// that lands between the two keeps the row (broker.Bye only clears the PID of
// a row that holds mail), and drainAndBye removes the id after a bye that
// returned OK. Two variants:
//   - subscribed (alice live): Bye closes the requester's own sink, so the bye
//     never gets its response; the id stays tracked by accident, and the next
//     tick's reaper finishes the row. The control.
//   - registered (alice not live): the bye returns OK, the id is removed, and
//     the row lingers untracked with the late ask unanswered.
//
// The ask goes through call, not send: one that lands after the bye gets
// unknown_target, which is an answer too.
func TestBridgeProbeP53GonePathLateMail(t *testing.T) {
	offs := []time.Duration{150 * time.Millisecond, 300 * time.Millisecond, 450 * time.Millisecond, 600 * time.Millisecond, 750 * time.Millisecond}
	for _, live := range []bool{true, false} {
		kind := "registered"
		if live {
			kind = "subscribed"
		}
		lingering := 0
		for _, off := range offs {
			t.Run(fmt.Sprintf("%s/offset_%dms", kind, off.Milliseconds()), func(t *testing.T) {
				dir := probeDir(t)
				l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
				brokerAt(t, l, broker.DefaultLimits())
				brokerAt(t, s, broker.DefaultLimits())
				lagForward(t, f, s, 200*time.Millisecond, 0)
				alice := dialSession(t, l, "alice", "alice", "pi", live)
				bob := dialSession(t, s, "bob", "bob", "pi", true)
				row := func() *broker.SessionInfo {
					return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
				}
				b := NewBridge(l, f, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				b.startCtx(ctx)
				t.Cleanup(b.Stop)
				b.tickOnce()
				waitFor(t, 5*time.Second, func() bool { r := row(); return r != nil && r.Live == live }, "laptop/alice mirrored")
				b.tickOnce()
				alice.call("bye", broker.Request{}, nil)
				b.tickOnce() // marks alice gone and nudges her worker: drainAndBye
				time.Sleep(off)
				var late broker.Message
				err := bob.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p53 late", ExpectsReply: true, NoWait: true}}, &late)
				time.Sleep(1500 * time.Millisecond)
				for i := 0; i < 4; i++ {
					b.tickOnce()
					time.Sleep(300 * time.Millisecond)
				}
				state := "gone"
				if r := row(); r != nil {
					state = fmt.Sprintf("kept, live=%v, queued=%d", r.Live, r.Queued)
				}
				b.mu.Lock()
				_, hasProxy := b.proxies[0]["alice"]
				b.mu.Unlock()
				tracked := slices.Contains(b.rows.snapshot(0), "alice")
				answer := "<none>"
				bob.mu.Lock()
				for _, x := range bob.msgs {
					if err == nil && x.ReplyTo == late.ID {
						answer = fmt.Sprintf("%q", x.Text)
					}
				}
				bob.mu.Unlock()
				t.Logf("P53 %s offset %v: ask err=%v; laptop/alice is %s, proxy=%v, tracked=%v; answer %s", kind, off, err, state, hasProxy, tracked, answer)
				if err == nil && state != "gone" && !hasProxy && !tracked && answer == "<none>" {
					lingering++
					t.Errorf("the gone path's bye kept the row (late mail) and dropped it from the set")
				}
			})
		}
		t.Logf("P53 %s: offsets that left an untracked row holding mail: %d of %d", kind, lingering, len(offs))
	}
}

// N9: the row set's file, directly. A corrupt file is moved aside to .bad
// and the set starts empty; adds and removes persist and survive a reload.
func TestBridgeRowSetFile(t *testing.T) {
	dir := probeDir(t)
	path := filepath.Join(dir, "m.map.rows")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	rs := loadRowSet(path, nil)
	if ids := rs.snapshot(0); len(ids) != 0 {
		t.Fatalf("corrupt row set loaded %d ids, want 0", len(ids))
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("corrupt row set not moved aside: %v", err)
	}
	if _, err := os.Stat(path + ".bad"); err != nil {
		t.Errorf("the .bad copy is missing: %v", err)
	}
	rs.add(1, "alice")
	rs2 := loadRowSet(path, nil) // persisted, then reloaded
	if !slices.Contains(rs2.snapshot(1), "alice") {
		t.Errorf("a reloaded row set lost alice")
	}
	rs2.remove(1, "alice")
	rs3 := loadRowSet(path, nil)
	if ids := rs3.snapshot(1); len(ids) != 0 {
		t.Errorf("a removed id survived the reload: %v", ids)
	}
}

// P54 from the 3c.1i review follow-up (silent-raven), kept as a regression
// test: a transient's whole life inside one tick's fetch-to-reap window. It
// fails without H1's stale-list guard and passes with it.
// P54: a transient's whole life inside one tick's fetch-to-reap window. The
// tick fetches the far list, then creates proxies for new sessions - one hello
// round trip each, on the tick - and only then reaps with that list. A
// departed sender's transient that dials, releases, drains and byes in between
// (its bye kept by late mail) is absent from the stale list, so the reap drops
// its id while the row is still there: untracked, the late ask unanswered.
// With H1's removes dropped and no guard this fails; with the guard it passes.
func TestBridgeProbeP54StaleListDropsAKeptRow(t *testing.T) {
	lost := 0
	offs := []time.Duration{1850 * time.Millisecond, 2000 * time.Millisecond, 2150 * time.Millisecond}
	for _, off := range offs {
		t.Run(fmt.Sprintf("offset_%dms", off.Milliseconds()), func(t *testing.T) {
			dir := probeDir(t)
			l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "lag.sock")
			brokerAt(t, l, broker.DefaultLimits())
			brokerAt(t, s, broker.DefaultLimits())
			lagForward(t, f, s, 300*time.Millisecond, 0)
			dave := dialSession(t, s, "dave", "dave", "pi", true)
			row := func() *broker.SessionInfo {
				return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
			}
			b := NewBridge(l, f, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			b.startCtx(ctx)
			t.Cleanup(b.Stop)
			b.tickOnce() // mirrors dave onto the laptop; nothing to mirror the other way yet
			// alice left before this bridge saw her go: her last words relay
			// through a transient (C2), driven here directly
			b.mu.Lock()
			b.departed[0]["alice"] = broker.SessionInfo{ID: "alice", Name: "alice", Harness: "pi"}
			b.mu.Unlock()
			// five new laptop sessions: the next tick creates five proxies,
			// 600 ms each, between its far fetch and its reap. Every tick
			// pings first (ctrlConn), so the far list is served at ~900 ms,
			// and the reap runs at ~4.2 s.
			for i := 0; i < 5; i++ {
				dialSession(t, l, fmt.Sprintf("f%d", i), fmt.Sprintf("f%d", i), "pi", true)
			}
			start := time.Now()
			tickDone := make(chan struct{})
			go func() { b.tickOnce(); close(tickDone) }()
			time.Sleep(800 * time.Millisecond) // the transient's hello lands (~1100 ms) after the far list is served
			trDone := make(chan struct{})
			var trErr error
			trackedWhileHeld := false
			go func() {
				defer close(trDone)
				tr, err := b.acquireTransient(0, "alice") // hello lands ~1100 ms
				if err != nil {
					trErr = err
					return
				}
				trackedWhileHeld = slices.Contains(b.rows.snapshot(0), "alice")
				b.releaseTransient(tr) // inbox ~1700 ms, bye ~2300 ms, back ~2600 ms
			}()
			time.Sleep(off - time.Since(start))
			var late broker.Message
			err := dave.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p54 late", ExpectsReply: true, NoWait: true}}, &late)
			<-trDone
			released := time.Since(start)
			trackedAfterRelease := slices.Contains(b.rows.snapshot(0), "alice")
			<-tickDone
			tickTook := time.Since(start)
			trackedAfterTick := slices.Contains(b.rows.snapshot(0), "alice")
			for i := 0; i < 3; i++ {
				b.tickOnce()
				time.Sleep(300 * time.Millisecond)
			}
			state := "gone"
			if r := row(); r != nil {
				state = fmt.Sprintf("kept, live=%v, queued=%d", r.Live, r.Queued)
			}
			tracked := slices.Contains(b.rows.snapshot(0), "alice")
			answer := "<none>"
			dave.mu.Lock()
			for _, x := range dave.msgs {
				if err == nil && x.ReplyTo == late.ID {
					answer = fmt.Sprintf("%q", x.Text)
				}
			}
			dave.mu.Unlock()
			t.Logf("P54 offset %v: transient err=%v, released at %v, tick took %v; tracked while held=%v, after the release=%v, after the tick=%v; ask err=%v; after 3 more ticks laptop/alice is %s, tracked=%v; answer %s",
				off, trErr, released.Round(10*time.Millisecond), tickTook.Round(10*time.Millisecond), trackedWhileHeld, trackedAfterRelease, trackedAfterTick, err, state, tracked, answer)
			if err == nil && state != "gone" && !tracked && answer == "<none>" {
				lost++
				t.Errorf("a row kept by late mail lost its id to a far list fetched before the transient's hello")
			}
		})
	}
	t.Logf("P54 offsets where a stale list dropped a kept row: %d of %d", lost, len(offs))
}

// P55 from the FREEZE-3c.1j review (silent-raven), kept as a regression
// test: the reap's bounce must carry the row's own from_name, not the
// prefix doubled (N8b).
// P55: N8's name. The reap's hello now carries the far row's own fields,
// but the far row's Name already has the prefix (proxyInfo writes
// pfx+Name) and dialTransient prepends it again. The broker stamps every
// message with the sender row's Name, so the reap's bounces reach their
// senders from "laptop/laptop/alice". bob's ask waits in the orphaned row;
// the restarted bridge reaps it; the bounce's from_name must be
// "laptop/alice".
func TestBridgeProbeP55ReapBounceFromName(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	mapPath := filepath.Join(dir, "m.map")
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b1 := NewBridge(l, s, "laptop/", "srv/", mapPath, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	b1.startCtx(ctx1)
	b1.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		r := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" })
		return r != nil && r.Live
	}, "laptop/alice live")
	before := findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" }).Name
	cancel1()
	b1.Stop()
	alice.call("bye", broker.Request{}, nil)
	ask := bob.send(broker.SendReq{To: "laptop/alice", Text: "p55 for alice", ExpectsReply: true, NoWait: true})
	b2 := NewBridge(l, s, "laptop/", "srv/", mapPath, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	b2.startCtx(ctx2)
	t.Cleanup(b2.Stop)
	b2.tickOnce()
	m := bob.waitMsg(func(x *broker.Message) bool { return x.ReplyTo == ask.ID }, "the reap's bounce")
	t.Logf("P55 the mirrored row's name was %q; the reap's bounce comes from %q (from_name %q): %q", before, m.From, m.FromName, m.Text)
	if m.FromName != before {
		t.Errorf("the reap's bounce carries from_name %q, want the row's own name %q", m.FromName, before)
	}
}

// P56 from the FREEZE-3c.1k review (silent-raven), kept as a regression
// test: the stamp delete must hold b.mu (K1) - a plain build turns an
// overlap with stampRow into a fatal, unrecoverable map-write crash.
// P56: N12's stamp delete runs outside b.mu. reapRows drops a missing row's
// id between two b.mu sections (the stale check, then the reaping delete),
// with rows.remove - a file write and a rename - in between, and the stamp
// delete sits in that gap. stampRow writes the same map under b.mu from every
// worker whose hello or bye returns. Here one goroutine stamps in a loop (the
// other workers) while the reap drops 50 missing rows. Under -race (make
// check) the detector reports the write-write race; in a plain build an
// overlap is "fatal error: concurrent map writes", which recover cannot catch.
func TestBridgeProbeP56StampDeleteOutsideTheLock(t *testing.T) {
	dir := probeDir(t)
	b := NewBridge(filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	for i := 0; i < 50; i++ {
		b.rows.add(0, fmt.Sprintf("gone%d", i))
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			b.stampRow(1, fmt.Sprintf("busy%d", n%8)) // another worker's hello or bye
		}
	}()
	b.reapRows(0, map[string]broker.SessionInfo{}, nil, 1) // every row missing, every stamp older
	close(stop)
	<-done
	if ids := b.rows.snapshot(0); len(ids) != 0 {
		t.Errorf("the reap kept %d of 50 missing ids", len(ids))
	}
}

// 3c.2a probes (kept from the FREEZE-3c.2a review cycle): P57 the -L half
// dying and coming back (tunnel loss end to end minus ssh), P59 the
// write-ahead row add, P60 the hold waiting out a returning sender's reap
// (N10), P61 the daemon capability gate.
// rebindForward is a plain forward whose listener can be stopped and started
// again on the same path: the ssh -L socket dying and coming back.
type rebindFwd struct {
	mu    sync.Mutex
	ln    net.Listener
	run   bool
	start func()
	stop  func()
}

func rebindForward(t *testing.T, path, target string) *rebindFwd {
	f := &rebindFwd{}
	start := func() {
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.ln, f.run = ln, true
		f.mu.Unlock()
		go f.accept(ln, target)
	}
	stop := func() {
		f.mu.Lock()
		ln, run := f.ln, f.run
		f.ln, f.run = nil, false
		f.mu.Unlock()
		if run {
			ln.Close()
		}
	}
	start()
	t.Cleanup(stop)
	f.start, f.stop = start, stop
	return f
}

func (f *rebindFwd) accept(ln net.Listener, target string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		d, err := net.Dial("unix", target)
		if err != nil {
			c.Close()
			continue
		}
		go pipe(c, d)
		go pipe(d, c)
	}
}

func pipe(a, b net.Conn) {
	buf := make([]byte, 16<<10)
	for {
		n, err := a.Read(buf)
		if n > 0 {
			if _, werr := b.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	a.Close()
	b.Close()
}

// P57: the -L half dying and coming back, end to end minus ssh. Both daemons
// stay up; only the forward socket goes away, so the far proxies survive past
// a short IdleTTL (Sweep keeps "/" rows until MailTTL), mail queued during
// the gap in both directions is relayed after the rebind, exactly once.
func TestBridgeProbeP57ForwardRebindTunnelLoss(t *testing.T) {
	lim := broker.DefaultLimits()
	lim.IdleTTL, lim.MailTTL = time.Second, time.Minute
	dir := probeDir(t)
	l, s, f := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "f.sock")
	brokerAt(t, l, lim)
	sb := brokerAt(t, s, lim)
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	fwd := rebindForward(t, f, s)
	b := NewBridge(l, f, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" && x.Live }) != nil &&
			findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/bob" && x.Live }) != nil
	}, "both sides mirrored")

	fwd.stop() // the tunnel dies: ssh dropped, the laptop slept
	time.Sleep(1200 * time.Millisecond)
	// past IdleTTL: the rows survive because of the "/" carve-out
	if gone := sb.Sweep(); len(gone) > 0 {
		t.Fatalf("the far daemon swept %v during the gap", gone)
	}
	if findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" }) == nil {
		t.Fatal("laptop/alice did not survive past IdleTTL while the tunnel was down")
	}
	carol := dialSession(t, l, "carol", "carol", "pi", true)
	carol.send(broker.SendReq{To: "srv/bob", Text: "p57 gap laptop"})
	dave := dialSession(t, s, "dave", "dave", "pi", true)
	dave.send(broker.SendReq{To: "laptop/alice", Text: "p57 gap server"})

	fwd.start() // the -L forward comes back on the same path
	for i := 0; i < 6; i++ {
		b.tickOnce()
		time.Sleep(300 * time.Millisecond)
	}
	waitFor(t, 10*time.Second, func() bool {
		return countText(bob, "p57 gap laptop") == 1 && countText(alice, "p57 gap server") == 1
	}, "gap mail relayed both ways after the rebind")
	if n := countText(bob, "p57 gap laptop"); n != 1 {
		t.Errorf("bob got the gap mail %d times, want 1", n)
	}
	if n := countText(alice, "p57 gap server"); n != 1 {
		t.Errorf("alice got the gap mail %d times, want 1", n)
	}
	for _, m := range []*bsession{alice, bob, carol, dave} {
		m.mu.Lock()
		for _, x := range m.msgs {
			if strings.Contains(x.Text, "not delivered") {
				t.Errorf("a gap message bounced: %s", x.Text)
			}
		}
		m.mu.Unlock()
	}
}

// firstFullForward passes the first connection through (the ctrl ping) and
// stalls every later one in both directions: the creation hello's answer
// never arrives.
func firstFullForward(t *testing.T, path, target string) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		n := 0
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n++
			if n == 1 {
				d, err := net.Dial("unix", target)
				if err != nil {
					c.Close()
					continue
				}
				go pipe(c, d)
				go pipe(d, c)
				continue
			}
			// stalled: accepted, never answered; closed only at cleanup by
			// the test process exit or the client side giving up
			go func() {
				buf := make([]byte, 512)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
}

// P59: the write-ahead row add. The far side answers the ctrl ping but stalls
// the creation hello's response, so the proxy never publishes - and the id is
// in the row set before the hello returns. After the bridge dies and the
// session ends, a restarted bridge still finishes the row (the stalled hello
// landed: the row exists) and the id leaves the set by observation.
func TestBridgeProbeP59WriteAheadRowAdd(t *testing.T) {
	dir := probeDir(t)
	l, s, g := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock"), filepath.Join(dir, "g.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	firstFullForward(t, g, s)
	b1 := NewBridge(l, g, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	b1.startCtx(ctx1)
	tickDone := make(chan struct{})
	go func() { b1.tickOnce(); close(tickDone) }()
	waitFor(t, 8*time.Second, func() bool {
		return b1.rows.has(0, "alice") // rs.mu only: probes must not model a b.mu order
	}, "the id tracked before the hello returns")
	select {
	case <-tickDone:
		t.Fatal("the tick finished while the hello was still stalled")
	default:
	}
	b1.Stop()
	cancel1()
	alice.call("bye", broker.Request{}, nil) // the session ends while no bridge runs
	b2 := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(cancel2)
	b2.startCtx(ctx2)
	t.Cleanup(b2.Stop)
	for i := 0; i < 4; i++ {
		b2.tickOnce()
		time.Sleep(200 * time.Millisecond)
	}
	waitFor(t, 8*time.Second, func() bool {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" }) == nil
	}, "the restarted bridge finished the stalled row")
	if tracked := b2.rows.has(0, "alice"); tracked { // rs.mu only (N15)
		t.Error("the id stayed in the row set after the row was observed gone")
	}
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	var m broker.Message
	err := bob.call("send", broker.Request{SendReq: broker.SendReq{To: "laptop/alice", Text: "p59 late", ExpectsReply: true, NoWait: true}}, &m)
	t.Logf("P59 write-ahead tracked the id mid-hello; the restarted bridge finished the row; a later ask: err=%v", err)
	if err == nil {
		t.Errorf("an ask to the finished row was accepted")
	}
}

// P60: the arrival hold. A returning sender's first message is held while
// its old row is still being reaped and its proxy cannot exist yet; the
// ordering - the reap ends, at least one hold poll passes with NO tick, the
// message is still held, then a tick delivers - pins that the hold waits on
// the unmet need, not on the reap alone. (N10's floor against a tick during
// the reap is P62d's.)
func TestBridgeProbeP60HoldWaitsOutAReap(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		return findSession(listOn(t, s), func(x broker.SessionInfo) bool { return x.ID == "laptop/alice" && x.Live }) != nil
	}, "alice mirrored")

	// Her session ends without the gone path running for it: the row stays
	// registered on the far daemon, tracked, with no proxy - the reaper's
	// case (a restart gap or a late-mail bye), reached here directly.
	alice.call("bye", broker.Request{}, nil)
	b.mu.Lock()
	p := b.proxies[0]["alice"]
	delete(b.proxies[0], "alice")
	b.mu.Unlock()
	p.stopWorker()
	time.Sleep(200 * time.Millisecond) // the far row settles to registered

	// A slow reap: drive finishRow directly on a lagged dial so b.reaping
	// stays set long enough to observe the hold.
	b.mu.Lock()
	b.reaping[transientKey(0, "alice")] = true
	b.mu.Unlock()
	reapDone := make(chan struct{})
	go func() {
		defer close(reapDone)
		b.finishRow(0, "alice", broker.SessionInfo{ID: "alice", Name: "alice", Harness: "pi"})
		b.mu.Lock()
		delete(b.reaping, transientKey(0, "alice"))
		b.mu.Unlock()
	}()

	// She returns and messages at once: bob's proxy holds her first message
	// because her proxy cannot be created while the reap runs.
	alice2 := dialSession(t, l, "alice", "alice", "pi", true)
	alice2.send(broker.SendReq{To: "srv/bob", Text: "p60 first"})
	select {
	case <-reapDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the reap never finished")
	}
	// The pinned ordering: at least one hold poll with NO tick after the
	// reap ended. The need set at arrival is still above listApplied, so
	// this poll waits - whatever the reset would do (P62d pins that).
	time.Sleep(400 * time.Millisecond)
	if n := countText(bob, "p60 first"); n != 0 {
		t.Fatalf("delivered before a tick: %d", n)
	}
	alice2.mu.Lock()
	for _, x := range alice2.msgs {
		if strings.Contains(x.Text, "not delivered") {
			t.Fatalf("the returning sender's first message bounced: %s", x.Text)
		}
	}
	alice2.mu.Unlock()
	b.tickOnce() // creation: the reap is done
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "p60 first") == 1 }, "held, then delivered")
}

// answerStub answers the protocol op per answer() (read per request, so a
// test can flip it under a live connection), an empty list and a hello; used
// by the capability probes.
func answerStub(t *testing.T, path string, answer func() (any, *broker.Error)) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				sc := bufio.NewScanner(c)
				sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
				for sc.Scan() {
					var req broker.Request
					if json.Unmarshal(sc.Bytes(), &req) != nil {
						continue
					}
					resp := broker.Response{ID: req.ID}
					switch req.Op {
					case "protocol":
						resp.Result, resp.Error = answer()
					case "list":
						resp.Result = []broker.SessionInfo{}
					case "hello":
						resp.Result = map[string]any{"id": ""}
					default:
						resp.Error = &broker.Error{Code: broker.CodeBadRequest, Message: "stub: not supported"}
					}
					data, _ := json.Marshal(resp)
					c.Write(append(data, '\n'))
				}
			}(c)
		}
	}()
}

// P61: the capability gate. A daemon whose protocol answer lacks bridge
// support - or predates the op and answers not_registered before hello - is
// up but not mirrored through, with one log line per change (a capable first
// observation is silent); a daemon that gains the field is picked up on the
// next ping without restarting the bridge.
func TestBridgeProbeP61CapabilityGate(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail bool
	}{
		{name: "field absent", fail: false},
		{name: "op refused", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := probeDir(t)
			l, stub := filepath.Join(dir, "l.sock"), filepath.Join(dir, "stub.sock")
			brokerAt(t, l, broker.DefaultLimits())
			upgraded, fail := 0, tc.fail
			answerStub(t, stub, func() (any, *broker.Error) {
				if fail && upgraded == 0 {
					return nil, &broker.Error{Code: broker.CodeNotRegistered, Message: "say hello first"}
				}
				r := map[string]any{"protocol": 2}
				if upgraded >= 1 {
					r["bridge"] = 1
				}
				return r, nil
			})
			dialSession(t, l, "alice", "alice", "pi", true)
			logs := &lockedStrings{}
			b := NewBridge(l, stub, "laptop/", "srv/", filepath.Join(dir, "m.map"), logs.logf)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			b.startCtx(ctx)
			t.Cleanup(b.Stop)
			b.tickOnce()
			b.mu.Lock()
			cap0, nprox := b.daemonCap[1], len(b.proxies[0])
			b.mu.Unlock()
			if cap0 != 0 {
				t.Fatalf("daemonCap[1] = %d, want 0", cap0)
			}
			if !logs.has("no bridge support") {
				t.Errorf("missing the refusal log; got %v", logs.all())
			}
			if nprox != 0 {
				t.Errorf("%d proxies created through an incapable daemon", nprox)
			}
			if logs.has("mirroring resumes") { // N17: a capable first observation is silent
				t.Errorf("spurious resume log before any refusal; got %v", logs.all())
			}
			// the upgrade: agm restart on the far host
			upgraded, fail = 1, false
			b.tickOnce()
			b.mu.Lock()
			cap1, nprox1 := b.daemonCap[1], len(b.proxies[0])
			b.mu.Unlock()
			if cap1 < 1 {
				t.Fatalf("daemonCap[1] = %d after the upgrade, want >= 1", cap1)
			}
			if !logs.has("mirroring resumes") {
				t.Errorf("missing the resume log; got %v", logs.all())
			}
			if nprox1 != 1 {
				t.Errorf("%d proxies after the upgrade, want 1 (mirroring resumed)", nprox1)
			}
		})
	}
}

// P63: a daemon from before the protocol op, on the bridge's anonymous ctrl
// connection. The dispatch answers an unknown op before hello with
// not_registered ("say hello first"), not bad_request; needProtocol accepts
// both as "older". The bridge must report it as lacking bridge support (the
// upgrade advice), not as a side that is down.
//
// P64: a newer daemon whose protocol answer gains a non-int field. The bridge
// decodes into a struct, so the extra field is ignored: the side is up, and
// mirroring runs.
func TestBridgeProbeP63P64ProtocolAnswers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer func() (any, *broker.Error)
		want   int // daemonCap[1] the bridge should hold
	}{
		{"P63 v0.1.x: not_registered before hello", func() (any, *broker.Error) {
			return nil, &broker.Error{Code: broker.CodeNotRegistered, Message: "say hello first"}
		}, 0},
		{"P64 a newer daemon: a string field next to bridge", func() (any, *broker.Error) {
			return map[string]any{"protocol": 2, "bridge": 1, "version": "v0.6.0"}, nil
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := probeDir(t)
			l, stub := filepath.Join(dir, "l.sock"), filepath.Join(dir, "stub.sock")
			brokerAt(t, l, broker.DefaultLimits())
			answerStub(t, stub, tc.answer)
			dialSession(t, l, "alice", "alice", "pi", true)
			logs := &lockedStrings{}
			b := NewBridge(l, stub, "laptop/", "srv/", filepath.Join(dir, "m.map"), logs.logf)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			b.startCtx(ctx)
			t.Cleanup(b.Stop)
			for i := 0; i < 3; i++ {
				b.tickOnce()
			}
			b.mu.Lock()
			got, nprox := b.daemonCap[1], len(b.proxies[0])
			b.mu.Unlock()
			t.Logf("%s: daemonCap[1]=%d, proxies=%d, logs=%q", tc.name, got, nprox, logs.all())
			if got != tc.want {
				t.Errorf("daemonCap[1] = %d, want %d", got, tc.want)
			}
		})
	}
}

func (l *lockedStrings) logf(format string, a ...any) {
	l.add(fmt.Sprintf(format, a...))
}

func (l *lockedStrings) has(sub string) bool {
	for _, s := range l.all() {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// P62d from the FREEZE-3c.2a review (silent-raven), kept as a regression
// test: N10's hold floor, deterministic with a 1 s hold poll. A tick during
// the reap skips the creation but advances listApplied; the floor keeps the
// hold waiting for a list that could have created the sender.
func TestBridgeProbeP62dDeterministic(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	bob := dialSession(t, s, "bob", "bob", "pi", true)
	b := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), nil)
	b.holdPoll = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		return findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/bob" }) != nil
	}, "srv/bob mirrored")
	key := transientKey(0, "alice")
	b.mu.Lock()
	b.reaping[key] = true
	b.mu.Unlock()
	alice := dialSession(t, l, "alice", "alice", "pi", true)
	alice.send(broker.SendReq{To: "srv/bob", Text: "p62d first"})
	time.Sleep(300 * time.Millisecond) // the first poll has seen the reap
	start := time.Now()
	b.tickOnce() // skips her creation, advances listApplied
	b.mu.Lock()
	delete(b.reaping, key)
	b.mu.Unlock()
	t.Logf("tick + reap end took %s", time.Since(start).Round(time.Millisecond))
	time.Sleep(1500 * time.Millisecond) // at least one poll, no tick
	alice.mu.Lock()
	for _, x := range alice.msgs {
		if strings.Contains(x.Text, "not delivered") {
			t.Errorf("bounced: %s", x.Text)
		}
	}
	alice.mu.Unlock()
	b.tickOnce()
	waitFor(t, 8*time.Second, func() bool { return countText(bob, "p62d first") == 1 }, "held, then delivered")
}

// P58: the NAME/ overlap, both directions, with the gate and the bridge in
// one process. (a) a gate hello for an id the bridge mirrors is refused;
// (b) the bridge does not mirror an id a live gate session claims, and does
// after the claim is released; (c) a pre-existing row the bridge does not
// track (an earlier process's gate session) is not mirrored over, while a
// tracked row (the resume case) is.
func TestBridgeProbeP58NameOverlap(t *testing.T) {
	dir := probeDir(t)
	l, s := filepath.Join(dir, "l.sock"), filepath.Join(dir, "s.sock")
	brokerAt(t, l, broker.DefaultLimits())
	brokerAt(t, s, broker.DefaultLimits())
	dialSession(t, s, "alice", "alice", "pi", true) // DEST side: mirrored here as srv/alice
	// bob dials only after the gate claims his id
	dialSession(t, s, "carol", "carol", "pi", true) // collides with a pre-existing row
	dialSession(t, s, "dave", "dave", "pi", true)   // tracked row: the resume case
	// a gate session from an earlier process, still connected: srv/carol is
	// live on the local daemon, and no running bridge tracks it
	carolGate := dialSession(t, l, "srv/carol", "srv/carol", "pi", true)
	// and an offline one with queued mail: srv/erin is registered, holding a
	// message nobody delivered, untracked
	erin := dialSession(t, l, "srv/erin", "srv/erin", "pi", false)
	erin.conn.close()
	dave2 := dialSession(t, l, "dave", "dave", "pi", true)
	dave2.send(broker.SendReq{To: "srv/erin", Text: "p58 for erin"})
	_ = carolGate
	logs := &lockedStrings{}
	b := NewBridge(l, s, "laptop/", "srv/", filepath.Join(dir, "m.map"), logs.logf)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.startCtx(ctx)
	t.Cleanup(b.Stop)
	b.rows.add(1, "dave") // a tracked row with no proxy: the resume case
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		return findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/alice" && x.Live }) != nil
	}, "srv/alice mirrored")

	// (a) the gate direction: a hello for a mirrored id is refused
	if _, _, rerr := checkLinkRequest("srv", helloReq("srv/alice", "srv/alice", "pi"), b, ""); rerr == nil || !strings.Contains(rerr.Message, "mirrored session") {
		t.Fatalf("gate hello for a mirrored id: %v", rerr)
	}
	// and for a merely tracked id (a restart gap)
	if _, _, rerr := checkLinkRequest("srv", helloReq("srv/dave", "srv/dave", "pi"), b, ""); rerr == nil || !strings.Contains(rerr.Message, "mirrored session") {
		t.Fatalf("gate hello for a tracked id: %v", rerr)
	}

	// (b) the live claim: the gate claims srv/bob before bob ever appears,
	// so bob is not mirrored while the claim stands
	if !b.gateClaim(1, "bob") {
		t.Fatal("the gate could not claim srv/bob")
	}
	dialSession(t, s, "bob", "bob", "pi", true)
	for i := 0; i < 2; i++ {
		b.tickOnce()
		time.Sleep(100 * time.Millisecond)
	}
	if findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/bob" }) != nil {
		t.Error("srv/bob was mirrored while a gate session held the id")
	}
	if !logs.has("held by a gate session") {
		t.Errorf("missing the overlap log; got %v", logs.all())
	}
	b.gateRelease(1, "bob")
	b.tickOnce()
	waitFor(t, 5*time.Second, func() bool {
		return findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/bob" }) != nil
	}, "srv/bob mirrored after the claim was released")

	// (c) a pre-existing row the bridge does not track: carol is skipped; the
	// tracked dave row is re-created (resume)
	for i := 0; i < 2; i++ {
		b.tickOnce()
		time.Sleep(100 * time.Millisecond)
	}
	b.mu.Lock()
	carolProxy := b.proxies[1]["carol"] != nil
	b.mu.Unlock()
	if carolProxy {
		t.Error("the bridge created a proxy over a live foreign subscriber's row")
	}
	if !logs.has("its mail is not ours to touch") {
		t.Errorf("missing the untracked-row log; got %v", logs.all())
	}
	// the offline untracked row keeps its queued mail: no drain, no bye
	if x := findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/erin" }); x == nil {
		t.Fatal("the untracked offline row srv/erin vanished")
	} else if x.Queued != 1 {
		t.Errorf("srv/erin holds %d queued, want 1 (its mail is not ours to drain)", x.Queued)
	}
	if x := findSession(listOn(t, l), func(x broker.SessionInfo) bool { return x.ID == "srv/dave" }); x == nil || !x.Live {
		t.Error("the tracked srv/dave row was not resumed")
	}
}

// helloReq builds a gate hello request.
func helloReq(id, name, harness string) *broker.Request {
	return &broker.Request{ID: 1, Op: "hello", Session: &broker.SessionInfo{ID: id, Name: name, Harness: harness}}
}

// gatePipe drives one real serveLinkConn over a net.Pipe against a real
// daemon (testBroker), for the gate-claim probes.
func gatePipe(t *testing.T, br *Bridge) (*linkRemote, <-chan struct{}) {
	t.Helper()
	peer, gate := net.Pipe()
	done := make(chan struct{})
	go func() {
		serveLinkConn(gate, "srv", br)
		close(done)
	}()
	sc := bufio.NewScanner(peer)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	r := &linkRemote{t: t, nc: peer, sc: sc}
	peer.SetReadDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() {
		peer.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("gate worker did not stop")
		}
	})
	return r, done
}

func gateClaimCount(br *Bridge) int {
	br.mu.Lock()
	defer br.mu.Unlock()
	return len(br.gateRows)
}

// P65 (review B2): a gate connection that hellos srv/alice, then tries to
// rebind to srv/bob, then closes, must leave no claim behind. The rebind is
// refused by the gate itself (one id per connection: the daemon would refuse
// it anyway, and a claim released before that refusal would leak).
func TestBridgeProbeP65GateRejectedRebindNoLeak(t *testing.T) {
	testBroker(t)
	br := NewBridge("unused-local", "unused-remote", "laptop/", "srv/", filepath.Join(t.TempDir(), "map"), nil)
	r, done := gatePipe(t, br)
	r.say(helloReq("srv/alice", "srv/alice", "pi"))
	r.ok(1)
	second := helloReq("srv/bob", "srv/bob", "pi")
	second.ID = 2
	r.say(second)
	f := r.frame()
	e, _ := f["error"].(map[string]any)
	if e == nil || !strings.Contains(e["message"].(string), "reconnect to change the id") {
		t.Fatalf("expected the gate rebind refusal, got %v", f)
	}
	r.nc.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("gate worker did not stop")
	}
	if n := gateClaimCount(br); n != 0 {
		t.Fatalf("closed gate connection leaves %d claim(s); expected 0", n)
	}
}

// P66 (review B3): an exclusive reconnect - a second connection hellos the
// same id subscribed, the broker closes the first - must keep the claim: the
// old connection's release races the new one's hello, so the claim counts
// holders instead of belonging to one connection.
func TestBridgeProbeP66GateReconnectKeepsClaim(t *testing.T) {
	testBroker(t)
	br := NewBridge("unused-local", "unused-remote", "laptop/", "srv/", filepath.Join(t.TempDir(), "map"), nil)
	first, firstDone := gatePipe(t, br)
	h := helloReq("srv/alice", "srv/alice", "pi")
	h.Subscribe, h.Wait = true, true
	first.say(h)
	first.ok(1)
	second, _ := gatePipe(t, br)
	second.say(h)
	second.ok(1)
	// The exclusive re-hello closes the first subscriber; wait for its actual
	// cleanup, not a timing guess, while the second stays live.
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old gate connection did not end on exclusive replacement")
	}
	if n := gateClaimCount(br); n != 1 {
		t.Fatalf("new gate connection is live but has %d claims; expected 1", n)
	}
}

// P67 (late review, same-ID refresh and rejected rebind on a live gate
// connection): repeated same-ID hellos must not add holders, and a refused
// rebind must leave the accepted identity claimed while the connection stays
// open - claims count connections, not hello requests.
func lateGatePipe(t *testing.T, br *Bridge) (*linkRemote, <-chan struct{}) {
	t.Helper()
	peer, gate := net.Pipe()
	done := make(chan struct{})
	go func() {
		serveLinkConn(gate, "srv", br)
		close(done)
	}()
	sc := bufio.NewScanner(peer)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	peer.SetReadDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() {
		peer.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("gate worker did not stop")
		}
	})
	return &linkRemote{t: t, nc: peer, sc: sc}, done
}

func TestBridgeProbeP67RepeatedHelloDoesNotLeakHolder(t *testing.T) {
	testBroker(t)
	br := NewBridge(
		"unused-local", "unused-remote", "laptop/", "srv/",
		filepath.Join(t.TempDir(), "map"), nil,
	)
	r, done := lateGatePipe(t, br)
	h := helloReq("srv/alice", "srv/alice", "pi")
	r.say(h)
	r.ok(1)
	h.ID = 2
	r.say(h)
	r.ok(2)
	r.nc.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("gate did not stop")
	}
	br.mu.Lock()
	n := br.gateRows[transientKey(1, "alice")]
	br.mu.Unlock()
	if n != 0 {
		t.Fatalf("one connection, two accepted same-ID hellos, then disconnect leaves %d holders; expected 0", n)
	}
}

func TestBridgeProbeP67RejectedRebindRetainsAcceptedIdentity(t *testing.T) {
	testBroker(t)
	br := NewBridge(
		"unused-local", "unused-remote", "laptop/", "srv/",
		filepath.Join(t.TempDir(), "map"), nil,
	)
	r, _ := lateGatePipe(t, br)
	r.say(helloReq("srv/alice", "srv/alice", "pi"))
	r.ok(1)
	h := helloReq("srv/bob", "srv/bob", "pi")
	h.ID = 2
	r.say(h)
	f := r.frame()
	e, _ := f["error"].(map[string]any)
	// The refusal now comes from the gate itself (one id per connection):
	// same invariant the daemon enforces, but before any claim moves.
	if e == nil || !strings.Contains(e["message"].(string), "reconnect to change the id") {
		t.Fatalf("expected the gate bound-identity refusal, got %v", f)
	}
	// Connection stays bound to alice, and remains open after rejecting bob.
	br.mu.Lock()
	alice := br.gateRows[transientKey(1, "alice")]
	bob := br.gateRows[transientKey(1, "bob")]
	br.mu.Unlock()
	if alice != 1 || bob != 0 {
		t.Fatalf("daemon rejected rebind, still connected as alice: holders alice=%d bob=%d; expected 1,0", alice, bob)
	}
}
