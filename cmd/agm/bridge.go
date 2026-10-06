package main

// The bridge (3c, level 2): every host runs its own daemon, and a bridge
// between two daemons mirrors each side's sessions as proxies on the other and
// relays their mail. It is a plain client of both daemons: no wire-protocol
// change, no broker changes beyond the waker skip and the Sweep carve-out.
//
// Identity model: sessions on daemon i whose id contains no "/" are mirrored
// on daemon 1-i as pfx[i]+id (pfx[0] is how daemon-0 sessions are named on
// daemon 1, and vice versa). Gate sessions (NAME/...) and existing proxies
// already contain "/", so there is no proxy of a proxy and no loop.
//
// Lifecycle is level-triggered: every tick fetches both session lists and
// reconciles - creates missing proxies, reopens dead or swept ones, follows
// live transitions, refreshes old offline rows, drains offline mailboxes, and
// byes proxies of sessions that left. Delivery is at-least-once: a relay acks
// on the source daemon only after the send on the target daemon succeeded and
// the id pair is recorded in memory. A crash between the far send and the
// persist may resend once after a restart; a reply_to whose pair was rotated
// out of the map is resent without reply_to (like a local reply_to the daemon
// forgot), which also restarts the hop chain at 0 - hop is derived from
// reply_to on each daemon and is never a request field.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

const (
	bridgeMirrorCap = 64   // mirrored sessions per direction
	bridgeMapCap    = 8192 // id pairs kept, 2x the broker's seen window
	bridgeQueueCap  = 1024 // pending relay messages per proxy (mailbox cap is 256)
	refreshAfter    = time.Hour
	retryBackoff    = 2 * time.Second
	unmirrorWait    = 150 * time.Millisecond // a not-yet-mirrored sender: poll cadence
	cwdCap          = 4096                   // the far list's cwd is peer data; cap it
	retireSlack     = 250 * time.Millisecond // retire grace beyond the newest call deadline
	departedCap     = 256                    // remembered byed senders (C2)
)

// bridgeTransient is the error-code set retried in place; everything else the
// daemon answers is permanent and bounces. Transport errors (the conn died,
// a call timed out) are retried too: they are not broker.Errors.
func bridgeTransient(code string) bool {
	switch code {
	case broker.CodeRateLimited, broker.CodeMailboxFull, broker.CodeTooManyAsks:
		return true
	}
	return false
}

// ---------------------------------------------------------------- connections

// countReader records when bytes last arrived, so a call waiting on a slow
// link can tell "the answer is coming" (bytes flowing) from "the connection
// is dead" (silence). The daemon writes in order: while bytes arrive, the
// answer is behind them.
type countReader struct {
	r    io.Reader
	mu   sync.Mutex
	last time.Time
}

func (cr *countReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.mu.Lock()
		cr.last = time.Now()
		cr.mu.Unlock()
	}
	return n, err
}

// bframe is one line from a daemon: a response for a pending call, or a pushed
// message event.
type bframe struct {
	ID      int64           `json:"id"`
	Event   string          `json:"event,omitempty"`
	Message *broker.Message `json:"message,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *broker.Error   `json:"error,omitempty"`
}

// bconn is one framed daemon connection. A reader goroutine routes frames:
// responses to waiting callers, message events to the proxy's queue, so calls
// and pushes share one socket. Every call has a deadline: a wedged daemon or a
// half-open tunnel must not stop the tick; on timeout the connection is closed
// so the next reconcile reopens it. A connection that is replaced or byed is
// retired, not closed: a tunnel still delivers what it accepted before the
// close, so an in-flight call must get its answer - closing under it re-sends
// and duplicates the relay (P22).
type bconn struct {
	nc          net.Conn
	wmu         sync.Mutex // serializes writes
	sc          *bufio.Scanner
	rmu         sync.Mutex // guards waiters/next/retired/lastDL
	waiters     map[int64]*bwaiter
	next        int64
	retired     bool                  // no new calls; closes once the last waiter is done
	lastDL      time.Time             // the newest armed call deadline
	outstanding int                   // encoded bytes of calls written but unanswered (C3)
	progressDL  time.Duration         // silence that means death (0: no progress rule)
	cr          *countReader          // when bytes last arrived (D2)
	onPush      func(*broker.Message) // nil on anonymous conns
	dead        chan struct{}
	once        sync.Once
}

// bwaiter is one pending call: its answer channel and its encoded size (the
// size returns to outstanding when the answer arrives).
type bwaiter struct {
	ch chan *bframe
	n  int
}

// dialBConn is a plain dial, never an auto-start: a daemon started on a
// forwarded path would defeat the bridge's whole point (3c, far endpoint).
func dialBConn(sock string, onPush func(*broker.Message)) (*bconn, error) {
	nc, err := net.Dial("unix", sock)
	if err != nil {
		return nil, err
	}
	cr := &countReader{r: nc, last: time.Now()}
	sc := bufio.NewScanner(cr)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	c := &bconn{nc: nc, sc: sc, cr: cr, waiters: map[int64]*bwaiter{}, onPush: onPush, dead: make(chan struct{})}
	go c.readLoop()
	return c, nil
}

// idleFor reports how long the connection has received nothing.
func (c *bconn) idleFor() time.Duration {
	c.cr.mu.Lock()
	defer c.cr.mu.Unlock()
	return time.Since(c.cr.last)
}

func (c *bconn) readLoop() {
	for c.sc.Scan() {
		var f bframe
		if json.Unmarshal(c.sc.Bytes(), &f) != nil {
			continue // the daemon never sends junk; ignore anyway
		}
		if f.Event == "message" && f.Message != nil {
			if c.onPush != nil {
				c.onPush(f.Message)
			}
			continue
		}
		c.rmu.Lock()
		w, ok := c.waiters[f.ID]
		c.rmu.Unlock()
		if ok {
			w.ch <- &f
			c.dropWaiter(f.ID)
		}
	}
	c.once.Do(func() { close(c.dead) })
	c.nc.Close()
}

func (c *bconn) close() {
	c.once.Do(func() { close(c.dead) })
	c.nc.Close()
}

// retire stops the connection gracefully: no new calls, and the socket stays
// open until the last in-flight call got its answer (a tunnel delivers what it
// accepted even after a close, so the caller must see the reply, not a
// transport error it would retry into a duplicate). The grace is bounded by
// the newest armed call deadline plus slack.
func (c *bconn) retire() {
	c.rmu.Lock()
	if c.retired {
		c.rmu.Unlock()
		return
	}
	c.retired = true
	empty := len(c.waiters) == 0
	grace := time.Until(c.lastDL) + retireSlack
	c.rmu.Unlock()
	if empty {
		c.close()
		return
	}
	if grace < retireSlack {
		grace = retireSlack
	}
	go func() {
		t := time.NewTimer(grace)
		defer t.Stop()
		for {
			select {
			case <-c.dead: // the last waiter finished and closed us
				return
			case <-t.C:
				if c.progressDL > 0 && c.idleFor() < c.progressDL {
					t.Reset(c.progressDL - c.idleFor()) // still draining
					continue
				}
				c.close() // silent past every armed deadline
				return
			}
		}
	}()
}

// dropWaiter removes a finished call, returns its bytes to outstanding, and
// closes a retired connection whose last waiter just left.
func (c *bconn) dropWaiter(id int64) {
	c.rmu.Lock()
	if w := c.waiters[id]; w != nil {
		delete(c.waiters, id)
		c.outstanding -= w.n
	}
	shut := c.retired && len(c.waiters) == 0
	c.rmu.Unlock()
	if shut {
		c.close()
	}
}

func (c *bconn) isDead() bool {
	select {
	case <-c.dead:
		return true
	default:
		return false
	}
}

// encode renders a request without HTML escaping: the source daemon accepted
// the text, so `<`, `>` and `&` must not grow sixfold across our own frame
// limit (the gate's lesson from FREEZE-3a).
func encode(req broker.Request) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(req); err != nil { // Encode appends '\n'
		return nil, err
	}
	return buf.Bytes(), nil
}

// fixedDL is a call budget that ignores the frame size (the ping, and the
// answer-heavy list/inbox ops, whose cost is in the response).
func fixedDL(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}

// callDL performs one call. The budget closure sees the bytes the call must
// wait for and returns the request-side deadline: twice its own encoded
// request plus twice the unanswered bytes already queued ahead of it on the
// connection (calls serialize on one socket, and a relay's payload crosses
// twice - request in, stored-message response out - so both sides count,
// D3). But that deadline alone is not what closes the call: when it expires,
// the call only times out if the connection has been SILENT for progressDL
// (D2) - on a slow downlink an answer queues behind pushes and mailbox
// replays, and while bytes still arrive, the answer is coming. Counting
// silence, not frames, means a single full frame at the floor is waited out.
func (c *bconn) callDL(ctx context.Context, req broker.Request, out any, budget func(int) time.Duration) error {
	c.rmu.Lock()
	c.next++
	req.ID = c.next
	id := req.ID
	c.rmu.Unlock()
	data, err := encode(req)
	if err != nil {
		return err
	}
	if len(data) >= broker.MaxFrame {
		return &broker.Error{Code: broker.CodeTooLarge, Message: fmt.Sprintf("request is %d bytes encoded, limit %d", len(data), broker.MaxFrame)}
	}
	ch := make(chan *bframe, 1)
	c.rmu.Lock()
	if c.retired { // replaced or byed: callers re-resolve to the new conn
		c.rmu.Unlock()
		return fmt.Errorf("transport: connection retired")
	}
	ahead := c.outstanding
	timeout := budget(2 * (ahead + len(data)))
	c.waiters[id] = &bwaiter{ch: ch, n: len(data)}
	c.outstanding += len(data)
	if dl := time.Now().Add(timeout); dl.After(c.lastDL) {
		c.lastDL = dl
	}
	c.rmu.Unlock()
	c.wmu.Lock()
	_, werr := c.nc.Write(data)
	c.wmu.Unlock()
	if werr != nil {
		c.dropWaiter(id)
		return fmt.Errorf("transport: %w", werr)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case f := <-ch:
			if f.Error != nil {
				return f.Error
			}
			if out == nil || len(f.Result) == 0 {
				return nil
			}
			return json.Unmarshal(f.Result, out)
		case <-c.dead:
			c.dropWaiter(id)
			return fmt.Errorf("transport: connection closed")
		case <-timer.C:
			idle := c.idleFor()
			if c.progressDL > 0 && idle < c.progressDL {
				// the budget ran out, but bytes are still arriving: the
				// answer (and the pushes ahead of it) is coming
				timer.Reset(c.progressDL - idle)
				continue
			}
			c.close() // so the reconcile reopens this proxy
			return fmt.Errorf("transport: no answer within %s (silent for %s)", timeout, idle)
		case <-ctx.Done():
			c.dropWaiter(id)
			return ctx.Err()
		}
	}
}

// ------------------------------------------------------------------- id map

// bridgeMap persists relayed id pairs so replies and threads survive a bridge
// restart. A is the id on daemon 0, B on daemon 1, regardless of direction.
type bridgeMap struct {
	path    string
	logf    func(string, ...any)
	mu      sync.Mutex
	persist sync.Mutex // held across snapshot, write and rename
	pairs   [][2]string
	byKey   map[string]string // "0:"+id or "1:"+id → the other side's id
}

func bridgeMapKey(side int, id string) string { return fmt.Sprintf("%d:%s", side, id) }

func loadBridgeMap(path string, logf func(string, ...any)) *bridgeMap {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	m := &bridgeMap{path: path, logf: logf, byKey: map[string]string{}}
	b, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	var pairs [][2]string
	if jerr := json.Unmarshal(b, &pairs); jerr != nil {
		// A corrupt map must not silently become an empty one: move it aside
		// and say so. Dedupe is lost, but the operator can see why.
		os.Rename(path, path+".bad")
		logf("bridge: id map %s is corrupt (%v); moved aside, starting empty", path, jerr)
		return m
	}
	m.mu.Lock()
	for _, p := range pairs {
		m.addLocked(p)
	}
	m.mu.Unlock()
	return m
}

func (m *bridgeMap) addLocked(p [2]string) {
	m.pairs = append(m.pairs, p)
	m.byKey[bridgeMapKey(0, p[0])] = p[1]
	m.byKey[bridgeMapKey(1, p[1])] = p[0]
	if len(m.pairs) > bridgeMapCap { // FIFO: the oldest pair leaves the window
		old := m.pairs[0]
		m.pairs = m.pairs[1:]
		delete(m.byKey, bridgeMapKey(0, old[0]))
		delete(m.byKey, bridgeMapKey(1, old[1]))
	}
}

// record remembers the pair and persists it. A persist failure is logged, not
// returned: the in-memory pair still dedupes, and a restart may resend once -
// the documented window. The persist lock is held across snapshot, write and
// rename so concurrent recorders cannot interleave or clobber the tmp file.
func (m *bridgeMap) record(p [2]string) {
	m.mu.Lock()
	m.addLocked(p)
	m.mu.Unlock()
	m.persist.Lock()
	defer m.persist.Unlock()
	m.mu.Lock()
	pairs := append([][2]string(nil), m.pairs...)
	m.mu.Unlock()
	data, err := json.Marshal(pairs)
	if err != nil {
		m.logf("bridge: id map marshal: %v", err)
		return
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		m.logf("bridge: id map persist: %v", err)
		return
	}
	if err := os.Rename(tmp, m.path); err != nil {
		m.logf("bridge: id map persist: %v", err)
	}
}

func (m *bridgeMap) get(side int, id string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.byKey[bridgeMapKey(side, id)]
	return o, ok
}

func (m *bridgeMap) has(side int, id string) bool {
	_, ok := m.get(side, id)
	return ok
}

// rowSet persists the far row ids this bridge registered - proxies and
// transients - so a restarted bridge can finish the rows of sessions that
// ended while it was away (G2): answer for their mail, then bye them away.
// The prefix alone is not ownership (gate sessions share NAME/), and m.map
// holds only message-id pairs, so this is a separate set.
type rowSet struct {
	path    string
	logf    func(string, ...any)
	mu      sync.Mutex
	persist sync.Mutex // held across marshal, write and rename
	sides   [2]map[string]bool
}

func rowsPath(mapPath string) string {
	return strings.TrimSuffix(mapPath, ".map") + ".rows"
}

func loadRowSet(path string, logf func(string, ...any)) *rowSet {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rs := &rowSet{path: path, logf: logf}
	rs.sides[0] = map[string]bool{}
	rs.sides[1] = map[string]bool{}
	var sides [2][]string
	if b, err := os.ReadFile(path); err == nil {
		if jerr := json.Unmarshal(b, &sides); jerr != nil {
			os.Rename(path, path+".bad")
			logf("bridge: row set %s is corrupt (%v); moved aside, starting empty", path, jerr)
			sides = [2][]string{}
		}
	}
	rs.mu.Lock()
	for side := 0; side < 2; side++ {
		for _, id := range sides[side] {
			rs.sides[side][id] = true
		}
	}
	rs.mu.Unlock()
	return rs
}

func (rs *rowSet) writeLocked() {
	rs.persist.Lock()
	defer rs.persist.Unlock()
	sides := [2][]string{}
	for side := 0; side < 2; side++ {
		for id := range rs.sides[side] {
			sides[side] = append(sides[side], id)
		}
	}
	data, err := json.Marshal(sides)
	if err != nil {
		rs.logf("bridge: row set marshal: %v", err)
		return
	}
	tmp := rs.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		rs.logf("bridge: row set persist: %v", err)
		return
	}
	if err := os.Rename(tmp, rs.path); err != nil {
		rs.logf("bridge: row set persist: %v", err)
	}
}

func (rs *rowSet) add(side int, id string) {
	rs.mu.Lock()
	if !rs.sides[side][id] {
		rs.sides[side][id] = true
		rs.writeLocked()
	}
	rs.mu.Unlock()
}

func (rs *rowSet) remove(side int, id string) {
	rs.mu.Lock()
	if rs.sides[side][id] {
		delete(rs.sides[side], id)
		rs.writeLocked()
	}
	rs.mu.Unlock()
}

func (rs *rowSet) snapshot(side int) []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]string, 0, len(rs.sides[side]))
	for id := range rs.sides[side] {
		out = append(out, id)
	}
	return out
}

// -------------------------------------------------------------------- proxy

// bproxy mirrors one real session (on daemon home) as a proxy on the other
// daemon. All mail to the proxy is relayed through one serial queue, in order.
// The worker also owns the offline drains and, when the real session is gone,
// the final drain and the bye - network work stays off the tick goroutine, and
// the bye happens exactly when the last relay finished, not on a timer guess.
type bproxy struct {
	b      *Bridge
	home   int    // the daemon the real session lives on
	realID string // its id there
	info   broker.SessionInfo

	mu           sync.Mutex
	live         bool   // real session live → subscribed conn; else registered
	conn         *bconn // the proxy connection on the other daemon
	gone         bool   // real session left its daemon: drain, bounce, bye
	foreign      bool   // someone else subscribes the proxy row (logged once)
	removed      bool   // the worker byed this proxy; the tick must skip it
	retiring     *bconn // a retired subscribed conn still draining (C6)
	reopenFailed bool   // gone-branch reopen failed (logged once per outage)
	pending      int    // enqueued-but-not-yet-relayed messages (B1: channel
	// state lies - Go hands a send on an empty channel to
	// the waiting receiver, so len(queue) is 0 at once)
	done      chan struct{}
	closeOnce sync.Once
	queue     chan *broker.Message
	drainCh   chan struct{} // the tick nudges the worker to drain (buffered 1)
}

// enqueue counts a message as pending before it enters the queue, so an idle
// worker that receives it straight off the channel still shows work in
// flight. Dropping it on a full queue undoes the count.
func (p *bproxy) enqueue(m *broker.Message) {
	p.mu.Lock()
	p.pending++
	p.mu.Unlock()
	select {
	case p.queue <- m:
	default:
		p.mu.Lock()
		p.pending--
		p.mu.Unlock()
		p.b.logf("bridge: proxy %s relay queue full; mail stays queued", p.b.proxyID(p))
	}
}

func (p *bproxy) dropPending() {
	p.mu.Lock()
	p.pending--
	p.mu.Unlock()
}

func (p *bproxy) pendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}

func (p *bproxy) isGone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gone
}

func (p *bproxy) isRemoved() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.removed
}

func (p *bproxy) retiringConn() *bconn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retiring
}

// nudge asks the worker to run a drain (offline mail) or, when gone, the
// final drain-and-bye. Buffered 1: repeated nudges coalesce; a drain racing a
// relay simply runs after it.
func (p *bproxy) nudge() {
	select {
	case p.drainCh <- struct{}{}:
	default:
	}
}

func (b *Bridge) proxyID(p *bproxy) string { return b.pfx[p.home] + p.realID }

// proxyInfo builds the session info for the proxy row: real harness and cwd,
// prefixed name, no pid, pane, parent or depth, and a capped cwd (the far
// list's cwd is peer data; without a cap 64 sessions could park ~1 MiB each
// in the near daemon's state). p.info is snapshotted under p.mu: the relay
// worker reaches this through ackOn's re-hello fallback while the tick
// writes it (P18's race).
func (b *Bridge) proxyInfo(p *bproxy) broker.SessionInfo {
	p.mu.Lock()
	info := p.info
	p.mu.Unlock()
	cwd := capCwd(info.Cwd)
	return broker.SessionInfo{
		ID:      b.proxyID(p),
		Name:    b.pfx[p.home] + info.Name,
		Harness: info.Harness,
		Cwd:     cwd,
	}
}

// openConn dials the proxy's connection on the far daemon and says hello,
// replacing any previous connection - and retiring it: a hard close under an
// in-flight relay re-sends the frame the tunnel is still carrying and
// duplicates it (P22), and a leaked replacement exhausts the fds (P14).
func (p *bproxy) openConn(subscribe bool) error {
	far := 1 - p.home
	conn, err := dialBConn(p.b.sockets[far], p.enqueue)
	if err != nil {
		return err
	}
	conn.progressDL = p.b.baseDL
	info := p.b.proxyInfo(p)
	connErr := conn.callDL(p.b.ctx, broker.Request{Op: "hello", Session: &info, Subscribe: subscribe}, nil, p.b.reqBudget())
	p.b.stampRow(p.home, p.realID) // whatever it said: only a later list may drop the id (H1)
	if connErr != nil {
		conn.close()
		return connErr
	}
	p.mu.Lock()
	if p.removed { // the worker byed this proxy while we dialed: nobody
		// would ever track this connection (C4). The hello already created
		// the far row, so bye it - a close would leave the row orphaned.
		p.mu.Unlock()
		conn.callDL(p.b.ctx, broker.Request{Op: "bye"}, nil, p.b.reqBudget())
		p.b.stampRow(p.home, p.realID)
		conn.retire()
		return fmt.Errorf("proxy removed")
	}
	old := p.conn
	wasSub := p.live
	p.conn = conn
	p.live = subscribe
	p.mu.Unlock()
	if old != nil && old != conn {
		old.retire()
		if wasSub {
			// the retired subscription keeps the far row live until it drains;
			// reconciling against that stale row misreads a foreign subscriber
			// (C6)
			p.mu.Lock()
			p.retiring = old
			p.mu.Unlock()
		}
	}
	return nil
}

// rehello pushes changed fields (name, harness, cwd) on the existing
// connection. Never with subscribe: a hello with subscribe replays the whole
// mailbox, and the map would only dedupe it after the fact.
func (p *bproxy) rehello() error {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn == nil {
		return nil
	}
	info := p.b.proxyInfo(p)
	return conn.callDL(p.b.ctx, broker.Request{Op: "hello", Session: &info}, nil, p.b.reqBudget())
}

func (p *bproxy) connSnapshot() *bconn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn
}

// transition switches the subscription state: for offline-to-live the new
// subscribed connection opens FIRST (its hello replays the mailbox, so no
// mail is missed); for live-to-offline the old one closes only after the
// registered replacement exists (openConn closes the replaced connection).
func (p *bproxy) transition(live bool) {
	if p.connSnapshot() == nil {
		return
	}
	p.mu.Lock()
	cur := p.live
	p.mu.Unlock()
	if cur == live {
		return
	}
	if err := p.openConn(live); err != nil {
		p.b.logf("bridge: proxy %s: %v", p.b.proxyID(p), err)
		return
	}
}

// relay is the proxy's single serial relay path: one message at a time, in
// order, retried in place until it lands or the proxy is byed. It also runs
// the offline drains the tick nudges, and when the real session is gone it
// finishes the mail (relays bounce back to the senders) and byes the proxy.
func (p *bproxy) relay() {
	for {
		select {
		case <-p.b.ctx.Done():
			return
		case <-p.done:
			return
		case m := <-p.queue:
			p.b.relayOne(p, m)
			p.dropPending()
		case <-p.drainCh:
			p.runDrain()
		}
		// The bye attempt only follows a wake-up (a relay finishing, or the
		// tick's nudge): a persistently failing inbox must not turn this loop
		// into a spin.
		if p.isGone() && p.pendingCount() == 0 {
			if p.drainAndBye() {
				return
			}
		}
	}
}

// runDrain pulls an offline proxy's queued mail into the relay path (M2), so
// it reaches the real session's mailbox and a leaving session does not take
// it into the void. On the worker, not the tick: a full-frame batch over a
// slow link must not stall mirroring for every other proxy.
func (p *bproxy) runDrain() {
	if p.isGone() {
		return // the bye path does its own final drain
	}
	conn := p.connSnapshot()
	if conn == nil || conn.isDead() {
		return
	}
	var msgs []*broker.Message
	if err := conn.callDL(p.b.ctx, broker.Request{Op: "inbox"}, &msgs, fixedDL(p.b.respDL())); err == nil {
		for _, m := range msgs {
			p.enqueue(m) // relayOne dedupes on the id map
		}
	}
}

// drainAndBye is the gone path: one last drain (mail that reached the kept
// row after the previous drain would otherwise die with it), let the worker
// chew through it - relays to a gone session bounce back to the senders -
// then bye and remove the proxy. Returns true when the proxy is gone for
// good and the worker should exit.
func (p *bproxy) drainAndBye() bool {
	b := p.b
	conn := p.connSnapshot()
	if conn == nil || conn.isDead() {
		// The far side is unreachable (an outage, a half-open tunnel): KEEP the
		// proxy. Removing it here would strand the far row's mail until
		// MailTTL with no sender ever told (P24); when the side answers again
		// the tick's gone branch reopens the connection and the next nudge
		// finishes the drain, the bounces and the bye. No spin: attempts only
		// follow wake-ups.
		return false
	}
	var msgs []*broker.Message
	if err := conn.callDL(b.ctx, broker.Request{Op: "inbox"}, &msgs, fixedDL(b.respDL())); err != nil {
		return false // transient (the tick nudges again)
	}
	for _, m := range msgs {
		p.enqueue(m)
	}
	if p.pendingCount() > 0 {
		return false // the queued mail bounces; the bye follows when it is done
	}
	p.mu.Lock()
	gone := p.gone
	p.mu.Unlock()
	if !gone {
		return false // the session came back while we finished (C5): keep it
	}
	conn.callDL(b.ctx, broker.Request{Op: "bye"}, nil, b.reqBudget()) // bye closes the caller's sub
	b.stampRow(p.home, p.realID)                                      // the id leaves the set only by observation (H1)
	conn.retire()                                                     // in-flight sends from other workers may still ride it
	b.removeProxy(p)
	return true
}

// ------------------------------------------------------------------- bridge

type Bridge struct {
	sockets [2]string // daemon sockets: 0 = local, 1 = remote
	pfx     [2]string // pfx[i]: prefix for daemon-i sessions on daemon 1-i
	mapPath string
	logf    func(string, ...any)
	tick    time.Duration

	// tests: called after a successful far send, after the map write, before
	// the near ack (the crash window).
	testHookPostSend func()

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	ctrl       [2]*bconn             // anonymous list connections
	proxies    [2]map[string]*bproxy // [home][realID]
	skipped    [2]int                // last logged skipped-by-cap count
	sideUp     [2]bool               // last known side health
	sideLogged [2]bool               // each side's initial state is logged once
	misses     [2]int                // consecutive failed side fetches
	// A1's two counters: started[i] bumps when a list fetch of side i begins,
	// applied[i] reaches that fetch's number only once the list has been
	// mirrored with creation enabled. A relay from a not-yet-mirrored sender
	// holds until a fetch that STARTED after the message arrived has been
	// applied; before that, "unmirrored" just means "the tick has not caught
	// up", which over a real tunnel is seconds, not microseconds.
	listStarted [2]int
	listApplied [2]int
	idmap       *bridgeMap

	// departed remembers recently byed proxies, with their info, so mail a
	// session sent just before leaving still relays under its identity
	// through a transient registered connection (C2). FIFO-bounded.
	departed     [2]map[string]broker.SessionInfo
	departedFIFO [2][]string

	// transients are the shared registered connections for departed senders
	// (D4): one per id, reference-counted, so two relays of the same
	// departed sender share a row instead of bying it out from under each
	// other.
	transients map[string]*btransient

	// daemonCap is each daemon's bridge capability, read from the protocol
	// answer on every ctrl ping. A daemon without it (an upgraded binary
	// nobody restarted, or v0.4.x) sweeps "/" rows at IdleTTL and wakes "/"
	// ids locally, so mirroring halts on that side until `agm restart` brings
	// the new code up; the ping keeps retrying.
	daemonCap [2]int

	// rows is the persisted set of far rows this bridge registered (G2), and
	// reaping marks rows a reap currently finishes, so ticks do not double-
	// spawn their cleanup. rowStamp records, per tracked id, the far fetch
	// counter at its last returned hello or bye: an id may leave the set only
	// by a far list fetched after that (H1), never by trusting a bye.
	rows     *rowSet
	reaping  map[string]bool
	rowStamp map[string]int
	// holdFloor: a tick that skipped an id's creation because its old row
	// was mid-reap; that sender's held mail may bounce only on a later list.
	holdFloor map[string]int

	// call budgets (fields so the slow-link tests can shrink them): the ping
	// is the side health check; baseDL covers small ops; chunkDL is added per
	// 16 KiB of payload, so a big relay over a slow link is not re-sent while
	// still in transit (the tunnel delivers what it accepted even after the
	// client closed - a re-send duplicates).
	pingDL   time.Duration
	holdPoll time.Duration // the hold's poll cadence; tests lengthen it
	baseDL   time.Duration
	chunkDL  time.Duration
}

func NewBridge(sock0, sock1, pfx0, pfx1, mapPath string, logf func(string, ...any)) *Bridge {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	b := &Bridge{
		sockets:  [2]string{sock0, sock1},
		pfx:      [2]string{pfx0, pfx1},
		mapPath:  mapPath,
		logf:     logf,
		tick:     2 * time.Second,
		idmap:    loadBridgeMap(mapPath, logf),
		pingDL:   5 * time.Second,
		holdPoll: unmirrorWait,
		baseDL:   10 * time.Second,
		chunkDL:  time.Second,
	}
	b.proxies[0] = map[string]*bproxy{}
	b.proxies[1] = map[string]*bproxy{}
	b.departed[0] = map[string]broker.SessionInfo{}
	b.departed[1] = map[string]broker.SessionInfo{}
	b.transients = map[string]*btransient{}
	b.rows = loadRowSet(rowsPath(mapPath), logf)
	b.reaping = map[string]bool{}
	b.rowStamp = map[string]int{}
	b.holdFloor = map[string]int{}
	b.daemonCap = [2]int{-1, -1} // unknown until the first ping answers
	return b
}

// btransient is the shared registered connection for one departed sender id.
// The first acquire dials; concurrent first acquires wait on ready instead of
// racing their own dials (E1: two dials meant two rows, and the first release
// byed the second's away).
type btransient struct {
	b       *Bridge
	home    int
	id      string
	conn    *bconn
	refs    int
	dialErr error
	ready   chan struct{} // closed once conn/dialErr are final
	closing chan struct{} // set when the last release starts: acquires wait
	// it out instead of dialing a row the release's bye would kill (F1)
}

func transientKey(home int, id string) string { return fmt.Sprintf("%d:%s", home, id) }

// acquireTransient returns the shared transient for a departed sender,
// dialing it on first use. Concurrent relays of the same departed sender
// share one connection and one row (Bye is per id: two rows deregister each
// other, P29).
func (b *Bridge) acquireTransient(home int, id string) (*btransient, error) {
	key := transientKey(home, id)
	for {
		b.mu.Lock()
		t := b.transients[key]
		if t != nil && t.closing != nil {
			closing := t.closing
			b.mu.Unlock()
			<-closing // the last release is draining and bying; a dial now
			continue  // would register a row its bye then kills (F1)
		}
		var waited *btransient
		if t == nil {
			// be the dialer: publish a placeholder first, so a concurrent first
			// acquire waits on it instead of dialing a second row (E1)
			t = &btransient{b: b, home: home, id: id, ready: make(chan struct{})}
			b.transients[key] = t
			b.mu.Unlock()
			b.rows.add(home, id) // write-ahead, before the dial: the row is ours
			// to finish even if the bridge dies mid-hello (G2)
			waited = t
			info, ok := b.departedInfo(home, id)
			var c *bconn
			var err error
			if ok {
				c, err = b.dialTransient(home, id, info)
			} else {
				err = fmt.Errorf("sender %s is no longer departed", id)
			}
			if err != nil {
				b.mu.Lock()
				t.dialErr = err
				if b.transients[key] == t {
					delete(b.transients, key)
				}
				b.mu.Unlock()
				close(t.ready)
			} else {
				b.mu.Lock()
				t.conn = c // under b.mu: a failed acquirer's re-read must be
				// ordered against this write (G1)
				b.mu.Unlock()
				close(t.ready)
			}
		} else {
			b.mu.Unlock()
			<-t.ready // single-flight: the dialer owns the dial
			waited = t
		}
		b.mu.Lock()
		cur := b.transients[key]
		if cur == nil || cur.conn == nil || cur.closing != nil {
			// a failed or superseded dial: surface ITS error, so a refused
			// hello still bounces instead of waiting forever (F2)
			err := fmt.Errorf("transient for %s unavailable", id)
			if waited != nil && waited.dialErr != nil {
				err = waited.dialErr
			}
			b.mu.Unlock()
			return nil, err
		}
		if cur.conn.isDead() {
			err := fmt.Errorf("transient for %s is dead", id)
			if cur.refs == 0 { // nobody will release it: purge (N5)
				delete(b.transients, key)
			}
			b.mu.Unlock()
			return nil, err
		}
		cur.refs++
		b.mu.Unlock()
		return cur, nil
	}
}

// releaseTransient drops one reference. The last release removes the row -
// but only if the session has not come back: a resumed session's new proxy
// owns the row by then, and a bye would close its subscription (P32).
func (b *Bridge) releaseTransient(t *btransient) {
	key := transientKey(t.home, t.id)
	b.mu.Lock()
	t.refs--
	if t.refs > 0 {
		b.mu.Unlock()
		return
	}
	var closing chan struct{}
	if b.transients[key] == t {
		// Stay published as closing until the bye is done: an acquire in the
		// drain window must wait it out, not dial a second row whose
		// registration this bye would remove (F1).
		closing = make(chan struct{})
		t.closing = closing
	}
	_, hasProxy := b.proxies[t.home][t.id]
	b.mu.Unlock()
	if t.conn != nil && !hasProxy {
		b.drainRowBye(t.conn, t.home, t.id)
	} else if t.conn != nil {
		t.conn.retire()
	}
	b.mu.Lock()
	if b.transients[key] == t {
		delete(b.transients, key)
	}
	b.mu.Unlock()
	if closing != nil {
		close(closing)
	}
}

// stampRow records that a hello or a bye for the row (home, id) just
// returned, with the far side's fetch counter at that moment. An id leaves
// the row set only by observation - a far list fetched after this point
// showing the row gone (H1): a returned bye proves nothing, since Bye keeps
// a row whose mailbox is not empty.
func (b *Bridge) stampRow(home int, id string) {
	b.mu.Lock()
	b.rowStamp[transientKey(home, id)] = b.listStarted[1-home]
	b.mu.Unlock()
}

// drainRowBye answers for a registered row's remaining mail and byes it away:
// each message bounces to its sender (asks get their reply), the inbox is
// acked batch by batch until empty (N6), and the bye follows unless a proxy
// or another transient owns the row by then (N3). A bye that cannot be sent
// leaves the row tracked, so the reaper finishes it later (G2).
func (b *Bridge) drainRowBye(conn *bconn, home int, id string) {
	for {
		var msgs []*broker.Message
		if err := conn.callDL(b.ctx, broker.Request{Op: "inbox"}, &msgs, fixedDL(b.respDL())); err != nil || len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			b.bounceOn(conn, fmt.Sprintf("%s left", id), m)
		}
		ids := make([]string, 0, len(msgs))
		for _, m := range msgs {
			ids = append(ids, m.ID)
		}
		conn.callDL(b.ctx, broker.Request{Op: "ack", IDs: ids}, nil, b.reqBudget())
	}
	b.mu.Lock()
	_, hasProxy := b.proxies[home][id]
	tr := b.transients[transientKey(home, id)]
	b.mu.Unlock()
	if hasProxy || (tr != nil && tr.conn != conn) {
		conn.retire() // someone else owns the row now; leave it to them
		return
	}
	err := conn.callDL(b.ctx, broker.Request{Op: "bye"}, nil, b.reqBudget())
	conn.retire()
	b.stampRow(home, id)
	if err != nil {
		b.logf("bridge: row %s could not be byed (%v); it stays tracked", b.pfx[home]+id, err)
	}
	// the id stays tracked either way: a bye does not delete a row that still
	// holds mail, and the next tick's reap finishes it again (H1a)
}

// bounceOn tells the sender of m, over conn, that the message did not land.
// Used by a transient's last release (E2): the row held mail that nothing
// else will answer for. Best effort - the bye follows regardless.
func (b *Bridge) bounceOn(conn *bconn, reason string, m *broker.Message) {
	err := conn.callDL(b.ctx, broker.Request{
		Op: "send",
		SendReq: broker.SendReq{
			To:      m.From,
			Text:    fmt.Sprintf("[agent-mesh link] not delivered: %s", reason),
			ReplyTo: ternary(m.ExpectsReply, m.ID, ""),
		},
	}, nil, b.reqBudget())
	if err != nil {
		b.logf("bridge: bounce for %s's row failed: %v", m.To, err)
	}
}

// respDL is the budget for calls whose answer can be a full frame (list,
// inbox): a slow link must not turn a big answer into a timeout loop.
func (b *Bridge) respDL() time.Duration {
	return b.baseDL + b.chunkDL*time.Duration(broker.MaxFrame/(16<<10))
}

// reqBudget scales a call's deadline with its encoded request: baseDL plus
// chunkDL per 16 KiB actually on the wire, attachments included (B2).
func (b *Bridge) reqBudget() func(int) time.Duration {
	return func(n int) time.Duration {
		return b.baseDL + b.chunkDL*time.Duration(n/(16<<10))
	}
}

// Run ticks until ctx is done. Errors are logged, never fatal: a down side is
// expected (the laptop may sleep, the tunnel may drop).
func (b *Bridge) Run(ctx context.Context) {
	b.startCtx(ctx)
	defer b.cancel()
	defer b.closeConns()
	b.tickOnce() // mirror immediately, not after the first interval
	t := time.NewTicker(b.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.tickOnce()
		}
	}
}

// startCtx wires the context for Run; tests that drive tickOnce by hand use
// it without the loop.
func (b *Bridge) startCtx(ctx context.Context) {
	b.ctx, b.cancel = context.WithCancel(ctx)
}

// Stop cancels the context and tears the connections down.
func (b *Bridge) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	b.closeConns()
}

// closeConns tears the bridge's connections down on stop: without this, a
// stopped bridge's proxy connections would stay subscribed and keep the proxy
// rows live next to a restarted bridge.
func (b *Bridge) closeConns() {
	b.mu.Lock()
	for i := 0; i < 2; i++ {
		if c := b.ctrl[i]; c != nil {
			c.close()
		}
		b.ctrl[i] = nil
	}
	var conns []*bconn
	var proxies []*bproxy
	for i := 0; i < 2; i++ {
		for _, p := range b.proxies[i] {
			proxies = append(proxies, p)
			if c := p.connSnapshot(); c != nil {
				conns = append(conns, c)
			}
		}
	}
	b.mu.Unlock()
	for _, p := range proxies {
		p.stopWorker() // safety net: in-flight holds and retries return
	}
	for _, c := range conns {
		c.close()
	}
}

func (b *Bridge) ctrlConn(i int) (*bconn, error) {
	b.mu.Lock()
	c := b.ctrl[i]
	b.mu.Unlock()
	if c == nil || c.isDead() {
		nc, err := dialBConn(b.sockets[i], nil) // plain dial: never auto-start
		if err != nil {
			return nil, err
		}
		b.mu.Lock()
		b.ctrl[i] = nc
		b.mu.Unlock()
		c = nc
	}
	// Every tick pings, fresh or cached: the health check has its own short
	// deadline, so a wedged daemon (or a half-open tunnel) fails within the
	// ping budget instead of stalling the tick for a full call. The answer
	// doubles as the capability probe: a daemon whose protocol result lacks
	// bridge support (or predates the op and answers bad_request) is up but
	// unusable, which is not a side-down.
	var cap struct{ Protocol, Bridge int } // a struct ignores fields a newer daemon adds
	if err := c.callDL(b.ctx, broker.Request{Op: "protocol"}, &cap, fixedDL(b.pingDL)); err != nil {
		var be *broker.Error
		if errors.As(err, &be) && (be.Code == broker.CodeBadRequest || be.Code == broker.CodeNotRegistered) {
			cap.Bridge = 0 // older than the protocol op (not_registered before hello): no bridge support
		} else {
			c.close()
			b.mu.Lock()
			if b.ctrl[i] == c {
				b.ctrl[i] = nil
			}
			b.mu.Unlock()
			return nil, err
		}
	}
	b.setDaemonCap(i, cap.Bridge)
	return c, nil
}

// setDaemonCap records a daemon's bridge capability and logs each transition
// once. Mirroring on a side needs its daemon capable; the tick keeps pinging,
// so `agm restart` on that host is picked up without restarting the link.
func (b *Bridge) setDaemonCap(i int, v int) {
	b.mu.Lock()
	was := b.daemonCap[i] // -1 = never answered yet
	b.daemonCap[i] = v
	b.mu.Unlock()
	switch {
	case v < 1 && (was == -1 || was >= 1):
		b.logf("bridge: daemon on %s has no bridge support (its protocol answer lacks it): upgrade agm on that host and run agm restart there; mirroring is paused and retried", b.sockets[i])
	case v >= 1 && was == 0:
		b.logf("bridge: daemon on %s has bridge support; mirroring resumes", b.sockets[i])
	}
}

func (b *Bridge) tickOnce() {
	var lists [2][]broker.SessionInfo
	var ok [2]bool
	var fetch [2]int
	for i := 0; i < 2; i++ {
		b.mu.Lock()
		b.listStarted[i]++
		fetch[i] = b.listStarted[i]
		b.mu.Unlock()
		ctrl, err := b.ctrlConn(i)
		if err != nil {
			b.miss(i)
			continue
		}
		b.mu.Lock()
		capOK := b.daemonCap[i] >= 1
		b.mu.Unlock()
		if !capOK {
			continue // up but unusable: no list, no mirroring, and not a miss
		}
		var l []broker.SessionInfo
		if err := ctrl.callDL(b.ctx, broker.Request{Op: "list"}, &l, fixedDL(b.respDL())); err != nil {
			b.miss(i)
			continue
		}
		b.mu.Lock()
		b.misses[i] = 0
		b.mu.Unlock()
		b.setSide(i, true)
		lists[i], ok[i] = l, true
	}
	for i := 0; i < 2; i++ {
		if ok[i] {
			b.applyMirrors(i, lists[i], ok[1-i], lists[1-i], fetch[1-i])
			if ok[1-i] {
				// this fetch is now applied: a list that began after a message
				// arrived and reached this point has had its chance to mirror
				// the sender (A1)
				b.mu.Lock()
				b.listApplied[i] = fetch[i]
				b.mu.Unlock()
			}
		}
	}
}

// miss records a failed side fetch. One miss only skips the tick (the list is
// missing); a side is declared down after two consecutive misses, so a ping
// that just lost a race with a large relay on ssh's single TCP stream does
// not flap the side (A6).
func (b *Bridge) miss(i int) {
	b.mu.Lock()
	b.misses[i]++
	n := b.misses[i]
	b.mu.Unlock()
	if n >= 2 {
		b.setSide(i, false)
	}
}

// setSide records side health, logging the initial state of each side once
// and then once per transition (a down side is normal - a laptop on a plane -
// and must not flood the log per tick or per session).
func (b *Bridge) setSide(i int, up bool) {
	b.mu.Lock()
	first := !b.sideLogged[i]
	was := b.sideUp[i]
	b.sideUp[i] = up
	b.sideLogged[i] = true
	b.mu.Unlock()
	if first {
		if up {
			b.logf("bridge: %s is up", b.sockets[i])
		} else {
			b.logf("bridge: %s is not reachable yet; mirroring pauses until it answers", b.sockets[i])
		}
		return
	}
	if was == up {
		return
	}
	if up {
		b.logf("bridge: %s is back up", b.sockets[i])
	} else {
		b.logf("bridge: %s is down; mirroring pauses until it answers", b.sockets[i])
	}
}

// applyMirrors reconciles daemon i's mirror onto daemon 1-i, level-triggered,
// from the lists this tick already fetched. farOK/farList describe the other
// side; without them no new proxies are created (the flood fix) and none are
// reconciled.
func (b *Bridge) applyMirrors(i int, own []broker.SessionInfo, farOK bool, farList []broker.SessionInfo, farFetch int) {
	// Mirror only ids without "/": gate sessions and the other side's proxies
	// are excluded, so there is no proxy of a proxy and no loop.
	var cands []broker.SessionInfo
	for _, s := range own {
		if s.ID != "" && !strings.Contains(s.ID, "/") {
			cands = append(cands, s)
		}
	}
	// Deterministic cap: live sessions first, then the newest LastSeen.
	slices.SortStableFunc(cands, func(a, c broker.SessionInfo) int {
		if a.Live != c.Live {
			if a.Live {
				return -1
			}
			return 1
		}
		return c.LastSeen.Compare(a.LastSeen) // newest first
	})
	skip := 0
	if len(cands) > bridgeMirrorCap {
		skip = len(cands) - bridgeMirrorCap
		cands = cands[:bridgeMirrorCap]
	}
	b.mu.Lock()
	if skip != b.skipped[i] { // log once per change, not every tick
		b.skipped[i] = skip
		b.mu.Unlock()
		b.logf("bridge: mirroring %d of %d sessions on %s (cap %d)", len(cands), len(cands)+skip, b.sockets[i], bridgeMirrorCap)
	} else {
		b.mu.Unlock()
	}

	want := map[string]broker.SessionInfo{}
	for _, s := range cands {
		want[s.ID] = s
	}

	// subscribedOK decides whether a fresh connection may SUBSCRIBE for id:
	// while the far row still holds mail, it must not - a subscribed hello
	// replays the whole mailbox ahead of its own answer, and on a slow
	// downlink that replay outlasts any request-side budget (P31/P31b/P33).
	// Instead the proxy opens registered, the worker drains the row through
	// frame-sized inbox batches, and the subscription follows once Queued
	// reaches zero.
	subscribedOK := func(id string) bool {
		row := findRow(farList, b.pfx[i]+id)
		return row == nil || row.Queued == 0
	}
	// Proxies this tick already created or re-opened: the far list was
	// fetched before their row existed or changed, so reconciling them
	// against it would re-open a fresh connection for nothing (and leak the
	// one it replaces). The next tick reconciles them with a fresh list.
	touched := map[string]bool{}

	// Create missing proxies (only when the far side answered this tick).
	if farOK {
		for id, s := range want {
			b.mu.Lock()
			_, exists := b.proxies[i][id]
			b.mu.Unlock()
			if exists {
				continue
			}
			if len(b.pfx[i]+id) > 256 {
				b.logf("bridge: skipping %s: proxy id over 256 bytes", id)
				continue
			}
			b.mu.Lock()
			reaping := b.reaping[transientKey(i, id)]
			if reaping {
				b.holdFloor[transientKey(i, id)] = b.listStarted[i] + 1 // this list could not create it
			}
			b.mu.Unlock()
			if reaping {
				continue // its old row's reap is finishing; create next tick (N10)
			}
			// Write-ahead: the id is wanted, so the reaper cannot want it, but
			// a bridge that dies mid-hello still leaves the row tracked (G2).
			// A failed hello keeps the id too; the reaper drops or finishes it.
			b.rows.add(i, id)
			p := &bproxy{
				b:       b,
				home:    i,
				realID:  id,
				info:    s,
				done:    make(chan struct{}),
				queue:   make(chan *broker.Message, bridgeQueueCap),
				drainCh: make(chan struct{}, 1),
			}
			if err := p.openConn(s.Live && subscribedOK(id)); err != nil {
				b.logf("bridge: proxy %s: %v", b.pfx[i]+id, err)
				continue
			}
			b.mu.Lock()
			b.proxies[i][id] = p
			delete(b.holdFloor, transientKey(i, id))
			b.mu.Unlock()
			touched[id] = true
			go p.relay()
		}
	}

	// Update and reconcile what is held. Field changes reach the proxy by a
	// re-hello; live transitions swap the connection; a dead connection, a
	// missing row (swept, or the far daemon restarted) or a live mismatch
	// reopens it.
	b.mu.Lock()
	held := make([]*bproxy, 0, len(b.proxies[i]))
	for _, p := range b.proxies[i] {
		held = append(held, p)
	}
	b.mu.Unlock()
	for _, p := range held {
		if p.isRemoved() { // the worker byed it between the snapshot and here
			continue
		}
		id := p.realID
		s, wanted := want[id]
		if !wanted {
			// The real session left its daemon (or fell out of the cap): the
			// worker drains what its proxy still holds - those relays bounce
			// back to the senders - and byes when it is done. The connection
			// must stay alive until then: the bounces go out on it. No waiting
			// here (P20c): the tick just marks, keeps the conn alive and nudges.
			p.mu.Lock()
			first := !p.gone
			p.gone = true
			p.mu.Unlock()
			if first {
				b.logf("bridge: %s left %s; finishing its mail, then removing the proxy", id, b.sockets[i])
			}
			// Reopen only when the far side answered this tick (C1): behind a
			// half-open ssh -L the dial succeeds and the hello then holds for
			// its whole budget, so an unguarded reopen would stall the tick by
			// that much per gone proxy. While the side is down the proxy just
			// stays; when it answers again the reopen, drain, bounces and bye
			// follow - instead of stranding the mail for MailTTL (P24).
			if farOK {
				if c := p.connSnapshot(); c == nil || c.isDead() {
					if err := p.openConn(false); err != nil {
						p.mu.Lock()
						logIt := !p.reopenFailed
						p.reopenFailed = true
						p.mu.Unlock()
						if logIt { // once per outage, not per tick
							b.logf("bridge: proxy %s: holding its mail until the far side answers: %v", b.pfx[i]+id, err)
						}
					} else {
						p.mu.Lock()
						p.reopenFailed = false
						p.mu.Unlock()
					}
				}
			}
			p.nudge()
			continue
		}
		p.mu.Lock()
		changed := p.info.Name != s.Name || p.info.Harness != s.Harness || p.info.Cwd != s.Cwd
		p.gone = false // it came back before the bye (C5): keep the proxy
		live := p.live
		p.mu.Unlock()
		if !farOK {
			// The far side cannot take a hello or a transition right now, and
			// behind a half-open tunnel each attempt would stall the tick for
			// a full call budget per changed session (E3). p.info is applied
			// only on a successful re-hello below, so the change is seen
			// again on the next farOK tick.
			continue
		}
		if changed {
			p.mu.Lock()
			prev := p.info
			p.info = s // the re-hello sends these fields...
			p.mu.Unlock()
			if err := p.rehello(); err != nil {
				p.mu.Lock()
				p.info = prev // ...but only count them applied on success, or
				p.mu.Unlock() // the change is re-sent on the next farOK tick (E3)
				b.logf("bridge: proxy %s re-hello: %v", b.pfx[i]+id, err)
			}
		}
		if live != s.Live {
			if !s.Live || subscribedOK(id) { // an upgrade waits for the row to
				// drain; a downgrade proceeds at once (P31b)
				p.transition(s.Live)
				touched[id] = true
			}
		}
		if touched[id] {
			continue
		}
		// A retired subscribed connection keeps the far row live until the
		// tunnel drains it - up to the call's deadline for a full frame, so a
		// tick count can never cover it (C6). While it is open, skip the
		// row-based reconcile: re-fighting the stale row burns a dial and
		// hello per tick and logs a false "someone else". When it dies, clear
		// it but skip one more tick: the far daemon's detach trails the close
		// by a moment, and the row settles by the next list.
		rc := p.retiringConn()
		if rc != nil && rc.isDead() {
			p.mu.Lock()
			p.retiring = nil
			p.mu.Unlock()
		}
		rowDraining := rc != nil
		// Reconcile against the far list (M3): reopen dead or swept proxies,
		// follow a row the far daemon flipped, refresh old offline rows.
		row := findRow(farList, b.pfx[i]+id)
		conn := p.connSnapshot()
		connDead := conn == nil || conn.isDead()
		want := wantLive(p)
		if rowDraining {
			// the stale row stays live until the retired conn drains; nothing
			// to reconcile against it
		} else if row != nil && row.Live && !want {
			// The row is live while the bridge holds the session offline: a
			// subscriber the bridge does not own sits on the proxy id.
			// Reopening re-fights the same row and burns a dial and hello per
			// tick (P14), so say it once and leave the row alone.
			p.mu.Lock()
			first := !p.foreign
			p.foreign = true
			p.mu.Unlock()
			if first {
				b.logf("bridge: proxy %s is subscribed by someone else; leaving the row alone", b.pfx[i]+id)
			}
		} else {
			p.mu.Lock()
			p.foreign = false
			p.mu.Unlock()
			if connDead || row == nil || row.Live != want {
				if err := p.openConn(want && subscribedOK(id)); err != nil {
					b.logf("bridge: proxy %s reopen: %v", b.pfx[i]+id, err)
				}
			} else if !row.Live && time.Since(row.LastSeen) > refreshAfter {
				if err := p.rehello(); err != nil { // refresh LastSeen: a held
					// registered row never refreshes itself, and Sweep reads it
					b.logf("bridge: proxy %s refresh: %v", b.pfx[i]+id, err)
				}
			}
		}
		// Offline drain (M2): nudged onto the worker, which pulls the queued
		// mail through the same serial path. Only when idle: while the worker
		// is stuck (say, on mailbox_full) every tick would re-queue the whole
		// mailbox and pile up duplicates (P16) - the next idle tick drains
		// whatever is left anyway.
		if !want && !p.isGone() && p.pendingCount() == 0 {
			p.nudge()
		}
	}

	if farOK {
		b.reapRows(i, want, farList, farFetch)
	}
}

// reapRows finishes rows this bridge registered whose real session no longer
// exists: a session that ended while no running bridge held its proxy (a
// restart gap, a departure during an outage before the bridge itself went
// away), or a transient whose bye could not be sent. Without this the row
// lingers until MailTTL accepting mail nobody answers (G2). Ownership is the
// persisted row set - the prefix alone is not proof, gate sessions share it.
func (b *Bridge) reapRows(home int, want map[string]broker.SessionInfo, farList []broker.SessionInfo, farFetch int) {
	ids := b.rows.snapshot(home)
	b.mu.Lock()
	var cand []string
	for _, id := range ids {
		if _, w := want[id]; w {
			continue // the session exists: this tick's creation owns it
		}
		if _, p := b.proxies[home][id]; p {
			continue
		}
		if _, tr := b.transients[transientKey(home, id)]; tr {
			continue
		}
		if b.reaping[transientKey(home, id)] {
			continue // a reap is already finishing it
		}
		cand = append(cand, id)
	}
	for _, id := range cand {
		b.reaping[transientKey(home, id)] = true
	}
	b.mu.Unlock()
	for _, id := range cand {
		row := findRow(farList, b.pfx[home]+id)
		if row == nil {
			// Nothing to finish: swept at MailTTL, or the far daemon restarted.
			// Drop the id only by observation - and only from a list fetched
			// after the id's last hello or bye returned: a row whose whole
			// life fell between this tick's fetch and its reap would otherwise
			// be dropped on a stale list (H1).
			b.mu.Lock()
			stale := b.rowStamp[transientKey(home, id)] >= farFetch
			if !stale {
				// N12, under b.mu: stampRow writes the same map from every
				// worker whose hello or bye returns, and a plain build turns
				// an overlapping write into a fatal, unrecoverable crash (K1)
				delete(b.rowStamp, transientKey(home, id))
			}
			b.mu.Unlock()
			if !stale {
				b.rows.remove(home, id)
				// A transient or proxy whose write-ahead add raced this remove
				// (it published after the candidate selection) must not lose
				// its row: re-check under b.mu and re-add. One published after
				// this re-check re-adds itself at its own acquire.
				b.mu.Lock()
				_, hasP := b.proxies[home][id]
				_, hasT := b.transients[transientKey(home, id)]
				b.mu.Unlock()
				if hasP || hasT {
					b.rows.add(home, id)
				}
			}
			b.mu.Lock()
			delete(b.reaping, transientKey(home, id))
			b.mu.Unlock()
			continue
		}
		if row.Live {
			// a subscriber the bridge does not own sits on the row; bying it
			// would take their subscription with it
			b.mu.Lock()
			delete(b.reaping, transientKey(home, id))
			b.mu.Unlock()
			continue
		}
		// N8/N8b: hello with the row's own fields, prefix stripped from both
		// its ID and its Name (dialTransient re-prefixes both), so the reap
		// neither flashes a blank row nor bounces as "laptop/laptop/alice".
		info := *row
		info.ID = strings.TrimPrefix(info.ID, b.pfx[home])
		info.Name = strings.TrimPrefix(info.Name, b.pfx[home])
		go b.finishRow(home, id, info)
	}
}

// finishRow ends one tracked row on a goroutine: hello registered (the drain
// needs a conn; the row may even have been swept and the hello recreates it
// so the bye removes it cleanly), answer for its mail, bye (G2).
func (b *Bridge) finishRow(home int, id string, info broker.SessionInfo) {
	defer func() {
		b.mu.Lock()
		delete(b.reaping, transientKey(home, id))
		b.mu.Unlock()
	}()
	c, err := b.dialTransient(home, id, info)
	if err != nil {
		return // keep the id: the next farOK tick retries
	}
	b.drainRowBye(c, home, id)
}

func wantLive(p *bproxy) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.live
}

// findRow finds a session row by id in a list.
func findRow(list []broker.SessionInfo, id string) *broker.SessionInfo {
	for i := range list {
		if list[i].ID == id {
			return &list[i]
		}
	}
	return nil
}

// removeProxy drops a byed proxy from the bridge's maps (the worker exits on
// its own after drainAndBye returns true) and remembers it as departed (C2):
// mail it sent that is still queued elsewhere relays through a transient
// registered connection under its identity instead of bouncing to a session
// that no longer exists.
func (b *Bridge) removeProxy(p *bproxy) {
	p.mu.Lock()
	p.removed = true
	conn := p.conn // a reopen may have installed a fresh connection between
	// the worker's retire and here (P26): nobody tracks it anymore, so bye it
	// here - the row must not linger as an orphan the bridge forgot about
	info := p.info
	home := p.home
	id := p.realID
	p.mu.Unlock()
	if conn != nil {
		conn.callDL(b.ctx, broker.Request{Op: "bye"}, nil, b.reqBudget())
		b.stampRow(p.home, p.realID)
		conn.retire()
	}
	b.mu.Lock()
	delete(b.proxies[home], id)
	if _, ok := b.departed[home][id]; !ok {
		b.departedFIFO[home] = append(b.departedFIFO[home], id)
		if len(b.departedFIFO[home]) > departedCap {
			old := b.departedFIFO[home][0]
			b.departedFIFO[home] = b.departedFIFO[home][1:]
			delete(b.departed[home], old)
		}
	}
	b.departed[home][id] = info
	b.mu.Unlock()
}

// departedInfo returns the remembered info of a byed proxy, if recent.
func (b *Bridge) departedInfo(side int, id string) (broker.SessionInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	info, ok := b.departed[side][id]
	return info, ok
}

// dialTransient opens a short-lived registered connection as a departed
// sender's proxy id on the target daemon (C2): the sender's last words still
// carry its identity, and the bye in releaseTransient removes the row again.
func (b *Bridge) dialTransient(src int, id string, info broker.SessionInfo) (*bconn, error) {
	si := broker.SessionInfo{
		ID:      b.pfx[src] + id,
		Name:    b.pfx[src] + info.Name,
		Harness: info.Harness,
		Cwd:     capCwd(info.Cwd),
	}
	c, err := dialBConn(b.sockets[1-src], nil)
	if err != nil {
		return nil, err
	}
	c.progressDL = b.baseDL
	helloErr := c.callDL(b.ctx, broker.Request{Op: "hello", Session: &si}, nil, b.reqBudget())
	b.stampRow(src, id)
	if helloErr != nil {
		c.close()
		return nil, helloErr
	}
	return c, nil
}

// stopWorker ends the relay worker exactly once (the Stop safety net; the
// normal exit is the worker's own bye).
func (p *bproxy) stopWorker() {
	p.closeOnce.Do(func() { close(p.done) })
}

// -------------------------------------------------------------------- relay

// relayOne delivers one message m that arrived at a proxy of session x (x
// lives on daemon p.home; m is on daemon 1-p.home, from m.From, to the proxy).
func (b *Bridge) relayOne(p *bproxy, m *broker.Message) {
	src := 1 - p.home // the daemon m is on
	dst := p.home     // the daemon x lives on

	// Dedupe: a replayed message (reconnect hello, offline drain after a push)
	// whose id is already mapped was relayed before.
	if b.idmap.has(src, m.ID) {
		b.ackOn(p, m.ID)
		return
	}

	// The sender must be mirrored on the target daemon. A message from a
	// session that said hello after the last tick is normal (an agent that
	// starts and messages at once): hold it, in order, until a list fetch
	// that STARTED after the message arrived has been APPLIED with creation
	// enabled (started/applied, A1: over a real tunnel the apply trails the
	// fetch by seconds, and bouncing in that window drops every first message
	// of a new session), and only bounce if the sender is still unmirrored
	// then. A "/" sender is either one of this bridge's own proxies (the
	// loop guard: log and ack) or a gate session, which gets a real bounce
	// (S3).
	b.mu.Lock()
	need := b.listStarted[src] + 1
	b.mu.Unlock()
	var senderConn *bconn
	var tref *btransient // the shared transient of a departed sender (C2/D4)
	for {
		var unm string
		senderConn, unm = b.senderConn(p, m)
		if unm == "" {
			break
		}
		if strings.Contains(m.From, "/") {
			if b.isOwnProxyOn(src, m.From) {
				b.logf("bridge: not relaying %s: sender %s is this bridge's own proxy (loop guard)", m.ID, m.From)
				b.ackOn(p, m.ID)
			} else {
				b.bounce(p, m, unm) // a gate session deserves an answer (S3)
			}
			return
		}
		// The sender left after this message queued (its proxy was byed):
		// relay its last words anyway, under its identity, through the shared
		// transient registered connection (C2/D4). A failed dial is a
		// transport error - wait for the side to answer, never bounce a
		// departed sender's mail away (D1).
		if _, ok := b.departedInfo(src, m.From); ok {
			if t, err := b.acquireTransient(src, m.From); err == nil {
				senderConn, tref = t.conn, t
				break
			} else {
				var be *broker.Error
				if errors.As(err, &be) { // refused for good (N2): do not wait forever
					b.bounce(p, m, be.Code+": "+be.Message)
					return
				}
			}
			// a transport failure: fall through to the wait below
		} else {
			b.mu.Lock()
			applied := b.listApplied[src]
			reaping := b.reaping[transientKey(src, m.From)]
			if reaping {
				// N10: the sender's old row is mid-reap, so this tick cannot
				// have created its proxy. Require a list STARTED after now
				// before any bounce, or a poll between the reap's end and the
				// next tick's creation bounces a returning sender's first
				// message as unmirrored.
				need = b.listStarted[src] + 1
			}
			if f := b.holdFloor[transientKey(src, m.From)]; f > need {
				need = f // a tick skipped its creation for the reap (N10)
			}
			b.mu.Unlock()
			if !reaping && applied >= need {
				b.bounce(p, m, unm) // a post-arrival list came and went: really unmirrored
				return
			}
		}
		select {
		case <-b.ctx.Done():
			return // stopping: m stays queued at the source daemon
		case <-p.done:
			return // byed or stopped mid-hold (safety net for Stop)
		case <-time.After(b.holdPoll):
		}
	}
	defer func() {
		if tref != nil {
			b.releaseTransient(tref)
		}
	}()

	text := m.Text
	var atts []broker.Attachment
	for _, a := range m.Attachments {
		if a.Type == "ref" {
			text += fmt.Sprintf("\n[agent-mesh link] dropped reference to %s (%s is on %s)", a.Name, a.Path, b.hostnameHint(src))
			continue
		}
		atts = append(atts, a)
	}

	// Map reply_to across. Absent (rotated out, or predates the map): send
	// without it plus a note, like a local reply the daemon forgot. The hop
	// chain then restarts at 0 on the target daemon.
	replyTo := ""
	unlinkedNote := ""
	if m.ReplyTo != "" {
		if o, ok := b.idmap.get(src, m.ReplyTo); ok {
			replyTo = o
		} else {
			unlinkedNote = fmt.Sprintf("\n[agent-mesh link] the message this replies to (%s) is no longer mapped; the reply arrives unlinked", m.ReplyTo)
		}
	}

	// Transient failures (rate_limited, mailbox_full, too_many_asks, a dead
	// or timed-out connection) are retried HERE, in place: this proxy's path
	// stays blocked on m, so per-proxy order holds, and m stays unacked at the
	// source daemon (that is the backpressure). The sender proxy is
	// re-resolved each round: its connection may have been swapped meanwhile.
	// A transport failure is logged once per message, and the eventual
	// landing too: a duplicate loop over a slow link must not be silent (A2).
	transportFails := 0
	created, err := b.relaySend(senderConn, p, m, text+unlinkedNote, atts, replyTo)
	for err != nil {
		var be *broker.Error
		if errors.As(err, &be) && be.Code == broker.CodeUnknownMsg && replyTo != "" {
			// The target daemon forgot the mapped reply_to (its seen window
			// is smaller than the map): resend without it, with the note.
			replyTo = ""
			unlinkedNote = "\n[agent-mesh link] the message this replies to is no longer known on the target host; the reply arrives unlinked"
			created, err = b.relaySend(senderConn, p, m, text+unlinkedNote, atts, "")
			if err == nil {
				break
			}
		}
		if errors.As(err, &be) && !bridgeTransient(be.Code) {
			b.bounce(p, m, be.Code+": "+be.Message)
			return
		}
		if b.ctx.Err() != nil {
			return // stopping: m stays queued at the source daemon
		}
		if !errors.As(err, &be) {
			transportFails++
			if transportFails == 1 {
				b.logf("bridge: relay of %s to %s: %v; retrying in place", m.ID, p.realID, err)
			}
		}
		select {
		case <-b.ctx.Done():
			return
		case <-p.done:
			return // byed or stopped mid-retry (safety net for Stop)
		case <-time.After(retryBackoff):
		}
		// Re-resolve the sender exactly as the hold does (D1): a live
		// transient is kept across rounds (a per-round bye-and-redial loses
		// the mail when the dial fails, P30); a dead one is re-dialed; a
		// vanished proxy falls back to the departed transient; only a sender
		// that is neither mirrored nor departed bounces.
		if tref != nil && (tref.conn == nil || tref.conn.isDead()) {
			b.releaseTransient(tref)
			tref = nil
		}
		if tref != nil {
			senderConn = tref.conn
		} else {
			senderConn = nil
			var unm string
			senderConn, unm = b.senderConn(p, m)
			if unm != "" {
				if _, ok := b.departedInfo(src, m.From); ok {
					if t, err := b.acquireTransient(src, m.From); err == nil {
						senderConn, tref = t.conn, t
					} else {
						var be *broker.Error
						if errors.As(err, &be) { // refused for good (N2)
							b.bounce(p, m, be.Code+": "+be.Message)
							return
						}
					}
				} else {
					b.bounce(p, m, unm)
					return
				}
			}
		}
		if senderConn == nil {
			// the transient dial failed (the far side is down): wait like any
			// transport error and retry - never bounce a departed sender
			select {
			case <-b.ctx.Done():
				return
			case <-p.done:
				return
			case <-time.After(retryBackoff):
			}
			continue
		}
		created, err = b.relaySend(senderConn, p, m, text+unlinkedNote, atts, replyTo)
	}
	if transportFails > 0 {
		b.logf("bridge: relay of %s to %s landed after %d transport retries", m.ID, p.realID, transportFails)
	}

	// Sent. Record the pair in memory first, then persist (a persist failure
	// is logged and still acks: in-process dedupe holds; a restart may resend
	// once, the documented window). testHookPostSend is the crash window the
	// restart test injects into: after the map write (memory and disk), before
	// the near ack - a stop there replays, dedupes on the map, and only acks.
	pair := [2]string{"", ""}
	pair[src], pair[dst] = m.ID, created.ID
	b.idmap.record(pair)
	if b.testHookPostSend != nil {
		b.testHookPostSend()
	}
	b.ackOn(p, m.ID)
}

// relaySend performs the send on the target daemon and returns the created
// message (its id is the map pair's target side).
func (b *Bridge) relaySend(conn *bconn, p *bproxy, m *broker.Message, text string, atts []broker.Attachment, replyTo string) (*broker.Message, error) {
	var created broker.Message
	// The deadline scales with the payload: a fixed one re-sends a large
	// relay that is still in transit (the tunnel delivers what it accepted
	// even after the client closed), duplicating it for as long as the link
	// is slower than size/10s (A2).
	err := conn.callDL(b.ctx, broker.Request{
		Op: "send",
		SendReq: broker.SendReq{
			To:           p.realID,
			Text:         text,
			Attachments:  atts,
			ReplyTo:      replyTo,
			ExpectsReply: m.ExpectsReply,
			NoWait:       m.ExpectsReply, // the bridge never blocks on an ask
		},
	}, &created, b.reqBudget())
	if err != nil {
		return nil, err
	}
	return &created, nil
}

// senderConn returns the connection the relay is sent from: the proxy of
// m.From on the target daemon. unmirrored is non-empty when m.From has no
// proxy there.
func (b *Bridge) senderConn(p *bproxy, m *broker.Message) (*bconn, string) {
	if strings.Contains(m.From, "/") {
		return nil, fmt.Sprintf("sender %s is not one of this bridge's sessions", m.From)
	}
	b.mu.Lock()
	sp := b.proxies[1-p.home][m.From]
	b.mu.Unlock()
	if sp == nil {
		return nil, fmt.Sprintf("sender %s is not mirrored on the target host", m.From)
	}
	return sp.connSnapshot(), ""
}

// isOwnProxyOn reports whether id is a proxy this bridge holds on daemon side.
// Gate sessions share the NAME/ prefix with bridge proxies, so membership, not
// the prefix, is the loop guard.
func (b *Bridge) isOwnProxyOn(side int, id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.proxies[1-side] { // proxies living on `side`
		if b.pfx[p.home]+p.realID == id {
			return true
		}
	}
	return false
}

// ackOn acks ids on the daemon the proxy connection speaks to. not_registered
// (the proxy was swept while we held the connection) self-heals with a
// re-hello and one retry.
func (b *Bridge) ackOn(p *bproxy, ids ...string) {
	for attempt := 0; attempt < 2; attempt++ {
		conn := p.connSnapshot()
		if conn == nil {
			return
		}
		err := conn.callDL(b.ctx, broker.Request{Op: "ack", IDs: ids}, nil, b.reqBudget())
		if err == nil {
			return
		}
		var be *broker.Error
		if errors.As(err, &be) && be.Code == broker.CodeNotRegistered {
			if rerr := p.rehello(); rerr == nil {
				continue
			}
		}
		return
	}
}

// bounce tells the sender on the source daemon that the message did not land.
// For an ask it is a reply, so a blocking ask returns at once. Bounces go to
// sessions this bridge does not hold as proxies, and never bounce themselves.
func (b *Bridge) bounce(p *bproxy, m *broker.Message, reason string) {
	conn := p.connSnapshot()
	if conn == nil {
		return
	}
	text := fmt.Sprintf("[agent-mesh link] not delivered to %s: %s", b.proxyID(p), reason)
	// A transient bounce failure is retried in place: the asker's wait must
	// end, and m stays unacked until it does.
	for {
		err := conn.callDL(b.ctx, broker.Request{
			Op: "send",
			SendReq: broker.SendReq{
				To:      m.From,
				Text:    text,
				ReplyTo: ternary(m.ExpectsReply, m.ID, ""),
			},
		}, nil, b.reqBudget())
		if err == nil {
			break
		}
		var be *broker.Error
		if errors.As(err, &be) && !bridgeTransient(be.Code) {
			b.logf("bridge: bounce to %s failed: %v", m.From, err)
			break // the message is undeliverable; ack it away
		}
		if b.ctx.Err() != nil {
			return
		}
		select {
		case <-b.ctx.Done():
			return
		case <-p.done:
			return // byed or stopped mid-bounce (safety net for Stop)
		case <-time.After(retryBackoff):
		}
		conn = p.connSnapshot()
		if conn == nil {
			return
		}
	}
	// Record the bounce so a replay after a lost ack does not bounce twice:
	// both ids on the same side, which dedupes and nothing else reads.
	b.idmap.record([2]string{m.ID, m.ID})
	b.ackOn(p, m.ID)
}

// capCwd caps peer cwd data on a rune boundary: an invalid trailing byte
// would garble the far daemon's state file and every list it serves.
func capCwd(cwd string) string {
	if len(cwd) > cwdCap {
		return strings.ToValidUTF8(cwd[:cwdCap], "")
	}
	return cwd
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// hostnameHint names a host in ref-drop notes: the prefix without the slash.
func (b *Bridge) hostnameHint(i int) string {
	return strings.TrimSuffix(b.pfx[i], "/")
}
