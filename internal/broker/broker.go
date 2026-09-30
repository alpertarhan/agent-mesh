package broker

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

type Limits struct {
	MailboxCap   int
	SendPerMin   float64
	SendBurst    float64
	MaxHops      int
	AskTimeout   time.Duration
	AsksInFlight int
	// GC of sessions with no subscriber and no live PID: removed after IdleTTL with an
	// empty mailbox (immediately if their PID is known dead), after MailTTL otherwise.
	IdleTTL time.Duration
	MailTTL time.Duration
	// Spawned sessions: at most SpawnMax live (or pending) at once, nested at most
	// SpawnDepth deep (user-started agents are depth 0).
	SpawnMax   int
	SpawnDepth int
}

func DefaultLimits() Limits {
	return Limits{MailboxCap: 256, SendPerMin: 20, SendBurst: 10, MaxHops: 8, AskTimeout: 120 * time.Second, AsksInFlight: 4,
		IdleTTL: 10 * time.Minute, MailTTL: 24 * time.Hour, SpawnMax: 8, SpawnDepth: 2}
}

// Sink receives pushed messages. Push must not block; false means the sink is gone.
type Sink interface {
	Push(*Message) bool
	Close()
}

type session struct {
	info    SessionInfo
	mailbox []*Message
	subs    map[Sink]bool // value: exclusive (at most one per session, newest wins)
	tokens  float64
	refill  time.Time
}

type pendingAsk struct {
	From     string    `json:"from"`
	To       string    `json:"to"`
	Hop      int       `json:"hop"`
	Deadline time.Time `json:"deadline"`
	Async    bool      `json:"async,omitempty"` // asker does not wait: never a deadlock edge, reply always queued
	timer    *time.Timer
	sink     Sink // connection waiting for the reply, if still alive
}

type seenMsg struct {
	From string `json:"from"`
	Hop  int    `json:"hop"`
}

const seenCap = 4096

type Broker struct {
	mu       sync.Mutex
	lim      Limits
	sessions map[string]*session
	asks     map[string]*pendingAsk
	seen     map[string]seenMsg // recent message id → sender/hop, for reply routing
	seenRing []string
	spool    string
	now      func() time.Time
	alive    func(pid int) bool
	spawns   map[string]*spawnRec // pending spawns by lowercased name
	quit     chan struct{}
	quitOnce sync.Once
	// history: capped copies of routed messages, oldest first (see history.go).
	history      []*Message
	historyBytes int
	// watchers: question id → connections of `wait` (value: the session that asked).
	// In memory only; a matching reply is pushed to them in addition to normal delivery.
	watchers map[string]map[Sink]string
	// Waker, if set, is called (in a new goroutine) when mail is queued for a session
	// that has no subscriber, so the daemon can push it by other means (Codex app-server).
	Waker func(SessionInfo)
}

// New creates a broker. If spool is non-empty, state is loaded from and saved to it.
func New(lim Limits, spool string) (*Broker, error) {
	b := &Broker{
		lim:      lim,
		sessions: map[string]*session{},
		asks:     map[string]*pendingAsk{},
		seen:     map[string]seenMsg{},
		spool:    spool,
		now:      time.Now,
		alive:    pidAlive,
		spawns:   map[string]*spawnRec{},
		watchers: map[string]map[Sink]string{},
		quit:     make(chan struct{}),
	}
	return b, b.load()
}

// dirtyRune reports whitespace/control/format runes: session ids may not contain them
// (ids are identities and are never rewritten, so hello rejects them instead).
func dirtyRune(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }

// CleanLine returns s reduced to one visible line: every whitespace, control and
// format rune (unicode.Cf: bidi overrides, zero-width marks) becomes a space, runs
// collapse, ends trim, and at most max runes survive (0 = no cap). Peer-supplied
// identity fields pass through it, so they cannot forge line or message structure.
func CleanLine(s string, max int) string {
	var b strings.Builder
	sp := false
	for _, r := range s {
		if dirtyRune(r) || unicode.IsSpace(r) {
			sp = b.Len() > 0
			continue
		}
		if sp {
			b.WriteByte(' ')
			sp = false
		}
		b.WriteRune(r)
	}
	out := b.String()
	if max > 0 {
		if rs := []rune(out); len(rs) > max {
			out = string(rs[:max])
		}
	}
	return strings.TrimRight(out, " ") // the cap can cut right after a space
}

// Hello registers or refreshes a session. Non-empty fields overwrite stored ones.
// With subscribe, sink receives new messages and the current mailbox is replayed to it.
// An exclusive subscriber closes the previous exclusive one (e.g. background waiters
// that a harness spawns once per turn without dedup).
func (b *Broker) Hello(info SessionInfo, sink Sink, subscribe, exclusive bool) error {
	if strings.TrimSpace(info.ID) == "" {
		return errf(CodeBadRequest, "session id required")
	}
	if len(info.ID) > 256 || CleanLine(info.ID, 0) != info.ID {
		return errf(CodeBadRequest, "session id must be one clean line of at most 256 bytes")
	}
	// Identity fields are peer-supplied and printed everywhere: one clean line each.
	info.Name, info.Harness = CleanLine(info.Name, 64), CleanLine(info.Harness, 32)
	info.Cwd, info.Pane = CleanLine(info.Cwd, 0), CleanLine(info.Pane, 64)
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.ensure(info.ID)
	if info.Harness != "" {
		s.info.Harness = info.Harness
	}
	if info.Name != "" {
		s.info.Name = info.Name
	}
	if rec, ok := b.spawns[strings.ToLower(s.info.Name)]; ok && s.info.Name != "" && s.info.Parent == "" && s.info.Depth == 0 {
		s.info.Parent, s.info.Depth = rec.Parent, rec.Depth
		delete(b.spawns, strings.ToLower(s.info.Name))
	}
	if s.info.Name == "" {
		s.info.Name = genName(s.info.ID, func(n string) bool {
			for _, o := range b.sessions {
				if o != s && strings.EqualFold(o.info.Name, n) {
					return true
				}
			}
			return false
		})
	}
	if info.Cwd != "" {
		s.info.Cwd = info.Cwd
	}
	if info.PID != 0 {
		s.info.PID = info.PID
	}
	if info.Pane != "" {
		s.info.Pane = info.Pane
	}
	if subscribe && sink != nil {
		if exclusive {
			for old, ex := range s.subs {
				if ex && old != sink {
					delete(s.subs, old)
					old.Close()
				}
			}
		}
		s.subs[sink] = exclusive
		for _, m := range s.mailbox {
			if !sink.Push(m) {
				delete(s.subs, sink)
				break
			}
		}
	}
	b.save()
	return nil
}

func (b *Broker) ensure(id string) *session {
	s, ok := b.sessions[id]
	if !ok {
		s = &session{info: SessionInfo{ID: id}, subs: map[Sink]bool{}, tokens: b.lim.SendBurst, refill: b.now()}
		b.sessions[id] = s
	}
	s.info.LastSeen = b.now()
	return s
}

// Detach removes sink from session id. Asks waiting on it fall back to the mailbox.
func (b *Broker) Detach(id string, sink Sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.sessions[id]; ok {
		delete(s.subs, sink)
		s.info.LastSeen = b.now()
	}
	for _, a := range b.asks {
		if a.sink == sink {
			a.sink = nil
		}
	}
	b.unwatch(sink)
}

// Shutdown asks the daemon to exit (e.g. `agm restart` after an upgrade).
func (b *Broker) Shutdown()             { b.quitOnce.Do(func() { close(b.quit) }) }
func (b *Broker) Quit() <-chan struct{} { return b.quit }

// Bye ends session id: subscribers are closed; the session is removed, or, if mail
// is queued, kept without PID so MailTTL applies (the harness may resume it).
func (b *Broker) Bye(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return
	}
	for sink := range s.subs {
		sink.Close()
	}
	clear(s.subs)
	if len(s.mailbox) == 0 {
		delete(b.sessions, id)
	} else {
		s.info.PID = 0
	}
	b.save()
}

// Sweep garbage-collects stale sessions (see Limits.IdleTTL/MailTTL). Returns removed ids.
func (b *Broker) Sweep() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var gone []string
	for id, s := range b.sessions {
		if b.live(s) {
			continue
		}
		idle := now.Sub(s.info.LastSeen)
		dead := s.info.PID != 0 // and not alive, per live()
		if idle >= b.lim.MailTTL || (len(s.mailbox) == 0 && (dead || idle >= b.lim.IdleTTL)) {
			delete(b.sessions, id)
			gone = append(gone, id)
		}
	}
	if len(gone) > 0 {
		b.save()
	}
	return gone
}

// ponytail: kill(pid, 0) is fooled by PID reuse; a reused PID only delays GC, it never deletes a live session.
func (b *Broker) live(s *session) bool {
	return len(s.subs) > 0 || (s.info.PID != 0 && b.alive(s.info.PID))
}

func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func (b *Broker) List() []SessionInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]SessionInfo, 0, len(b.sessions))
	for _, s := range b.sessions {
		info := s.info
		info.Live = b.live(s)
		info.Queued = len(s.mailbox)
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Send routes a message from session `from`. sink is the caller's connection; it
// receives the reply directly if r.ExpectsReply and the connection is still alive.
func (b *Broker) Send(from string, r SendReq, sink Sink) (*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var wake *SessionInfo
	defer func() {
		if wake != nil && b.Waker != nil {
			go b.Waker(*wake)
		}
	}()
	src, ok := b.sessions[from]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", from)
	}
	if strings.TrimSpace(r.Text) == "" {
		return nil, errf(CodeBadRequest, "text required")
	}
	if err := checkRefs(r.Attachments); err != nil {
		return nil, err
	}
	hop := 0
	if r.ReplyTo != "" {
		orig, ok := b.seen[r.ReplyTo]
		if !ok {
			return nil, errf(CodeUnknownMsg, "unknown message %q", r.ReplyTo)
		}
		if r.To == "" {
			r.To = orig.From
		}
		hop = orig.Hop + 1
		if r.To == orig.From && b.resurrect(orig.From) {
			defer func() { // the reply failed: leave no empty offline session behind
				if s := b.sessions[orig.From]; s != nil && len(s.mailbox) == 0 && len(s.subs) == 0 {
					delete(b.sessions, orig.From)
				}
			}()
		}
	}
	dst, err := b.resolve(r.To)
	if err != nil {
		return nil, err
	}
	if dst == src {
		return nil, errf(CodeBadRequest, "cannot send to self")
	}
	if hop > b.lim.MaxHops {
		return nil, errf(CodeHopLimit, "reply chain exceeded %d hops", b.lim.MaxHops)
	}
	ask := b.asks[r.ReplyTo]
	answersAsk := ask != nil && ask.From == dst.info.ID && ask.To == src.info.ID
	if !answersAsk && len(dst.mailbox) >= b.lim.MailboxCap {
		return nil, errf(CodeMailboxFull, "mailbox of %q is full (%d)", dst.info.ID, b.lim.MailboxCap)
	}
	if r.NoWait && !r.ExpectsReply {
		return nil, errf(CodeBadRequest, "no_wait needs expects_reply")
	}
	if r.ExpectsReply {
		n := 0 // async asks count too: at most AsksInFlight open questions per sender
		for _, a := range b.asks {
			if a.From == src.info.ID {
				n++
			}
		}
		if n >= b.lim.AsksInFlight {
			return nil, errf(CodeTooManyAsks, "%d asks already pending", n)
		}
		if !r.NoWait && b.waitsOn(dst.info.ID, src.info.ID) { // an async asker never blocks
			return nil, errf(CodeDeadlock, "%q is already waiting on %q; use ask -no-wait (the reply arrives as a message)", dst.info.ID, src.info.ID)
		}
	}
	m := &Message{
		ID: newID(), From: src.info.ID, FromName: src.info.Name, To: dst.info.ID,
		Text: r.Text, Attachments: r.Attachments, ReplyTo: r.ReplyTo, ExpectsReply: r.ExpectsReply,
		Hop: hop, At: b.now(),
	}
	if err := checkSize(m); err != nil {
		return nil, err
	}
	if !b.take(src) {
		return nil, errf(CodeRateLimited, "more than %.0f messages/min", b.lim.SendPerMin)
	}
	b.remember(m)
	b.archive(m)
	b.notifyWatchers(m)
	if r.ExpectsReply {
		a := &pendingAsk{From: m.From, To: m.To, Hop: hop, Deadline: m.At.Add(b.lim.AskTimeout), Async: r.NoWait, sink: sink}
		if r.NoWait {
			a.sink = nil // the reply is queued, never handed to this (possibly closing) connection
		}
		b.addAsk(m.ID, a)
	}
	if answersAsk {
		b.dropAsk(r.ReplyTo)
		// ponytail: a reply pushed to the waiting ask connection is not queued. It was
		// archived above, so if that process dies before printing it, `wait -reply-to`
		// and `history` still find it until history evicts it. Enqueue + ack if that matters.
		if ask.sink != nil && ask.sink.Push(m) {
			b.save()
			return m, nil
		}
	}
	dst.mailbox = append(dst.mailbox, m)
	for s := range dst.subs {
		if !s.Push(m) {
			delete(dst.subs, s)
		}
	}
	if len(dst.subs) == 0 {
		info := dst.info
		wake = &info
	}
	b.save()
	return m, nil
}

type spawnRec struct {
	Parent string
	Depth  int
	At     time.Time
}

const spawnPendingTTL = 2 * time.Minute // a spawned agent must register within this

// resurrect recreates session id (an asker that GC collected before the reply arrived)
// as an offline mailbox with a generated name, so the reply is queued until that id
// says hello again (MailTTL applies). It reports whether it created the session, so a
// reply that fails later can roll it back; a plain send to a missing target still
// fails with unknown_target.
func (b *Broker) resurrect(id string) (created bool) {
	if _, ok := b.sessions[id]; ok {
		return false
	}
	s := b.ensure(id)
	s.info.Name = genName(s.info.ID, func(n string) bool {
		for _, o := range b.sessions {
			if o != s && strings.EqualFold(o.info.Name, n) {
				return true
			}
		}
		return false
	})
	return true
}

// Spawn reserves name for an agent that parent (a session id, or "" for the user)
// is about to start. An empty name gets a generated one. The session that later
// says hello with this name is linked to parent. Returns the name and depth.
func (b *Broker) Spawn(parent, name string) (string, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	name = CleanLine(name, 64) // same rule as hello, so the later hello still matches
	depth := 1
	if parent != "" {
		p, ok := b.sessions[parent]
		if !ok {
			return "", 0, errf(CodeNotRegistered, "session %q has not said hello", parent)
		}
		depth = p.info.Depth + 1
	}
	if depth > b.lim.SpawnDepth {
		return "", 0, errf(CodeSpawnLimit, "spawn depth %d exceeds %d (spawned agents may spawn only %d level(s) deep)", depth, b.lim.SpawnDepth, b.lim.SpawnDepth-1)
	}
	now := b.now()
	n := 0
	for k, rec := range b.spawns {
		if now.Sub(rec.At) > spawnPendingTTL {
			delete(b.spawns, k)
			continue
		}
		n++
	}
	for _, s := range b.sessions {
		if s.info.Depth > 0 && b.live(s) {
			n++
		}
	}
	if n >= b.lim.SpawnMax {
		return "", 0, errf(CodeSpawnLimit, "%d spawned agents already running (max %d)", n, b.lim.SpawnMax)
	}
	taken := func(n string) bool {
		if _, ok := b.spawns[strings.ToLower(n)]; ok {
			return true
		}
		for _, s := range b.sessions {
			if strings.EqualFold(s.info.Name, n) && b.live(s) {
				return true
			}
		}
		return false
	}
	if name == "" {
		name = genName(newID(), taken)
	} else if taken(name) {
		return "", 0, errf(CodeNameTaken, "a live session or pending spawn is already named %q", name)
	}
	b.spawns[strings.ToLower(name)] = &spawnRec{Parent: parent, Depth: depth, At: now}
	return name, depth, nil
}

// Requeue puts msgs back at the front of session id's mailbox after a failed delivery.
func (b *Broker) Requeue(id string, msgs []*Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok || len(msgs) == 0 {
		return
	}
	s.mailbox = append(append([]*Message(nil), msgs...), s.mailbox...)
	b.save()
}

// Pending lists sessions with queued mail and no subscriber (candidates for the Waker).
func (b *Broker) Pending() []SessionInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []SessionInfo
	for _, s := range b.sessions {
		if len(s.mailbox) > 0 && len(s.subs) == 0 {
			out = append(out, s.info)
		}
	}
	return out
}

// Inbox returns the oldest queued messages of session id that fit one frame.
func (b *Broker) Inbox(id string) ([]*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	return append([]*Message(nil), batch(s.mailbox)...), nil
}

// Take atomically returns and removes the oldest frame-sized batch of queued messages. Used by
// hooks that race each other for delivery; it trades at-least-once for exactly-one-taker.
func (b *Broker) Take(id string) ([]*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	// One frame-sized batch, oldest first; the rest stays queued for the next take.
	msgs := append([]*Message(nil), batch(s.mailbox)...)
	s.mailbox = append([]*Message(nil), s.mailbox[len(msgs):]...)
	if len(msgs) > 0 {
		b.save()
	}
	return msgs, nil
}

// Ack removes delivered messages from the mailbox of session id.
// Ack removes the given messages from session id's mailbox and returns the ids it
// actually removed. Ids not queued there (unknown, foreign, already gone) are ignored.
func (b *Broker) Ack(id string, ids []string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return nil, errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	drop := make(map[string]bool, len(ids))
	for _, id := range ids {
		drop[id] = true
	}
	removed := []string{}
	kept := s.mailbox[:0]
	for _, m := range s.mailbox {
		if drop[m.ID] {
			removed = append(removed, m.ID)
		} else {
			kept = append(kept, m)
		}
	}
	clear(s.mailbox[len(kept):])
	s.mailbox = kept
	if len(removed) > 0 {
		b.save()
	}
	return removed, nil
}

// resolve finds a session by exact id, exact name, or unique id prefix.
// uniqueName picks a default name not held by another live session:
// base, base-<harness>, base-<harness>-2, ...
// Explicit names (harness session name, `agm hello -name`) are never altered.
func (b *Broker) uniqueName(self *session, base string) string {
	taken := func(n string) bool {
		for _, o := range b.sessions {
			if o != self && o.info.Name == n && b.live(o) {
				return true
			}
		}
		return false
	}
	if !taken(base) {
		return base
	}
	if h := self.info.Harness; h != "" {
		base += "-" + h
		if !taken(base) {
			return base
		}
	}
	for i := 2; ; i++ {
		if n := fmt.Sprintf("%s-%d", base, i); !taken(n) {
			return n
		}
	}
}

// Resolve returns the session that a message to `to` would reach (same rules as
// Send). Read-only.
func (b *Broker) Resolve(to string) (SessionInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.resolve(to)
	if err != nil {
		return SessionInfo{}, err
	}
	info := s.info
	info.Live, info.Queued = b.live(s), len(s.mailbox)
	return info, nil
}

// resolve finds a session by exact id, name (case-insensitive, optionally
// qualified as name@harness), or unique id prefix. When several match, live ones
// win; still several = ambiguous.
func (b *Broker) resolve(to string) (*session, error) {
	if to == "" {
		return nil, errf(CodeBadRequest, "target required")
	}
	if s, ok := b.sessions[to]; ok {
		return s, nil
	}
	name, harness := to, ""
	if i := strings.LastIndexByte(to, '@'); i > 0 {
		name, harness = to[:i], to[i+1:]
	}
	var byName, byPrefix []*session
	for id, s := range b.sessions {
		if strings.EqualFold(s.info.Name, name) && (harness == "" || strings.EqualFold(s.info.Harness, harness)) {
			byName = append(byName, s)
		}
		if strings.HasPrefix(id, to) {
			byPrefix = append(byPrefix, s)
		}
	}
	for _, cands := range [][]*session{byName, byPrefix} {
		if len(cands) > 1 {
			var live []*session
			for _, s := range cands {
				if b.live(s) {
					live = append(live, s)
				}
			}
			if len(live) > 0 {
				cands = live
			}
		}
		switch len(cands) {
		case 0:
			continue
		case 1:
			return cands[0], nil
		}
		desc := make([]string, len(cands))
		for i, s := range cands {
			desc[i] = fmt.Sprintf("%s (%s, %s)", s.info.ID, s.info.Harness, s.info.Cwd)
		}
		sort.Strings(desc)
		return nil, errf(CodeAmbiguous, "%q matches %d live sessions, use an id: %s", to, len(cands), strings.Join(desc, "; "))
	}
	var live []string
	for _, s := range b.sessions {
		if b.live(s) {
			live = append(live, s.info.Name+"@"+s.info.Harness)
		}
	}
	sort.Strings(live)
	if len(live) > 12 {
		live = append(live[:12], "...")
	}
	return nil, errf(CodeUnknownTarget, "no session %q; live sessions: %s", to, strings.Join(live, ", "))
}

// waitsOn reports whether `from` transitively waits (via pending blocking asks) on `target`.
func (b *Broker) waitsOn(from, target string) bool {
	seen := map[string]bool{}
	stack := []string{from}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == target {
			return true
		}
		if seen[cur] {
			continue
		}
		seen[cur] = true
		for _, a := range b.asks {
			if a.From == cur && !a.Async {
				stack = append(stack, a.To)
			}
		}
	}
	return false
}

func (b *Broker) take(s *session) bool {
	now := b.now()
	s.tokens = min(b.lim.SendBurst, s.tokens+now.Sub(s.refill).Minutes()*b.lim.SendPerMin)
	s.refill = now
	if s.tokens < 1 {
		return false
	}
	s.tokens--
	return true
}

func (b *Broker) remember(m *Message) {
	b.seen[m.ID] = seenMsg{From: m.From, Hop: m.Hop}
	b.seenRing = append(b.seenRing, m.ID)
	if len(b.seenRing) > seenCap {
		delete(b.seen, b.seenRing[0])
		b.seenRing = b.seenRing[1:]
	}
}

func (b *Broker) addAsk(id string, a *pendingAsk) {
	b.asks[id] = a
	a.timer = time.AfterFunc(time.Until(a.Deadline), func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.asks[id] == a {
			b.dropAsk(id)
			b.save()
		}
	})
}

func (b *Broker) dropAsk(id string) {
	if a, ok := b.asks[id]; ok {
		a.timer.Stop()
		delete(b.asks, id)
	}
}

func newID() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

// --- spool ---

type snapshot struct {
	Sessions  []SessionInfo          `json:"sessions"`
	Mailboxes map[string][]*Message  `json:"mailboxes"`
	Asks      map[string]*pendingAsk `json:"asks"`
	Seen      map[string]seenMsg     `json:"seen"`
	SeenRing  []string               `json:"seen_ring"`
	History   []*Message             `json:"history,omitempty"`
}

// ponytail: rewrites the whole snapshot on every mutation, O(state) per message.
// Fine for hundreds of queued messages; switch to append-only log if that shows in profiles.
func (b *Broker) save() {
	if b.spool == "" {
		return
	}
	snap := snapshot{Mailboxes: map[string][]*Message{}, Asks: b.asks, Seen: b.seen, SeenRing: b.seenRing, History: b.history}
	for id, s := range b.sessions {
		snap.Sessions = append(snap.Sessions, s.info)
		if len(s.mailbox) > 0 {
			snap.Mailboxes[id] = s.mailbox
		}
	}
	data, err := json.Marshal(snap)
	if err == nil {
		tmp := b.spool + ".tmp"
		if err = os.WriteFile(tmp, data, 0o600); err == nil {
			err = os.Rename(tmp, b.spool)
		}
	}
	if err != nil {
		log.Printf("mesh: spool save: %v", err)
	}
}

func (b *Broker) load() error {
	if b.spool == "" {
		return nil
	}
	data, err := os.ReadFile(b.spool)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	for _, info := range snap.Sessions {
		s := b.ensure(info.ID)
		info.Live, info.Queued = false, 0
		s.info = info
		s.mailbox = snap.Mailboxes[info.ID]
	}
	if snap.Seen != nil {
		b.seen, b.seenRing = snap.Seen, snap.SeenRing
	}
	for _, m := range snap.History {
		if m != nil {
			b.history = append(b.history, m)
			b.historyBytes += msgSize(m)
		}
	}
	b.trimHistory()
	for id, a := range snap.Asks {
		if a.Deadline.After(b.now()) {
			b.addAsk(id, a)
		}
	}
	return nil
}
