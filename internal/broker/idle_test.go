package broker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// served runs a broker with a spool on a private socket until the test ends.
// activity is what a daemon reads between its ticks: when the last handler ended.
func activity(b *Broker) (time.Time, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastDone, b.finished
}

// book is the daemon's wake bookkeeping, for tests.
type book struct {
	busy bool
	done time.Time
	owed func(SessionInfo, string) bool
}

func (k *book) Busy() bool      { return k.busy }
func (k *book) Done() time.Time { return k.done }
func (k *book) Owed(info SessionInfo, newest string) bool {
	return k.owed != nil && k.owed(info, newest)
}

func served(t *testing.T) (*Broker, string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "agm-idle") // private: a socket directory must be 0700
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock, spool := filepath.Join(dir, "s.sock"), filepath.Join(dir, "spool.json")
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
	return b, sock, spool
}

// say dials, writes lines and returns the connection and every line that came back.
func say(t *testing.T, sock string, lines ...string) (net.Conn, string) {
	t.Helper()
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		if _, err := fmt.Fprintln(nc, l); err != nil {
			t.Fatal(err)
		}
	}
	var out strings.Builder
	sc := bufio.NewScanner(nc)
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	for range lines {
		if !sc.Scan() {
			break
		}
		out.WriteString(sc.Text() + "\n")
	}
	return nc, out.String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const protocolLine = "{\"id\":1,\"op\":\"protocol\"}"

// Every visit counts, also one that begins and ends between two daemon ticks, and a
// connection that said nothing yet keeps the broker from being idle.
func TestActivityCountsVisitsAndOpenConnections(t *testing.T) {
	b, sock, _ := served(t)
	idle := func() bool { return b.Idle(time.Now(), 0, nil) }
	if start, gen := activity(b); gen != 0 || start.After(time.Now()) || !idle() {
		t.Fatalf("fresh broker: start %s, finished %d, idle %v", start, gen, idle())
	}
	nc, out := say(t, sock, protocolLine)
	nc.Close()
	if !strings.Contains(out, "\"protocol\":2") {
		t.Fatalf("protocol: %q", out)
	}
	eventually(t, "the visit was not counted", func() bool { _, gen := activity(b); return gen == 1 })
	nc, _ = say(t, sock) // connected, nothing said
	eventually(t, "an anonymous connection is not work", func() bool { return !idle() })
	nc.Close()
	eventually(t, "the closed connection's handler did not finish", func() bool {
		_, gen := activity(b)
		return gen == 2 && idle()
	})
	watcher, _ := say(t, sock, "{\"id\":1,\"op\":\"hello\",\"session\":{\"id\":\"w\"},\"subscribe\":true}")
	eventually(t, "a watch-only subscriber is not work", func() bool { return !idle() })
	watcher.Close()
	eventually(t, "the watch-only handler did not finish", func() bool { return idle() })
}

func TestIdleProtectsAsksAndSpawnReservations(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "a")
	hello(t, b, "b", "b")
	now := time.Now()
	if _, _, err := b.Spawn("a", "newbie"); err != nil {
		t.Fatal(err)
	}
	if b.Idle(now.Add(time.Second), 0, nil) {
		t.Fatal("idle with an unexpired spawn reservation")
	}
	if !b.Idle(now.Add(spawnPendingTTL+time.Second), 0, nil) {
		t.Fatal("an expired reservation that was not purged keeps the daemon up")
	}
	asked := time.Now()
	if _, err := b.Send("a", SendReq{To: "b", Text: "question", ExpectsReply: true, NoWait: true}, nil); err != nil {
		t.Fatal(err)
	}
	if b.Idle(asked.Add(spawnPendingTTL-time.Second), 0, nil) {
		t.Fatal("idle with an unexpired ask")
	}
	if !b.Idle(asked.Add(24*time.Hour), 0, nil) {
		t.Fatal("an expired ask keeps the daemon up")
	}
}

// DrainIfIdle writes what memory holds, including a LastSeen a plain rebind did not
// write, and only then stops admitting: a late connection is closed unread. A save
// that fails leaves the broker open, and mail nobody is owed a wake for is not work.
func TestDrainSavesThenClosesTheDoor(t *testing.T) {
	b, sock, spool := served(t)
	b.Hello(SessionInfo{ID: "a", Name: "a"}, nil, false, false)
	time.Sleep(10 * time.Millisecond)
	b.Hello(SessionInfo{ID: "a", Name: "a"}, nil, false, false)
	if drained, err := b.DrainIfIdle(time.Now(), 0, nil); err != nil || !drained {
		t.Fatalf("drain: %v, %v", drained, err)
	}
	info, _ := b.Resolve("a")
	data, err := os.ReadFile(spool)
	if err != nil || !strings.Contains(string(data), info.LastSeen.Format(time.RFC3339Nano)) {
		t.Fatalf("the drain did not save the current state: %v, %s", err, data)
	}
	if nc, out := say(t, sock, protocolLine); out != "" {
		nc.Close()
		t.Fatalf("a late connection was read: %q", out)
	} else {
		nc.Close()
	}
	if again, err := b.DrainIfIdle(time.Now(), 0, nil); err != nil || again || b.Idle(time.Now(), 0, nil) {
		t.Fatalf("a drained broker is not idle and does not drain twice: %v, %v", again, err)
	}

	owedB, _, _ := served(t)
	owedB.Hello(SessionInfo{ID: "c", Name: "c", Harness: "codex"}, nil, false, false)
	hello(t, owedB, "a", "a")
	if _, err := owedB.Send("a", SendReq{To: "c", Text: "wake me"}, nil); err != nil {
		t.Fatal(err)
	}
	owed := func(info SessionInfo, newest string) bool { return info.ID == "c" && newest != "" }
	if drained, err := owedB.DrainIfIdle(time.Now(), 0, &book{owed: owed}); err != nil || drained {
		t.Fatalf("drained with a wake still owed: %v, %v", drained, err)
	}
	if drained, err := owedB.DrainIfIdle(time.Now(), 0, nil); err != nil || !drained {
		t.Fatalf("offline mail alone kept the daemon up: %v, %v", drained, err)
	}

	failing, failSock, failSpool := served(t)
	if err := os.Mkdir(failSpool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if drained, err := failing.DrainIfIdle(time.Now(), 0, nil); code(err) != CodePersistence || drained {
		t.Fatalf("a failed save must not drain: %v, %v", drained, err)
	}
	if nc, out := say(t, failSock, protocolLine); !strings.Contains(out, "\"protocol\":2") {
		nc.Close()
		t.Fatalf("a broker whose save failed no longer serves: %q", out)
	}
}

// The grace, a busy waker and a delivery that ends are rechecked under the broker
// lock: a time sampled before any of them is not enough to drain.
func TestDrainRechecksGraceAndWakesUnderTheLock(t *testing.T) {
	b, sock, _ := served(t)
	sampled := time.Now()
	nc, out := say(t, sock, protocolLine)
	nc.Close()
	if !strings.Contains(out, "\"protocol\":2") {
		t.Fatalf("protocol: %q", out)
	}
	eventually(t, "handler did not finish", func() bool { _, gen := activity(b); return gen == 1 })
	if drained, err := b.DrainIfIdle(sampled, time.Hour, nil); err != nil || drained {
		t.Fatalf("drained on a time sampled before the handler ended: %v, %v", drained, err)
	}
	if drained, err := b.DrainIfIdle(time.Now(), 0, nil); err != nil || !drained {
		t.Fatalf("did not drain once the grace is over: %v, %v", drained, err)
	}

	busy, _, _ := served(t)
	if drained, err := busy.DrainIfIdle(time.Now(), 0, &book{busy: true}); err != nil || drained {
		t.Fatalf("drained with a wake scheduled or in flight: %v, %v", drained, err)
	}
	late, _, _ := served(t)
	if drained, err := late.DrainIfIdle(time.Now(), 0, &book{done: time.Now().Add(time.Hour)}); err != nil || drained {
		t.Fatalf("drained although a delivery ends after the sampled time: %v, %v", drained, err)
	}
}

// Tail is the newest queued message: the marker Idle compares with.
func TestTailIsTheNewestQueuedMessage(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	first := sendMany(t, b, "one", "two", "three")
	if got := b.Tail("bob"); got != first[2] {
		t.Fatalf("tail %q, want %q", got, first[2])
	}
	if _, err := b.Ack("bob", []string{first[2]}); err != nil {
		t.Fatal(err)
	}
	if got := b.Tail("bob"); got != first[1] {
		t.Fatalf("tail after ack %q, want %q", got, first[1])
	}
	if _, err := b.Ack("bob", first[:2]); err != nil {
		t.Fatal(err)
	}
	b.Bye("bob")
	if got := b.Tail("bob"); got != "" {
		t.Fatalf("tail of a gone session %q", got)
	}
}
