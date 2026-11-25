package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/codex"
)

// wakerBroker is a broker on a private socket with no daemon attached, so tests
// decide which wakes run.
func wakerBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir, err := os.MkdirTemp("", "agm-wake")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	ln, err := broker.Listen(filepath.Join(dir, "s.sock"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.New(broker.DefaultLimits(), filepath.Join(dir, "spool.json"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
	return b
}

func join(t *testing.T, b *broker.Broker, id, harness string) broker.SessionInfo {
	t.Helper()
	if err := b.Hello(broker.SessionInfo{ID: id, Name: id, Harness: harness}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	info, err := b.Resolve(id)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// queue leaves messages for id and returns their ids.
func queue(t *testing.T, b *broker.Broker, from, to string, texts ...string) []string {
	t.Helper()
	join(t, b, from, "shell")
	ids := make([]string, len(texts))
	for i, text := range texts {
		m, err := b.Send(from, broker.SendReq{To: to, Text: text}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = m.ID
	}
	return ids
}

// refusedWaker returns a waker whose Codex host refuses every delivery.
func refusedWaker(t *testing.T, b *broker.Broker, onDeliver func()) *waker {
	t.Helper()
	old := codexDeliver
	t.Cleanup(func() { codexDeliver = old })
	codexDeliver = func(id, text string) error {
		if onDeliver != nil {
			onDeliver()
		}
		return codex.ErrNotLoaded
	}
	return &waker{b: b}
}

// A wake deals with the queue as it stood when the wake read it. The marker Idle
// compares with is the real tail of the mailbox, so a queue longer than one frame
// is still settled after a refusal, and a message that arrives during the refusal
// stays owed.
func TestWakerSettlesOnTheTailReadBeforeTheAttempt(t *testing.T) {
	b := wakerBroker(t)
	c1 := join(t, b, "c1", "codex")
	peer := join(t, b, "peer", "shell")
	_ = peer
	ids := queue(t, b, "peer", "c1", "first", "second")
	w := refusedWaker(t, b, func() {
		queue(t, b, "peer", "c1", "arrived during the refusal")
	})
	w.wake(c1)
	if w.Owed(c1, ids[1]) {
		t.Fatal("the refused queue is still owed")
	}
	tail := b.Tail("c1")
	if tail == ids[1] {
		t.Fatal("the message queued during the refusal did not arrive")
	}
	if !w.Owed(c1, tail) {
		t.Fatalf("the message that arrived during the refusal is not owed any more")
	}
	if !w.Done().IsZero() {
		t.Fatalf("a refused wake counts as work: %v", w.Done())
	}
}

// A queue longer than one delivery frame is settled by its real tail.
func TestWakerSettlesALongQueueByItsTail(t *testing.T) {
	b := wakerBroker(t)
	c1 := join(t, b, "c1", "codex")
	long := strings.Repeat("x", 7000)
	ids := queue(t, b, "peer", "c1", long, long, long, long)
	if got := b.Tail("c1"); got != ids[3] {
		t.Fatalf("tail %q, want %q", got, ids[3])
	}
	w := refusedWaker(t, b, nil)
	w.wake(c1)
	if w.Owed(c1, ids[3]) {
		t.Fatal("a long refused queue is still owed")
	}
	// The next sweep delivers the rest: the host takes the mail this time.
	codexDeliver = func(id, text string) error { return nil }
	w.wake(c1)
	if mail, _ := b.Inbox("c1"); len(mail) != 0 {
		t.Fatalf("%d messages left after the second wake", len(mail))
	}
	if !b.Idle(time.Now(), 0, w) {
		t.Fatal("idle after the delivery: the queue is not settled")
	}
}

// Scheduled counts from the call, inflight until the goroutine ended.
func TestWakerBusyFromScheduleUntilDone(t *testing.T) {
	b := wakerBroker(t)
	c1 := join(t, b, "c1", "codex")
	queue(t, b, "peer", "c1", "held")
	w := refusedWaker(t, b, nil)
	w.wake(c1) // refused: the queue is settled, so from here only the wake itself holds the daemon
	if w.Owed(c1, b.Tail("c1")) {
		t.Fatal("setup: the refused queue is still owed")
	}
	proceed := make(chan struct{})
	codexDeliver = func(id, text string) error { <-proceed; return nil }
	w.schedule(c1)
	if !w.Busy() {
		t.Fatal("a scheduled wake is not busy")
	}
	drained, err := b.DrainIfIdle(time.Now(), 0, w)
	if err != nil || drained {
		t.Fatalf("drained under a scheduled wake: %v, %v", drained, err)
	}
	close(proceed)
	for deadline := time.Now().Add(3 * time.Second); w.Busy(); {
		if time.Now().After(deadline) {
			t.Fatal("the wake never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if w.Done().IsZero() {
		t.Fatal("a delivered wake left no trace")
	}
}

// A wake for mail that was taken in the meantime ends without counting as work.
func TestWakerForMailThatIsGoneEndsQuietly(t *testing.T) {
	b := wakerBroker(t)
	c1 := join(t, b, "c1", "codex")
	queue(t, b, "peer", "c1", "taken")
	w := refusedWaker(t, b, nil)
	if taken, err := b.Take("c1"); err != nil || len(taken) != 1 {
		t.Fatal(err)
	}
	w.wake(c1)
	if w.Busy() || !w.Done().IsZero() {
		t.Fatalf("busy %v, done %v", w.Busy(), w.Done())
	}
}

func TestOwedSkipsLinkSessions(t *testing.T) {
	b := wakerBroker(t)
	w := &waker{b: b}
	w.settled = map[string]string{"host/c1": "m1"}
	if w.Owed(broker.SessionInfo{ID: "host/c1", Harness: "codex"}, "m1") {
		t.Fatal("a link session is owed a wake")
	}
	w.nudged = map[string]string{"host/c2": "m2"}
	if w.Owed(broker.SessionInfo{ID: "host/c2", Harness: "codex"}, "m2") {
		t.Fatal("a link session is owed a nudge")
	}
}

func TestWakerForgetsSessionsThatAreGone(t *testing.T) {
	b := wakerBroker(t)
	c1 := join(t, b, "c1", "codex")
	ids := queue(t, b, "peer", "c1", "bye")
	w := refusedWaker(t, b, nil)
	w.wake(c1)
	if err := b.Hello(broker.SessionInfo{ID: "crush", Name: "crush", Harness: "crush", Pane: "pane-1", PID: os.Getpid()}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	queue(t, b, "peer", "crush", "bye too")
	// both paths leave markers: one refused Codex wake, one nudge that is refused
	oldGet, oldRun := herdrOutput, herdrRun
	herdrOutput = func(args ...string) ([]byte, error) {
		return []byte("{\"result\":{\"pane\":{\"agent\":\"crush\",\"agent_status\":\"idle\",\"focused\":false,\"tab_id\":\"tab-1\",\"workspace_id\":\"ws-1\"}}}"), nil
	}
	herdrRun = func(args ...string) error { return nil } // typed
	t.Cleanup(func() { herdrOutput, herdrRun = oldGet, oldRun })
	w.wake(mustResolve(t, b, "crush"))
	if len(w.settled) != 2 || len(w.nudged) != 1 {
		t.Fatalf("markers before prune: settled %v, nudged %v", w.settled, w.nudged)
	}
	for _, id := range []string{"c1", "crush"} {
		if _, err := b.Take(id); err != nil {
			t.Fatal(err)
		}
	}
	b.Ack("c1", ids)
	b.Ack("crush", mustMailIDs(t, b, "crush"))
	b.Bye("c1")
	b.Bye("crush")
	w.prune()
	if len(w.settled) != 0 || len(w.nudged) != 0 {
		t.Fatalf("markers after prune: settled %v, nudged %v", w.settled, w.nudged)
	}
}

func mustResolve(t *testing.T, b *broker.Broker, id string) broker.SessionInfo {
	t.Helper()
	info, err := b.Resolve(id)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func mustMailIDs(t *testing.T, b *broker.Broker, id string) []string {
	t.Helper()
	msgs, err := b.Inbox(id)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}
