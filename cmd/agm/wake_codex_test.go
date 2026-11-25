package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/codex"
)

// spooled serves an isolated broker like testBroker, with a spool to restart from.
func spooledBroker(t *testing.T) (*broker.Broker, string, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "agm-wake")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock, spool := filepath.Join(dir, "s.sock"), filepath.Join(dir, "spool.json")
	ln, err := broker.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	b, err := broker.New(broker.DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
	return b, sock, spool
}

// The Codex wake does not remove mail before the host has it. A host that refuses
// leaves it queued and on disk; a hook firing on the started turn cannot take it
// meanwhile; a crash on the way replays it; after acceptance it leaves the queue once,
// and once more only when that removal cannot be saved.
func TestWakeCodexHandsOffWithoutRemovingFirst(t *testing.T) {
	b, sock, spool := spooledBroker(t)
	b.Hello(broker.SessionInfo{ID: "peer", Name: "peer"}, nil, false, false)
	info := broker.SessionInfo{ID: "codex-1", Harness: "codex"}
	b.Hello(broker.SessionInfo{ID: info.ID, Name: "coder", Harness: "codex"}, nil, false, false)
	var deliver func(id, text string) error
	old := codexDeliver
	codexDeliver = func(id, text string) error { return deliver(id, text) }
	t.Cleanup(func() { codexDeliver = old })
	w := &waker{b: b, inflight: map[string]bool{}, nudged: map[string]string{}}
	queued := func(b *broker.Broker) []string {
		t.Helper()
		msgs, err := b.Inbox(info.ID)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, m := range msgs {
			ids = append(ids, m.ID)
		}
		return ids
	}
	onDisk := func() []string {
		t.Helper()
		restarted, err := broker.New(broker.DefaultLimits(), spool)
		if err != nil {
			t.Fatal(err)
		}
		return queued(restarted)
	}
	send := func(text string) string {
		t.Helper()
		m, err := b.Send("peer", broker.SendReq{To: info.ID, Text: text}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return m.ID
	}
	is := func(got []string, want ...string) bool { return slices.Equal(got, want) }

	// The host refuses: nothing was removed, in memory or on disk.
	first := send("first")
	deliver = func(id, text string) error {
		if id != info.ID || !strings.Contains(text, "first") {
			t.Errorf("delivered %q to %s", text, id)
		}
		return errors.New("app-server down")
	}
	w.wake(info)
	if !is(queued(b), first) || !is(onDisk(), first) {
		t.Fatalf("refused mail is not queued as before: %v / %v", queued(b), onDisk())
	}
	deliver = func(id, text string) error { return codex.ErrNotLoaded }
	w.wake(info)
	if !is(queued(b), first) {
		t.Fatalf("mail for an unloaded thread: %v", queued(b))
	}

	// A hook on the started turn cannot take or see what the wake is handing over.
	deliver = func(id, text string) error {
		hook := dialSession(t, sock, info.ID, "coder", "codex", false)
		var taken, seen []*broker.Message
		if err := hook.call("take", broker.Request{IDs: []string{first}}, &taken); err != nil {
			t.Fatal(err)
		}
		if err := hook.call("inbox", broker.Request{}, &seen); err != nil {
			t.Fatal(err)
		}
		if len(taken) != 0 || len(seen) != 0 {
			t.Fatalf("the hook got the mail being delivered: taken %+v, seen %+v", taken, seen)
		}
		return nil // the host accepted the turn
	}
	w.wake(info)
	if len(queued(b)) != 0 || len(onDisk()) != 0 {
		t.Fatalf("accepted mail is still queued: %v / %v", queued(b), onDisk())
	}

	// The host accepted, but the removal could not be saved: the mail stays queued, free
	// again, and is delivered once more.
	second := send("second")
	deliveries := 0
	deliver = func(id, text string) error { deliveries++; return nil }
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	w.wake(info)
	if deliveries != 1 || !is(queued(b), second) {
		t.Fatalf("after %d deliveries, queued: %v", deliveries, queued(b))
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	w.wake(info)
	if deliveries != 2 || len(queued(b)) != 0 || len(onDisk()) != 0 {
		t.Fatalf("after %d deliveries, queued: %v / %v", deliveries, queued(b), onDisk())
	}

	// The daemon dies while the host is being contacted: the spool still holds the mail.
	third := send("third")
	started, proceed := make(chan struct{}), make(chan struct{})
	deliver = func(id, text string) error { close(started); <-proceed; return nil }
	done := make(chan struct{})
	go func() { w.wake(info); close(done) }()
	<-started
	if !is(onDisk(), third) {
		t.Fatalf("mail left the spool before the host took it: %v", onDisk())
	}
	close(proceed)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("wake did not finish")
	}
	if len(queued(b)) != 0 || len(onDisk()) != 0 {
		t.Fatalf("accepted mail is still queued: %v / %v", queued(b), onDisk())
	}
}
