package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTrackFollowsCommittedEvidence(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	load := func() *Broker {
		t.Helper()
		b, err := New(DefaultLimits(), spool)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	b := load()
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	ids := sendMany(t, b, "take me", "ack me", "host delivery")
	id := ids[0]
	check := func(id, stage string, queued, seen bool) {
		t.Helper()
		r, err := b.Track("alice", id, false)
		if err != nil || r.Queued != queued || (r.SeenAt != nil) != seen || r.Delivery != stage {
			t.Fatalf("status: %+v %v", r, err)
		}
	}
	if _, err := b.Track("alice", id, true); code(err) != CodeUnknownMsg {
		t.Fatalf("sender marked seen: %v", err)
	}
	if _, err := b.Track("stranger", id, false); code(err) != CodeUnknownMsg {
		t.Fatalf("foreign track: %v", err)
	}
	if err := os.Mkdir(spool+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Track("bob", id, true); code(err) != CodePersistence {
		t.Fatal(err)
	}
	if _, err := b.Take("bob", id); code(err) != CodePersistence {
		t.Fatal(err)
	}
	check(id, "unknown", true, false)
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	r, err := b.Track("bob", id, true)
	if err != nil {
		t.Fatal(err)
	}
	b = load()
	again, err := b.Track("bob", id, true)
	if err != nil || !again.SeenAt.Equal(*r.SeenAt) {
		t.Fatal(again, err)
	}
	check(id, "unknown", true, true)
	msgs, err := b.Take("bob", id)
	if err != nil {
		t.Fatal(err)
	}
	b.Requeue("bob", msgs)
	check(id, "taken", true, true)
	if _, err := b.Ack("bob", []string{ids[1]}); err != nil {
		t.Fatal(err)
	}
	h, err := b.Hold("bob", ids[2])
	if err != nil {
		t.Fatal(err)
	}
	check(ids[2], "unknown", true, false)
	if err := h.Commit(); err != nil {
		t.Fatal(err)
	}
	reply, err := b.Send("bob", SendReq{ReplyTo: id, Text: "done"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b = load()
	check(ids[1], "acked", false, false)
	check(ids[2], "host_accepted", false, false)
	got, err := b.Track("alice", id, false)
	if err != nil || got.ReplyID != reply.ID {
		t.Fatal(got, err)
	}
}
