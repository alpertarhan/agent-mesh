package broker

import (
	"fmt"
	"testing"
)

func TestTakeIDsSelectionOrderAndOwnership(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	hello(t, b, "carol", "carol")
	messages := make([]*Message, 3)
	for i := range messages {
		var err error
		messages[i], err = b.Send("alice", SendReq{To: "bob", Text: fmt.Sprint(i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if stolen, err := b.Take("carol", messages[0].ID); err != nil || len(stolen) != 0 {
		t.Fatalf("foreign take: %+v, %v", stolen, err)
	}
	taken, err := b.Take("bob", messages[2].ID, messages[0].ID, messages[0].ID, "unknown")
	if err != nil || len(taken) != 2 || taken[0].ID != messages[0].ID || taken[1].ID != messages[2].ID {
		t.Fatalf("selected take changed mailbox order: %+v, %v", taken, err)
	}
	left, err := b.Inbox("bob")
	if err != nil || len(left) != 1 || left[0].ID != messages[1].ID {
		t.Fatalf("unselected message lost: %+v, %v", left, err)
	}
}

func TestTakeIDsConcurrentClaim(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	m, err := b.Send("alice", SendReq{To: "bob", Text: "one taker"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		messages []*Message
		err      error
	}
	// Buffered to let every worker finish even if an assertion stops the reader early.
	results := make(chan result, 16)
	for range cap(results) {
		go func() {
			messages, err := b.Take("bob", m.ID)
			results <- result{messages: messages, err: err}
		}()
	}
	count := 0
	for range cap(results) {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		count += len(r.messages)
	}
	if count != 1 {
		t.Fatalf("concurrent claim returned %d copies", count)
	}
}

func TestLegacyTakeRemovesRequeuedDuplicateEntries(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	m, err := b.Send("alice", SendReq{To: "bob", Text: "legacy requeue"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.Requeue("bob", []*Message{m})
	taken, err := b.Take("bob")
	if err != nil || len(taken) != 2 {
		t.Fatalf("legacy take changed: %+v, %v", taken, err)
	}
	if left, err := b.Inbox("bob"); err != nil || len(left) != 0 {
		t.Fatalf("legacy take left returned entries queued: %+v, %v", left, err)
	}
}
