package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSendKeyConcurrentReplayDoesNotPublishOrConsumeLimits(t *testing.T) {
	for _, ask := range []bool{false, true} {
		t.Run(fmt.Sprint("ask=", ask), func(t *testing.T) {
			b := newBroker(t, func(l *Limits) {
				l.SendBurst, l.SendPerMin, l.MailboxCap, l.AsksInFlight = 1, 0, 1, 1
			})
			hello(t, b, "alice", "alice")
			bob := hello(t, b, "bob", "bob")
			req := SendReq{To: "bob", Text: "once", RequestKey: "job-1", ExpectsReply: ask, NoWait: ask}
			type result struct {
				message *Message
				err     error
			}
			// Each worker can finish even if an assertion stops the receiver early.
			results := make(chan result, 32)
			for range cap(results) {
				go func() {
					m, err := b.Send("alice", req, nil)
					results <- result{message: m, err: err}
				}()
			}
			var original *Message
			for i := range cap(results) {
				r := <-results
				if r.err != nil {
					t.Fatalf("retry %d: %v", i, r.err)
				}
				if original == nil {
					original = r.message
				}
				if !reflect.DeepEqual(r.message, original) {
					t.Fatalf("retry %d returned another message", i)
				}
			}
			if bob.len() != 1 || len(b.history) != 1 || len(b.sendKeys) != 1 || b.sessions["alice"].tokens != 0 {
				t.Fatal("retries changed delivery/history/ledger/rate state")
			}
			if ask {
				if len(b.asks) != 1 || !b.asks[original.ID].Deadline.Equal(original.At.Add(b.lim.AskTimeout)) {
					t.Fatal("retry renewed or duplicated the pending ask")
				}
				b.mu.Lock()
				b.dropAsk(original.ID)
				b.mu.Unlock()
			}
		})
	}
}

func TestSendKeyConflictAndSenderScope(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	bob := hello(t, b, "bob", "bob")
	hello(t, b, "carol", "carol")
	req := SendReq{To: "bob", Text: "original", RequestKey: "same-key"}
	original, err := b.Send("alice", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	changes := []SendReq{req, req, req, req, req}
	changes[0].Text = "changed"
	changes[1].To = "carol"
	changes[2].ReplyTo = original.ID
	changes[3].Attachments = []Attachment{{Type: "snippet", Name: "code", Content: "changed"}}
	changes[4].ExpectsReply, changes[4].NoWait = true, true
	for _, changed := range changes {
		if m, err := b.Send("alice", changed, nil); code(err) != CodeKeyConflict || m != nil {
			t.Fatalf("changed request = %v, %v; want key_conflict", m, err)
		}
	}
	other, err := b.Send("carol", req, nil)
	if err != nil || other.ID == original.ID {
		t.Fatalf("key crossed sender scope: %v, %v", other, err)
	}
	if bob.len() != 2 || len(b.history) != 2 {
		t.Fatal("conflicting request was published")
	}
}

func TestSendKeySurvivesAckHistoryEvictionRecipientRemovalAndRestart(t *testing.T) {
	lim := DefaultLimits()
	lim.MailboxCap, lim.SendBurst = HistoryMax+2, HistoryMax+2
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(lim, spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	req := SendReq{
		To: "bob", Text: "original body", RequestKey: "persisted-key",
		Attachments: []Attachment{{Type: "snippet", Name: "source", Content: "original attachment"}},
	}
	original, err := b.Send("alice", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Ack("bob", []string{original.ID}); err != nil {
		t.Fatal(err)
	}
	for range HistoryMax + 1 {
		if _, err := b.Send("alice", SendReq{To: "bob", Text: "history filler"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Show("alice", original.ID); code(err) != CodeUnknownMsg {
		t.Fatalf("fixture did not evict message body: %v", err)
	}
	if _, err := b.Take("bob"); err != nil {
		t.Fatal(err)
	}
	b.Bye("bob")
	restored, err := New(lim, spool)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restored.Send("alice", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("replay changed original receipt/body: %s != %s", got, want)
	}
	if _, exists := restored.sessions["bob"]; exists {
		t.Fatal("replay recreated the recipient")
	}
	for _, entry := range restored.sendKeys {
		if entry.Message.Text != "" || len(entry.Message.Attachments) != 0 {
			t.Fatal("ledger retained message bodies")
		}
	}
}

func TestSendKeyReplyDoesNotRepublishOrNeedOldRouting(t *testing.T) {
	b := newBroker(t, nil)
	alice := hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	question, err := b.Send("alice", SendReq{To: "bob", Text: "question", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	watcher := &sink{}
	if _, err := b.Wait("alice", question.ID, watcher); err != nil {
		t.Fatal(err)
	}
	req := SendReq{ReplyTo: question.ID, Text: "answer", RequestKey: "answer-key"}
	original, err := b.Send("bob", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	delete(b.seen, question.ID) // reply routing can expire independently of accepted keys
	if replay, err := b.Send("bob", req, nil); err != nil || !reflect.DeepEqual(replay, original) {
		t.Fatalf("reply replay: %v, %v", replay, err)
	}
	if alice.len() != 1 || watcher.len() != 1 || len(b.asks) != 0 || len(b.history) != 2 {
		t.Fatal("reply retry published, notified, or changed ask/history state twice")
	}
}

func TestSendKeyFailedPersistenceDoesNotReserveKey(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	bob := hello(t, b, "bob", "bob")
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	req := SendReq{To: "bob", Text: "not accepted", RequestKey: "recovered-key"}
	if _, err := b.Send("alice", req, nil); code(err) != CodePersistence {
		t.Fatalf("want failed persistence, got %v", err)
	}
	if len(b.sendKeys) != 0 || bob.len() != 0 {
		t.Fatal("failed send reserved key or published")
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	req.Text = "accepted after recovery"
	original, err := b.Send("alice", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := restored.Send("alice", req, nil); err != nil || replay.ID != original.ID {
		t.Fatalf("durable retry: %v, %v", replay, err)
	}
	if bob.len() != 1 {
		t.Fatal("recovery did not publish exactly once")
	}
}

func TestSendKeyValidationAndBoundedWindow(t *testing.T) {
	b := newBroker(t, func(l *Limits) {
		l.MailboxCap, l.SendBurst = sendKeyCap+2, sendKeyCap+2
	})
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	for _, key := range []string{strings.Repeat("x", 129), "two words", "bad\nkey", "bad\x00key", "bad\u200bkey", "\xff"} {
		if _, err := b.Send("alice", SendReq{To: "bob", Text: "invalid", RequestKey: key}, nil); code(err) != CodeBadRequest {
			t.Fatalf("invalid key %q: %v", key, err)
		}
	}
	if _, err := b.Send("alice", SendReq{To: "bob", Text: "blocking", RequestKey: "blocking", ExpectsReply: true}, nil); code(err) != CodeBadRequest {
		t.Fatalf("keyed blocking ask: %v", err)
	}
	first, err := b.Send("alice", SendReq{To: "bob", Text: "first", RequestKey: "first"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range sendKeyCap {
		if _, err := b.Send("alice", SendReq{To: "bob", Text: "next", RequestKey: fmt.Sprint(i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.sendKeys) != sendKeyCap || len(b.sendKeyRing) != sendKeyCap {
		t.Fatal("ledger exceeded its bounded window")
	}
	fresh, err := b.Send("alice", SendReq{To: "bob", Text: "first", RequestKey: "first"}, nil)
	if err != nil || fresh.ID == first.ID {
		t.Fatalf("oldest key was not evicted: %v, %v", fresh, err)
	}
}

func TestSendKeyCapability(t *testing.T) {
	b := newBroker(t, nil)
	self := ""
	result, err := dispatch(b, nil, &self, &Request{Op: "protocol"})
	if err != nil {
		t.Fatal(err)
	}
	caps := result.(map[string]int)
	if caps["protocol"] != 2 || caps["idempotency"] != 1 {
		t.Fatalf("capabilities = %v", caps)
	}
	// Replay does not renew timestamps even if the broker clock advances.
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	req := SendReq{To: "bob", Text: "timestamp", RequestKey: "clock"}
	m, err := b.Send("alice", req, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return m.At.Add(time.Hour) }
	if replay, err := b.Send("alice", req, nil); err != nil || !replay.At.Equal(m.At) {
		t.Fatalf("replay changed original timestamp: %v, %v", replay, err)
	}
}
