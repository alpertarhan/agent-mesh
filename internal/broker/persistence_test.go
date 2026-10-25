package broker

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestFailedSendDoesNotPublishOrMutateState(t *testing.T) {
	for _, kind := range []string{"message", "ask", "reply"} {
		t.Run(kind, func(t *testing.T) {
			spool := filepath.Join(t.TempDir(), "spool.json")
			b, err := New(DefaultLimits(), spool)
			if err != nil {
				t.Fatal(err)
			}
			alice := hello(t, b, "alice", "alice")
			bob := hello(t, b, "bob", "bob")
			from := "alice"
			req := SendReq{To: "bob", Text: "must survive restart", ExpectsReply: kind == "ask"}
			if kind == "reply" {
				q, err := b.Send("alice", SendReq{To: "bob", Text: "question", ExpectsReply: true}, alice)
				if err != nil {
					t.Fatal(err)
				}
				from = "bob"
				req = SendReq{ReplyTo: q.ID, Text: "answer"}
			}
			before, err := os.ReadFile(spool)
			if err != nil {
				t.Fatal(err)
			}
			history := slices.Clone(b.history)
			seen := maps.Clone(b.seen)
			asks := len(b.asks)
			tokens := b.sessions[from].tokens
			a, z := alice.len(), bob.len()
			if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
				t.Fatal(err)
			}
			m, err := b.Send(from, req, nil)
			if code(err) != CodePersistence || m != nil {
				t.Fatalf("send = %v, %v; want persistence_failed without a message", m, err)
			}
			if alice.len() != a || bob.len() != z {
				t.Fatal("failed send reached a subscriber or blocking asker")
			}
			if !reflect.DeepEqual(history, b.history) || !maps.Equal(seen, b.seen) || len(b.asks) != asks {
				t.Fatal("failed send mutated history, reply routing, or pending asks")
			}
			if b.sessions[from].tokens < tokens {
				t.Fatal("failed send consumed rate allowance")
			}
			after, err := os.ReadFile(spool)
			if err != nil || string(after) != string(before) {
				t.Fatalf("failed send changed durable state: %v", err)
			}
			if err := os.Remove(spool + ".tmp"); err != nil {
				t.Fatal(err)
			}
			m, err = b.Send(from, req, nil)
			if err != nil {
				t.Fatalf("retry after recovery: %v", err)
			}
			restored, err := New(DefaultLimits(), spool)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := restored.Show(from, m.ID); err != nil {
				t.Fatalf("successful retry did not survive restart: %v", err)
			}
		})
	}
}

type durableSink struct {
	sink
	t     *testing.T
	spool string
	key   string
}

func (s *durableSink) Push(m *Message) bool {
	restored, err := New(DefaultLimits(), s.spool)
	if err != nil {
		s.t.Errorf("load during push: %v", err)
		return false
	}
	if _, err := restored.Show(m.To, m.ID); err != nil {
		s.t.Errorf("message was pushed before durable storage: %v", err)
	}
	if s.key != "" && restored.sendKeys[m.From+"\x00"+s.key].Message.ID != m.ID {
		s.t.Error("message was pushed before its retry key was committed")
	}
	queued, err := restored.Inbox(m.To)
	if err != nil || !slices.ContainsFunc(queued, func(q *Message) bool { return q.ID == m.ID }) {
		s.t.Errorf("message not queued durably before push: %v", err)
	}
	return s.sink.Push(m)
}

func TestSendPersistsBeforeDelivery(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	target := &durableSink{t: t, spool: spool, key: "durable-send"}
	if err := b.Hello(SessionInfo{ID: "bob"}, target, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send("alice", SendReq{To: "bob", Text: "durable", RequestKey: "durable-send"}, nil); err != nil {
		t.Fatal(err)
	}
	if target.len() != 1 {
		t.Fatal("message not delivered")
	}
}
