package broker

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func msgIDs(msgs []*Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

// sendMany sends texts to bob and returns their ids in order.
func sendMany(t *testing.T, b *Broker, texts ...string) []string {
	t.Helper()
	var out []string
	for _, text := range texts {
		m, err := b.Send("alice", SendReq{To: "bob", Text: text}, nil)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m.ID)
	}
	return out
}

// A take or an ack that cannot be saved hands over nothing: the mailbox stays as it
// was, order included, in memory and on disk.
func TestTakeAndAckAreDurable(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	want := sendMany(t, b, "one", "two", "three")
	queued := func(b *Broker) []string {
		t.Helper()
		msgs, err := b.Inbox("bob")
		if err != nil {
			t.Fatal(err)
		}
		return msgIDs(msgs)
	}
	onDisk := func() []string {
		t.Helper()
		restarted, err := New(DefaultLimits(), spool)
		if err != nil {
			t.Fatal(err)
		}
		return queued(restarted)
	}
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if msgs, err := b.Take("bob", want[1]); code(err) != CodePersistence || msgs != nil {
		t.Fatalf("failed take: %+v, %v", msgs, err)
	}
	if got, err := b.Ack("bob", []string{want[0], want[2]}); code(err) != CodePersistence || got != nil {
		t.Fatalf("failed ack: %v, %v", got, err)
	}
	if got := queued(b); !slices.Equal(got, want) {
		t.Fatalf("failed writes changed the mailbox: %v", got)
	}
	if got := onDisk(); !slices.Equal(got, want) {
		t.Fatalf("restart after failed writes: %v", got)
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if msgs, err := b.Take("bob", want[1]); err != nil || !slices.Equal(msgIDs(msgs), want[1:2]) {
		t.Fatalf("take: %+v, %v", msgs, err)
	}
	if got, err := b.Ack("bob", []string{want[2]}); err != nil || !slices.Equal(got, want[2:]) {
		t.Fatalf("ack: %v, %v", got, err)
	}
	if got := queued(b); !slices.Equal(got, want[:1]) {
		t.Fatalf("after take and ack: %v", got)
	}
	if got := onDisk(); !slices.Equal(got, want[:1]) {
		t.Fatalf("restart lost the saved removals: %v", got)
	}
}

// A handoff keeps mail queued and on disk until the host has it. While it lasts the
// mail is invisible to inbox, take, ack and a new subscriber's replay; a restart
// replays it; a release brings it back in its old place; a commit removes it for good.
func TestHandoffKeepsMailUntilTheHostTookIt(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	want := sendMany(t, b, "one", "two", "three")
	queued := func(b *Broker) []string {
		t.Helper()
		msgs, err := b.Inbox("bob")
		if err != nil {
			t.Fatal(err)
		}
		return msgIDs(msgs)
	}
	onDisk := func() []string {
		t.Helper()
		restarted, err := New(DefaultLimits(), spool)
		if err != nil {
			t.Fatal(err)
		}
		return queued(restarted)
	}
	held, err := b.Hold("bob", want[0], want[1])
	if err != nil || !slices.Equal(msgIDs(held.Messages()), want[:2]) {
		t.Fatalf("hold: %+v, %v", held, err)
	}
	if got := queued(b); !slices.Equal(got, want[2:]) {
		t.Fatalf("inbox shows reserved mail: %v", got)
	}
	if msgs, err := b.Take("bob", want[0], want[1]); err != nil || len(msgs) != 0 {
		t.Fatalf("take got reserved mail: %+v, %v", msgs, err)
	}
	if got, err := b.Ack("bob", want[:2]); err != nil || len(got) != 0 {
		t.Fatalf("ack removed reserved mail: %v, %v", got, err)
	}
	if msgs, err := b.Take("bob"); err != nil || !slices.Equal(msgIDs(msgs), want[2:]) {
		t.Fatalf("take of the free mail: %+v, %v", msgs, err)
	}
	// Only the free mail is replayed to a new subscriber: the reserved mail is not.
	if late := hello(t, b, "bob", "bob"); late.len() != 0 {
		t.Fatalf("a new subscriber was replayed reserved mail: %d", late.len())
	}
	if got := onDisk(); !slices.Equal(got, want[:2]) {
		t.Fatalf("reserved mail is not on disk: %v", got)
	}
	held.Release()
	if got := queued(b); !slices.Equal(got, want[:2]) {
		t.Fatalf("after release: %v", got)
	}
	held, err = b.Hold("bob", want[:2]...)
	if err != nil {
		t.Fatal(err)
	}
	if err := held.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(queued(b)) != 0 || len(onDisk()) != 0 {
		t.Fatal("committed mail is still queued")
	}
}

// A commit that cannot be saved leaves the mail queued and free again. A handoff whose
// session was replaced meanwhile never touches the new session's mailbox.
func TestHandoffCommitAfterFailedWriteAndReplacement(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	want := sendMany(t, b, "one")
	queued := func() []string {
		t.Helper()
		msgs, err := b.Inbox("bob")
		if err != nil {
			t.Fatal(err)
		}
		return msgIDs(msgs)
	}
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	held, err := b.Hold("bob", want...)
	if err != nil {
		t.Fatal(err)
	}
	if err := held.Commit(); code(err) != CodePersistence {
		t.Fatalf("want persistence failure, got %v", err)
	}
	if got := queued(); !slices.Equal(got, want) {
		t.Fatalf("after a failed commit: %v", got)
	}
	again, err := b.Hold("bob", want...)
	if err != nil || len(again.Messages()) != 1 {
		t.Fatalf("mail stayed reserved after a failed commit: %+v, %v", again, err)
	}
	again.Release()
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	// The old session struct goes (as after MailTTL) and the id says hello again.
	held, err = b.Hold("bob", want...)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	delete(b.sessions, "bob")
	b.mu.Unlock()
	b.Hello(SessionInfo{ID: "bob", Name: "bob"}, nil, false, false)
	fresh := sendMany(t, b, "for the new session")
	if err := held.Commit(); err != nil {
		t.Fatalf("stale commit: %v", err)
	}
	held.Release()
	if got := queued(); !slices.Equal(got, fresh) {
		t.Fatalf("a stale handoff changed the new session's mailbox: %v", got)
	}
}

// Hooks (take) and wakes (hold, then commit or release) racing on one mailbox deliver
// every message at most once between them.
func TestHandoffRacesWithTakers(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	all := map[string]bool{}
	var texts []string
	for i := 0; i < 60; i++ {
		texts = append(texts, "m"+strconv.Itoa(i))
	}
	for _, id := range sendMany(t, b, texts...) {
		all[id] = true
	}
	var mu sync.Mutex
	delivered := map[string]int{}
	count := func(id string) {
		mu.Lock()
		delivered[id]++
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(hook bool) {
			defer wg.Done()
			for n := 0; n < 400; n++ {
				msgs, err := b.Inbox("bob")
				if err != nil || len(msgs) == 0 {
					continue
				}
				if hook {
					taken, _ := b.Take("bob", msgs[0].ID)
					for _, m := range taken {
						count(m.ID)
					}
					continue
				}
				held, err := b.Hold("bob", msgs[0].ID)
				if err != nil || len(held.Messages()) == 0 {
					continue
				}
				if n%2 == 0 { // the host refused
					held.Release()
					continue
				}
				if err := held.Commit(); err == nil {
					for _, m := range held.Messages() {
						count(m.ID)
					}
				}
			}
		}(w%2 == 0)
	}
	wg.Wait()
	left, err := b.Inbox("bob")
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, m := range left {
		if delivered[m.ID] != 0 {
			t.Fatalf("message %s was delivered and is still queued", m.ID)
		}
		seen++
	}
	for id, n := range delivered {
		if n != 1 || !all[id] {
			t.Fatalf("message %s delivered %d times", id, n)
		}
		seen++
	}
	if seen != len(all) {
		b.mu.Lock()
		held, queued := len(b.sessions["bob"].held), len(b.sessions["bob"].mailbox)
		b.mu.Unlock()
		t.Fatalf("%d of %d messages accounted for: %d queued in memory, %d reserved, %d delivered", seen, len(all), queued, held, len(delivered))
	}
}

// Two handoffs on one session are independent: a commit removes only its own messages,
// so a message whose host refused, released afterwards, is still queued.
func TestHandoffsAreIndependent(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	want := sendMany(t, b, "one", "two", "three")
	first, err := b.Hold("bob", want[0], want[1])
	if err != nil || !slices.Equal(msgIDs(first.Messages()), want[:2]) {
		t.Fatalf("first hold: %+v, %v", first, err)
	}
	second, err := b.Hold("bob", want[2])
	if err != nil || !slices.Equal(msgIDs(second.Messages()), want[2:]) {
		t.Fatalf("second hold: %+v, %v", second, err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	second.Release()
	if got, _ := b.Inbox("bob"); !slices.Equal(msgIDs(got), want[2:]) {
		t.Fatalf("after one commit and one release: %v", msgIDs(got))
	}
}
