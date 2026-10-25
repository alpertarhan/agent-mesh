package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPresenceFreshnessAndIsolation(t *testing.T) {
	b := newBroker(t, nil)
	now := time.Now()
	b.now = func() time.Time { return now }
	owner := hello(t, b, "a", "alice")
	ttft := 500.0
	p := &Presence{
		State: "busy", Provider: "provider", Model: "model", ActiveTools: []string{"bash"},
		Context: &ContextUsage{Tokens: 500, Window: 1000},
		Metrics: &RequestMetrics{OutputTokens: 100, DurationMS: 2500, TTFTMS: &ttft, TTFTSource: "native", ObservedAt: now},
	}
	if err := b.Observe("a", p, true, owner); err != nil {
		t.Fatal(err)
	}
	info, _ := b.Resolve("a")
	if info.Activity != "busy" || info.Presence.Stale || *info.Presence.Metrics.TPS != 50 {
		t.Fatalf("bad observation: %+v", info)
	}
	first := info.Presence.ProgressAt
	// Neither caller-owned input nor returned slices/pointers may alter broker state.
	p.ActiveTools[0] = "mutated"
	info.Presence.Context.Tokens = 999
	*info.Presence.Metrics.TTFTMS = 999
	info, _ = b.Resolve("a")
	if info.Presence.ActiveTools[0] != "bash" || info.Presence.Context.Tokens != 500 || *info.Presence.Metrics.TTFTMS != 500 {
		t.Fatal("presence aliases caller memory")
	}
	now = now.Add(10 * time.Second)
	if err := b.Observe("a", info.Presence, false, owner); err != nil {
		t.Fatal(err)
	}
	info, _ = b.Resolve("a")
	if !info.Presence.ProgressAt.Equal(first) || !info.Presence.UpdatedAt.Equal(now) {
		t.Fatal("heartbeat pretended to make work progress")
	}
	b.Detach("a", &sink{}) // transient CLI connections do not invalidate the adapter
	info, _ = b.Resolve("a")
	if info.Presence.Stale {
		t.Fatal("unrelated disconnect invalidated observation")
	}
	now = now.Add(PresenceTTL)
	info, _ = b.Resolve("a")
	if info.Activity != "unknown" || !info.Presence.Stale {
		t.Fatal("expired presence was presented as live work")
	}
	if err := b.Observe("a", &Presence{State: "idle"}, true, owner); err != nil {
		t.Fatal(err)
	}
	info, _ = b.Resolve("a")
	if info.Activity != "idle" || info.Presence.Metrics != nil || !info.Presence.StateSince.Equal(now) {
		t.Fatal("new full observation retained old state/metrics")
	}
	b.Detach("a", owner)
	info, _ = b.Resolve("a")
	if info.Activity != "unknown" || !info.Presence.Stale {
		t.Fatal("disconnected observation not marked stale")
	}
}

func TestPresenceEphemeralAndTaskDurable(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	owner := hello(t, b, "a", "alice")
	before, _ := os.ReadFile(spool)
	if err := b.Observe("a", &Presence{State: "busy", Model: "private-model"}, true, owner); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(spool)
	if string(before) != string(after) {
		t.Fatal("observation rewrote the spool")
	}
	task := &TaskInfo{Summary: "review", Scope: []string{"/work/auth"}, WaitingFor: "peer reply"}
	if err := b.SetTask("a", task); err != nil {
		t.Fatal(err)
	}
	after, _ = os.ReadFile(spool)
	if strings.Contains(string(after), "private-model") {
		t.Fatal("a later checkpoint persisted transient presence")
	}
	task.Scope[0] = "/mutated"
	restored, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := restored.Resolve("a")
	if info.Activity != "unknown" || info.Presence != nil || info.Task.Summary != "review" || info.Task.Scope[0] != "/work/auth" {
		t.Fatalf("bad restart state: %+v", info)
	}
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("a", &TaskInfo{Summary: "failed update"}); code(err) != CodePersistence {
		t.Fatalf("want persistence failure, got %v", err)
	}
	info, _ = b.Resolve("a")
	if info.Task.Summary != "review" {
		t.Fatal("failed task update was not rolled back")
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("a", &TaskInfo{}); err != nil {
		t.Fatal(err)
	}
	restored, _ = New(DefaultLimits(), spool)
	info, _ = restored.Resolve("a")
	if info.Task != nil {
		t.Fatal("task clear did not survive restart")
	}
}

func TestPresenceValidationAndBinding(t *testing.T) {
	b := newBroker(t, nil)
	owner := hello(t, b, "alice", "alice")
	hello(t, b, "bob", "bob")
	for _, p := range []*Presence{
		nil,
		{State: "stuck"}, // silence must not become a fabricated diagnosis
		{State: "busy", Context: &ContextUsage{Tokens: -1, Window: 100}},
		{State: "busy", Context: &ContextUsage{Window: 0}},
		{State: "busy", Metrics: &RequestMetrics{DurationMS: -1}},
		{State: "busy", ActiveTools: make([]string, 9)},
	} {
		if err := b.Observe("alice", p, false, owner); code(err) != CodeBadRequest {
			t.Errorf("invalid presence accepted: %+v (%v)", p, err)
		}
	}
	for _, task := range []*TaskInfo{nil, {Scope: []string{"/w"}}, {Summary: "work", Scope: []string{"relative"}}} {
		if err := b.SetTask("alice", task); code(err) != CodeBadRequest {
			t.Errorf("invalid task accepted: %+v (%v)", task, err)
		}
	}
	c := &conn{out: make(chan []byte, 1), done: make(chan struct{})}
	self := "alice"
	_, err := dispatch(b, c, &self, &Request{Op: "task", Session: &SessionInfo{ID: "bob"}, Task: &TaskInfo{Summary: "own task"}})
	if err != nil {
		t.Fatal(err)
	}
	alice, _ := b.Resolve("alice")
	bob, _ := b.Resolve("bob")
	if alice.Task == nil || bob.Task != nil {
		t.Fatal("metadata request acted outside its bound session")
	}
}
