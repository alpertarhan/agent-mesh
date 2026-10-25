package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTaskLifecycleIsMetadataNotReply(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "requester", "requester")
	hello(t, b, "worker", "worker")
	q, err := b.Send("requester", SendReq{To: "worker", Text: "Review auth", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"accepted", "working", "blocked", "done"} {
		task := &TaskInfo{Summary: "Review auth", State: state}
		if state == "accepted" {
			task.RequestID = q.ID
			task.Scope = []string{"/work/auth"}
		}
		if state == "blocked" {
			task.WaitingFor = "a design decision"
		}
		if err := b.SetTask("worker", task); err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		info, _ := b.Resolve("worker")
		if info.Task.State != state || info.Task.RequestID != q.ID || info.Activity != "unknown" || len(info.Task.Scope) != 1 || info.Task.Scope[0] != "/work/auth" {
			t.Fatalf("task claim changed activity or lost linkage: %+v", info)
		}
		if reply, err := b.Wait("requester", q.ID, nil); err != nil || reply != nil {
			t.Fatalf("task %s incorrectly answered the question: %+v, %v", state, reply, err)
		}
	}
	if len(b.history) != 1 {
		t.Fatal("metadata updates emitted messages")
	}
	if mail, _ := b.Inbox("worker"); len(mail) != 1 {
		t.Fatal("task state acknowledged unread mail")
	}
	if mail, _ := b.Inbox("requester"); len(mail) != 0 {
		t.Fatal("task state woke the requester")
	}
	r, err := b.Send("worker", SendReq{ReplyTo: q.ID, Text: "Reviewed; tests pass; remaining risk: external auth provider"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reply, err := b.Wait("requester", q.ID, nil); err != nil || reply == nil || reply.ID != r.ID {
		t.Fatalf("normal result reply stopped working: %+v, %v", reply, err)
	}
}

func TestTaskLifecycleValidationAndRetention(t *testing.T) {
	b := newBroker(t, nil)
	hello(t, b, "a", "a")
	hello(t, b, "b", "b")
	q, err := b.Send("a", SendReq{To: "b", Text: "job"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []*TaskInfo{
		{State: "accepted", RequestID: q.ID},
		{Summary: "job", State: "busy", RequestID: q.ID},
		{Summary: "job", State: "working"},
		{Summary: "job", RequestID: q.ID},
		{Summary: "job", State: "working", RequestID: "bad\nid"},
	} {
		if err := b.SetTask("b", task); code(err) != CodeBadRequest {
			t.Fatalf("invalid task accepted: %+v, %v", task, err)
		}
	}
	if err := b.SetTask("a", &TaskInfo{Summary: "not my assignment", State: "accepted", RequestID: q.ID}); code(err) != CodeUnknownMsg {
		t.Fatalf("sender claimed a task addressed to another session: %v", err)
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "job", State: "accepted", RequestID: q.ID}); err != nil {
		t.Fatal(err)
	}
	// The validated current link survives even when its body is no longer retained.
	if _, err := b.Ack("b", []string{q.ID}); err != nil {
		t.Fatal(err)
	}
	b.history, b.historyBytes = nil, 0
	if err := b.SetTask("b", &TaskInfo{Summary: "still working", State: "working"}); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "updated summary"}); err != nil {
		t.Fatal(err)
	}
	info, _ := b.Resolve("b")
	if info.Task.RequestID != q.ID || info.Task.State != "working" {
		t.Fatal("summary-only update severed the current task link")
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "unknown request", State: "accepted", RequestID: "unknown"}); code(err) != CodeUnknownMsg {
		t.Fatalf("unknown request accepted: %v", err)
	}
	if err := b.SetTask("b", &TaskInfo{}); err != nil {
		t.Fatal(err)
	}
	if info, _ := b.Resolve("b"); info.Task != nil {
		t.Fatal("explicit clear did not remove the linked task")
	}
}

func TestTaskLifecyclePersistenceRollback(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "a", "a")
	hello(t, b, "b", "b")
	q, err := b.Send("a", SendReq{To: "b", Text: "job"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "job", State: "accepted", RequestID: q.ID}); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "finished", State: "done"}); code(err) != CodePersistence {
		t.Fatalf("want persistence failure: %v", err)
	}
	info, _ := b.Resolve("b")
	if info.Task.State != "accepted" || info.Task.RequestID != q.ID {
		t.Fatal("failed write changed task metadata")
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetTask("b", &TaskInfo{Summary: "finished", State: "done"}); err != nil {
		t.Fatal(err)
	}
	restored, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	info, _ = restored.Resolve("b")
	if info.Task == nil || info.Task.State != "done" || info.Task.RequestID != q.ID {
		t.Fatalf("restart lost declared task state/link: %+v", info.Task)
	}
}
