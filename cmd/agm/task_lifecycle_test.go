package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestTaskLifecycleCLI(t *testing.T) {
	b, dir := testBroker(t)
	t.Chdir(dir)
	for _, id := range []string{"requester", "worker"} {
		if err := b.Hello(broker.SessionInfo{ID: id, Name: id}, nil, false, false); err != nil {
			t.Fatal(err)
		}
	}
	q, err := b.Send("requester", broker.SendReq{To: "worker", Text: "review auth", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, stderr, code := agm(t, "worker", "task", "set", "-request", q.ID, "-state", "accepted", "-scope", "src", "review auth")
	if code != 0 || out != "task updated\n" {
		t.Fatalf("accept: %q %q (%d)", out, stderr, code)
	}
	for _, state := range []string{"working", "blocked", "done"} {
		out, stderr, code = agm(t, "worker", "task", "set", "-state", state, "review auth")
		if code != 0 || out != "task updated\n" {
			t.Fatalf("%s: %q %q (%d)", state, out, stderr, code)
		}
	}
	out, stderr, code = agm(t, "", "inspect", "worker")
	for _, want := range []string{"task state done (owner-declared)", "request  " + q.ID, "scope"} {
		if code != 0 || !strings.Contains(out, want) {
			t.Fatalf("inspect missing %q: %q %q", want, out, stderr)
		}
	}
	out, stderr, code = agm(t, "", "list")
	if code != 0 || !strings.Contains(out, "done: review auth") {
		t.Fatalf("list: %q %q", out, stderr)
	}
	out, stderr, code = agm(t, "", "inspect", "-json", "worker")
	var info broker.SessionInfo
	if code != 0 || json.Unmarshal([]byte(out), &info) != nil || info.Task == nil || info.Task.State != "done" || info.Task.RequestID != q.ID {
		t.Fatalf("JSON state/link: %q %q", out, stderr)
	}
	if reply, err := b.Wait("requester", q.ID, nil); err != nil || reply != nil {
		t.Fatalf("metadata completed the reply wait: %+v, %v", reply, err)
	}
	_, stderr, code = agm(t, "worker", "task", "set", "-json", "-state", "stuck", "bad")
	if code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("invalid state: %s (%d)", stderr, code)
	}
	_, stderr, code = agm(t, "worker", "task", "set", "-json", "summary", "-state", "working")
	if code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("trailing task flag became a summary: %s (%d)", stderr, code)
	}
}

func TestTaskLifecycleCapabilityGate(t *testing.T) {
	c := &client{proto: broker.Protocol, coordination: broker.CoordinationSupport}
	if err := c.needTaskLifecycle(); err == nil || !strings.Contains(err.Error(), "lacks linked task states") {
		t.Fatalf("old daemon accepted new task fields: %v", err)
	}
	c.taskLifecycle = broker.TaskLifecycleSupport
	if err := c.needTaskLifecycle(); err != nil {
		t.Fatal(err)
	}
}
