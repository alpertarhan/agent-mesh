package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestRoleCLI(t *testing.T) {
	b, _ := testBroker(t)
	run := func(as string, args ...string) string {
		t.Helper()
		out, stderr, code := agm(t, as, args...)
		if code != 0 {
			t.Fatalf("%v: %s (%d)", args, stderr, code)
		}
		return out
	}
	run("worker", "hello")
	run("lead", "hello")
	q, err := b.Send("lead", broker.SendReq{To: "worker", Text: "verify shutdown", ExpectsReply: true, NoWait: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run("worker", "task", "set", "-state", "accepted", "-request", q.ID, "verify shutdown")
	run("worker", "role", "set", "scout", "Check real hosts")
	run("worker", "role", "set", "reviewer", "Review hook lifecycle")
	list := run("", "list")
	if !strings.Contains(list, "ROLE") || !strings.Contains(list, "reviewer") || strings.Contains(list, "Review hook lifecycle") {
		t.Fatalf("list: %s", list)
	}
	info := run("", "inspect", "worker")
	for _, want := range []string{"reviewer", "Review hook lifecycle", "verify shutdown"} {
		if !strings.Contains(info, want) {
			t.Fatalf("inspect missing %q: %s", want, info)
		}
	}
	for _, args := range [][]string{{"role", "set", "-json", "", ""}, {"role", "clear", "-json", "worker"}} {
		_, stderr, code := agm(t, "worker", args...)
		if code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("bad arguments: %s (%d)", stderr, code)
		}
	}
	out := run("worker", "role", "clear", "-json")
	if !strings.Contains(out, `"cleared":true`) {
		t.Fatal(out)
	}
	var s broker.SessionInfo
	if err := json.Unmarshal([]byte(run("", "inspect", "-json", "worker")), &s); err != nil || s.Role != nil || s.Task == nil || s.Task.Summary != "verify shutdown" || s.Task.RequestID != q.ID || s.Task.State != "accepted" {
		t.Fatalf("role clear changed other state: %+v (%v)", s, err)
	}
	mail, err := b.Inbox("worker")
	if err != nil || len(mail) != 1 || mail[0].ID != q.ID {
		t.Fatalf("role change affected inbox: %v, %v", mail, err)
	}
	run("worker", "reply", q.ID, "verified")
	answer, err := b.Wait("lead", q.ID, nil)
	if err != nil || answer == nil || answer.From != "worker" || answer.ReplyTo != q.ID {
		t.Fatalf("role change affected reply: %+v, %v", answer, err)
	}
}

func TestRoleCapabilityGate(t *testing.T) {
	c := &client{proto: broker.Protocol}
	if err := c.needRole(); err == nil || !strings.Contains(err.Error(), "lacks session roles") {
		t.Fatalf("missing capability: %v", err)
	}
	c.roleSupport = broker.RoleSupport
	if err := c.needRole(); err != nil {
		t.Fatal(err)
	}
}
