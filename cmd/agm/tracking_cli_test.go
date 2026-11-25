package main

import (
	"encoding/json"
	"strings"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"testing"
)

func TestTrackingUsage(t *testing.T) {
	out, stderr, code := agm(t, "", "help")
	if code != 0 || stderr != "" || !strings.Contains(out, "agm track MESSAGE_ID") {
		t.Fatalf("help: %q %q (%d)", out, stderr, code)
	}
	for _, cmd := range []string{"track", "seen"} {
		_, stderr, code := agm(t, "", cmd, "-json", "prefix")
		if code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%s: %s (%d)", cmd, stderr, code)
		}
	}
}

func TestTrackingCLIEndToEnd(t *testing.T) {
	testBroker(t)
	run := func(self string, args ...string) string {
		t.Helper()
		out, stderr, code := agm(t, self, args...)
		if code != 0 {
			t.Fatalf("%v: %s (%d)", args, stderr, code)
		}
		return strings.TrimSpace(out)
	}
	run("sender", "hello")
	run("worker", "hello", "-harness", "droid")
	id := run("sender", "send", "worker", "check scope")
	track := func() broker.MessageStatus {
		t.Helper()
		var status broker.MessageStatus
		if err := json.Unmarshal([]byte(run("sender", "track", "-json", id)), &status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	run("worker", "show", id)
	if s := track(); !s.Queued || s.SeenAt != nil || s.Delivery != "unknown" {
		t.Fatalf("show isn't a receipt: %+v", s)
	}
	run("worker", "seen", id)
	run("worker", "ack", id)
	reply := run("worker", "reply", id, "scope verified")
	if s := track(); s.Queued || s.SeenAt == nil || s.Delivery != "acked" || s.ReplyID != reply {
		t.Fatalf("tracked: %+v", s)
	}
	_, stderr, code := agm(t, "outsider", "track", "-json", id)
	if code == 0 || jsonError(t, stderr) != broker.CodeUnknownMsg {
		t.Fatalf("foreign track: %s (%d)", stderr, code)
	}
}
