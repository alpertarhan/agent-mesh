package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

type observationOwner struct{}

func (observationOwner) Push(*broker.Message) bool { return true }
func (observationOwner) Close()                    {}

func TestCoordinationCLI(t *testing.T) {
	b, dir := testBroker(t)
	t.Chdir(dir)
	for _, id := range []string{"alice", "bob"} {
		if err := b.Hello(broker.SessionInfo{ID: id, Name: id, Cwd: dir}, nil, false, false); err != nil {
			t.Fatal(err)
		}
	}
	ttft := 500.0
	if err := b.Observe("alice", &broker.Presence{
		State: "busy", Provider: "provider", Model: "selected-model",
		Context: &broker.ContextUsage{Tokens: 500, Window: 1000},
		Metrics: &broker.RequestMetrics{Model: "actual-model", OutputTokens: 100, DurationMS: 2500, TTFTMS: &ttft, TTFTSource: "observed", ObservedAt: time.Now()},
	}, true, observationOwner{}); err != nil {
		t.Fatal(err)
	}
	out, stderr, code := agm(t, "alice", "task", "set", "-json", "-scope", "not-on-disk", "-waiting-for", "bob review", "review auth")
	if code != 0 || !strings.Contains(out, `"updated":true`) {
		t.Fatalf("task set: %s %s (%d)", out, stderr, code)
	}
	info, _ := b.Resolve("alice")
	if info.Task.Summary != "review auth" || info.Task.Scope[0] != filepath.Join(dir, "not-on-disk") {
		t.Fatalf("task metadata: %+v", info.Task)
	}
	out, stderr, code = agm(t, "", "list", "-json", "-state", "busy")
	var list []broker.SessionInfo
	if code != 0 || json.Unmarshal([]byte(out), &list) != nil || len(list) != 1 || list[0].ID != "alice" {
		t.Fatalf("filtered list: %s %s", out, stderr)
	}
	out, stderr, code = agm(t, "", "list", "-metrics")
	for _, want := range []string{"selected-model", "review auth", "50.0%", "~50.0", "~500ms", "unknown"} {
		if code != 0 || !strings.Contains(out, want) {
			t.Errorf("metrics list missing %q: %s %s", want, out, stderr)
		}
	}
	out, stderr, code = agm(t, "", "inspect", "-json", "alice")
	if code != 0 || json.Unmarshal([]byte(out), &info) != nil || info.Activity != "busy" || info.Presence.Metrics.Model != "actual-model" {
		t.Fatalf("inspect: %s %s", out, stderr)
	}
	out, stderr, code = agm(t, "", "inspect", "bob")
	if code != 0 || !strings.Contains(out, "unknown") {
		t.Fatalf("unsupported presence must be unknown: %s %s", out, stderr)
	}
	_, stderr, code = agm(t, "", "list", "-json", "-state", "stuck")
	if code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("invalid activity: %s (%d)", stderr, code)
	}
	_, stderr, code = agm(t, "alice", "task", "clear", "-json")
	info, _ = b.Resolve("alice")
	if code != 0 || info.Task != nil {
		t.Fatalf("task clear: %s (%d), %+v", stderr, code, info.Task)
	}
}
