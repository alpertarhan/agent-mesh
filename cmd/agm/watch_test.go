package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestCoordinationWatchHelper(t *testing.T) {
	if os.Getenv("AGM_TEST_WATCH_HELPER") != "1" {
		return
	}
	if err := run("", "watch", []string{"-json"}); err != nil {
		os.Exit(reportError(err, os.Stderr))
	}
	os.Exit(0)
}

func TestCoordinationWatch(t *testing.T) {
	b, _ := testBroker(t)
	if err := b.Hello(broker.SessionInfo{ID: "alice"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Resolve("alice")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCoordinationWatchHelper$")
	cmd.Env = append(os.Environ(), "AGM_TEST_WATCH_HELPER=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	dec := json.NewDecoder(out)
	readSnapshot := func() []broker.SessionInfo {
		t.Helper()
		var snapshot struct {
			Event    string               `json:"event"`
			Sessions []broker.SessionInfo `json:"sessions"`
		}
		if err := dec.Decode(&snapshot); err != nil {
			cancel()
			<-done
			t.Fatalf("watch decode: %v; %s", err, stderr.String())
		}
		if snapshot.Event != "snapshot" {
			t.Fatalf("watch event: %q", snapshot.Event)
		}
		return snapshot.Sessions
	}
	if sessions := readSnapshot(); len(sessions) != 1 || sessions[0].Task != nil {
		t.Fatalf("initial snapshot: %+v", sessions)
	}
	// Leave one polling tick unchanged. The next frame must be the task update,
	// not a duplicate snapshot. Watch itself must not register/refresh a session.
	changed := make(chan error, 1)
	timer := time.AfterFunc(1500*time.Millisecond, func() {
		changed <- b.SetTask("alice", &broker.TaskInfo{Summary: "review"})
	})
	defer timer.Stop()
	sessions := readSnapshot()
	if len(sessions) != 1 || sessions[0].Task == nil || sessions[0].Task.Summary != "review" {
		t.Fatalf("unchanged snapshot emitted or update lost: %+v", sessions)
	}
	if err := <-changed; err != nil {
		t.Fatal(err)
	}
	if !sessions[0].LastSeen.Equal(before.LastSeen) {
		t.Fatal("watch refreshed its target's identity")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("watch did not stop cleanly: %v; %s", err, stderr.String())
	}
}

func TestCoordinationCapabilityAndClockSkew(t *testing.T) {
	legacy := &client{proto: broker.Protocol}
	if err := legacy.needCoordination(); err == nil || !strings.Contains(err.Error(), "lacks coordination") {
		t.Fatalf("old daemon should be refused explicitly: %v", err)
	}
	_, _, _, age := metricColumns(&broker.Presence{Metrics: &broker.RequestMetrics{ObservedAt: time.Now().Add(time.Hour)}})
	if age != "clock-skew" {
		t.Fatalf("future metric timestamp presented as fresh: %s", age)
	}
}
