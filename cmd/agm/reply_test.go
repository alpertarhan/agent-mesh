package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// TestReplyReachesRemovedAsker replays the two L0 cases on an isolated daemon with a
// short IdleTTL instead of the real 10 minutes:
//
//	A: ask -no-wait, then stay idle (no connection at all);
//	B: ask -no-wait, then sit in `wait` across the sweep.
//
// Both askers are garbage-collected; both replies are still delivered.
func TestReplyReachesRemovedAsker(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	t.Setenv("AGM_SOCKET", sock)
	ln, err := broker.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	lim := broker.DefaultLimits()
	lim.IdleTTL = 250 * time.Millisecond
	b, _ := broker.New(lim, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })

	ok := func(stdout, stderr string, code int) string {
		t.Helper()
		if code != 0 {
			t.Fatalf("exit %d: %q %q", code, stdout, stderr)
		}
		return strings.TrimSpace(stdout)
	}

	ok(agm(t, "alice-a", "hello", "-name", "alice-a", "-harness", "shell"))
	ok(agm(t, "alice-b", "hello", "-name", "alice-b", "-harness", "shell"))
	ok(agm(t, "bob-1", "hello", "-name", "bob", "-harness", "shell"))
	qA := ok(agm(t, "alice-a", "ask", "-no-wait", "bob", "case A: ask, then stay idle"))
	qB := ok(agm(t, "alice-b", "ask", "-no-wait", "bob", "case B: ask, then block in wait"))
	t.Logf("asked %s (A), %s (B)", qA, qB)

	// Case B: a `wait` connection that stays open across the sweep (raw protocol, as
	// in the L0 evidence; the CLI test helper cannot block in parallel).
	wc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer wc.Close()
	wc.SetReadDeadline(time.Now().Add(10 * time.Second))
	wsc := bufio.NewScanner(wc)
	wsc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	say := func(v any) {
		t.Helper()
		if err := json.NewEncoder(wc).Encode(v); err != nil {
			t.Fatal(err)
		}
	}
	frame := func() map[string]any {
		t.Helper()
		if !wsc.Scan() {
			t.Fatalf("wait connection closed: %v", wsc.Err())
		}
		var f map[string]any
		if err := json.Unmarshal(wsc.Bytes(), &f); err != nil {
			t.Fatal(err)
		}
		return f
	}
	say(map[string]any{"id": 1, "op": "hello", "session": map[string]any{"id": "alice-b"}})
	if f := frame(); f["error"] != nil {
		t.Fatalf("hello: %v", f)
	}
	say(map[string]any{"id": 2, "op": "wait", "ids": []string{qB}})
	if f := frame(); f["id"].(float64) != 2 || f["error"] != nil {
		t.Fatalf("wait: %v", f)
	}

	time.Sleep(500 * time.Millisecond) // both alices idle past IdleTTL
	gone := b.Sweep()
	slices.Sort(gone)
	t.Logf("gc: removed %v", gone)
	if !slices.Equal(gone, []string{"alice-a", "alice-b"}) {
		t.Fatalf("sweep removed %v", gone)
	}

	t.Logf("reply A: %s", ok(agm(t, "bob-1", "reply", qA, "answer A")))
	t.Logf("reply B: %s", ok(agm(t, "bob-1", "reply", qB, "answer B")))

	// Case B: the open wait gets the reply pushed.
	for {
		f := frame()
		ev, _ := f["event"].(string)
		if ev != "message" {
			continue
		}
		m, _ := f["message"].(map[string]any)
		if m["reply_to"] == qB {
			if m["text"] != "answer B" {
				t.Fatalf("pushed %v", m)
			}
			t.Logf("case B: pushed to the open wait: %q", m["text"])
			break
		}
	}

	// Case A: the idle asker finds the reply later under the same -as id.
	if out := ok(agm(t, "alice-a", "wait", "-timeout", "2s", "-reply-to", qA)); out != "answer A" {
		t.Fatalf("case A wait: %q", out)
	} else {
		t.Logf("case A: later wait: %q", out)
	}
	t.Logf("sessions after:\n%s", ok(agm(t, "", "list")))
}
