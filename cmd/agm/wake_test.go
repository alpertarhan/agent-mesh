package main

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestNudgeRetriesAfterFailure(t *testing.T) {
	b, _ := broker.New(broker.DefaultLimits(), "")
	b.Hello(broker.SessionInfo{ID: "a", Name: "alice"}, nil, false, false)
	info := broker.SessionInfo{ID: "c", Name: "cr", Harness: "crush", Pane: "p1", PID: os.Getpid()}
	b.Hello(info, nil, false, false)
	if _, err := b.Send("a", broker.SendReq{To: "c", Text: "hi"}, nil); err != nil {
		t.Fatal(err)
	}
	w := &waker{b: b, inflight: map[string]bool{}, nudged: map[string]string{}}

	var mu sync.Mutex
	var getErr, textErr error
	var typed int
	var during func()
	oldOut, oldRun := herdrOutput, herdrRun
	t.Cleanup(func() { herdrOutput, herdrRun = oldOut, oldRun })
	herdrOutput = func(args ...string) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if during != nil {
			during()
		}
		if getErr != nil {
			return nil, getErr
		}
		return []byte(`{"result":{"pane":{"agent":"crush","agent_status":"idle","focused":false}}}`), nil
	}
	herdrRun = func(args ...string) error {
		mu.Lock()
		defer mu.Unlock()
		if args[1] == "send-text" {
			if textErr != nil {
				return textErr
			}
			typed++
			if !strings.Contains(args[3], "inbox -ack") {
				t.Errorf("nudge text %q", args[3])
			}
		}
		return nil
	}

	getErr = errors.New("herdr down")
	w.nudge(info)
	getErr, textErr = nil, errors.New("send failed")
	w.nudge(info) // must retry after the herdrGet failure
	textErr = nil
	w.nudge(info) // and after the send-text failure
	w.nudge(info) // succeeded: deduped
	if typed != 1 {
		t.Fatalf("typed %d times, want 1", typed)
	}

	// A failing older attempt must not erase a marker set meanwhile by a newer one.
	b.Send("a", broker.SendReq{To: "c", Text: "again"}, nil)
	during = func() { w.mu.Lock(); w.nudged["c"] = "newer"; w.mu.Unlock() }
	getErr = errors.New("down")
	w.nudge(info)
	if w.nudged["c"] != "newer" {
		t.Fatalf("marker %q", w.nudged["c"])
	}
}
