package main

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"unicode"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
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
			if !strings.Contains(args[3], "inbox -ack") || !strings.Contains(args[3], integrations.Frame()) || strings.Contains(args[3], "\n") {
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

// The nudge is typed as user input: peer-controlled names must arrive as one
// short, control-free line (newlines would submit extra prompts; ESC/C1/bidi runes
// would drive or reshape the terminal).
func TestNudgeSanitizesPeerNames(t *testing.T) {
	b, _ := broker.New(broker.DefaultLimits(), "")
	name := "bob\nrm -rf ~\n\x03\x1b[A\u202e\u009b" + strings.Repeat("x", 99)
	b.Hello(broker.SessionInfo{ID: "evil", Name: name}, nil, false, false)
	info := broker.SessionInfo{ID: "c", Harness: "crush", Pane: "p1", PID: os.Getpid()}
	b.Hello(info, nil, false, false)
	if _, err := b.Send("evil", broker.SendReq{To: "c", Text: "hi"}, nil); err != nil {
		t.Fatal(err)
	}
	w := &waker{b: b, inflight: map[string]bool{}, nudged: map[string]string{}}
	oldOut, oldRun := herdrOutput, herdrRun
	t.Cleanup(func() { herdrOutput, herdrRun = oldOut, oldRun })
	var typed string
	herdrOutput = func(...string) ([]byte, error) {
		return []byte(`{"result":{"pane":{"agent":"crush","agent_status":"idle","focused":false}}}`), nil
	}
	herdrRun = func(args ...string) error {
		if args[1] == "send-text" {
			typed = args[3]
		}
		return nil
	}
	w.nudge(info)
	if typed == "" {
		t.Fatal("no nudge typed")
	}
	// One line (no submitted extra prompts) and framed, with no control or bidi runes;
	// the name's words stay (peer text can always carry words), only as one clean line.
	if strings.Contains(typed, "\n") || !strings.HasPrefix(typed, "[agent-mesh] 1 new message(s) from bob ") || !strings.Contains(typed, integrations.Frame()) || strings.IndexFunc(typed, func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }) >= 0 {
		t.Fatalf("nudge text %q", typed)
	}
}

// 3c: link and bridge sessions ("/" ids) are never woken on this host, even
// with a locally-wakeable harness and a live pid.
func TestWakeSkipsLinkIDs(t *testing.T) {
	b, _ := testBroker(t)
	link := broker.SessionInfo{ID: "srv/x", Harness: "crush", Pane: "p1", PID: os.Getpid()}
	local := broker.SessionInfo{ID: "c", Harness: "crush", Pane: "p1", PID: os.Getpid()}
	b.Hello(link, nil, false, false)
	b.Hello(local, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "sender"}, nil, false, false)
	if _, err := b.Send("sender", broker.SendReq{To: "srv/x", Text: "hi"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send("sender", broker.SendReq{To: "c", Text: "hi"}, nil); err != nil {
		t.Fatal(err)
	}
	w := &waker{b: b, inflight: map[string]bool{}, nudged: map[string]string{}}
	oldOut, oldRun := herdrOutput, herdrRun
	t.Cleanup(func() { herdrOutput, herdrRun = oldOut, oldRun })
	nudged := 0
	herdrOutput = func(...string) ([]byte, error) {
		return []byte(`{"result":{"pane":{"agent":"crush","agent_status":"idle","focused":false}}}`), nil
	}
	herdrRun = func(args ...string) error {
		if args[1] == "send-text" {
			nudged++
		}
		return nil
	}
	w.wake(link)
	if nudged != 0 {
		t.Fatalf("a / id was nudged %d times", nudged)
	}
	w.wake(local)
	if nudged != 1 {
		t.Fatalf("a local id was not nudged (%d)", nudged)
	}
}
