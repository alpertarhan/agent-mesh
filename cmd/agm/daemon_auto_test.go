package main

import (
	"bufio"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/codex"
)

func shortIdle(t *testing.T) {
	t.Helper()
	grace, sweep := idleGrace, sweepEvery
	idleGrace, sweepEvery = 400*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { idleGrace, sweepEvery = grace, sweep })
}

func daemonDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "agm-auto")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// daemonStartMode runs the daemon as `agm daemon` or `agm daemon -auto` does.
func daemonStartMode(t *testing.T, sock string, auto bool) <-chan error {
	t.Helper()
	t.Setenv("AGM_SOCKET", sock)
	done := make(chan error, 1)
	go func() { done <- daemonMode(auto) }()
	return done
}

// daemonGone waits for the daemon to return on its own and for its socket to be gone.
func daemonGone(t *testing.T, sock string, done <-chan error, within time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon on %s: %v", sock, err)
		}
	case <-time.After(within):
		t.Fatalf("daemon on %s did not exit", sock)
	}
	if _, err := os.Lstat(sock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket %s still there after the exit: %v", sock, err)
	}
}

func daemonStays(t *testing.T, sock string, done <-chan error, forHowLong time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("daemon on %s exited early: %v", sock, err)
	case <-time.After(forHowLong):
	}
	daemonUp(t, sock)
}

// queueFor registers a pid-less session and leaves one message for it.
func queueFor(t *testing.T, sock, id, harness, text string) {
	t.Helper()
	c := dialSession(t, sock, id, "idle-one", harness, false)
	s := dialSession(t, sock, "peer", "peer", "shell", false)
	s.send(broker.SendReq{To: id, Text: text})
	s.conn.close()
	c.conn.close()
}

func spoolOf(sock string) *broker.Broker {
	b, _ := broker.New(broker.DefaultLimits(), filepath.Join(filepath.Dir(sock), "spool.json"))
	return b
}

// With no work an automatic daemon disappears after the grace, frees the socket and
// both locks, and the next start finds the mail.
func TestDaemonAutoExitsWhenIdleAndRestarts(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	queueFor(t, sock, "c1", "shell", "waiting")
	daemonGone(t, sock, done, 5*time.Second)

	done = daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	if mail, _ := spoolOf(sock).Inbox("c1"); len(mail) != 1 || mail[0].Text != "waiting" {
		t.Fatalf("restart lost the mail: %+v", mail)
	}
	daemonGone(t, sock, done, 5*time.Second)
}

// An open connection is work, whether it said nothing or only watches.
func TestDaemonAutoStaysForOpenConnections(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	daemonStays(t, sock, done, 3*idleGrace)
	c := dialSession(t, sock, "watcher", "watcher", "shell", true)
	nc.Close()
	daemonStays(t, sock, done, 3*idleGrace)
	c.conn.close()
	daemonGone(t, sock, done, 5*time.Second)
}

// A delivery in flight holds the daemon; the grace starts again when it is done.
func TestDaemonAutoStaysForAWakeInFlight(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	old := codexDeliver
	t.Cleanup(func() { codexDeliver = old })
	started, proceed := make(chan struct{}, 8), make(chan struct{})
	codexDeliver = func(id, text string) error { started <- struct{}{}; <-proceed; return nil }
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	queueFor(t, sock, "c1", "codex", "in flight")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the wake never reached the host")
	}
	daemonStays(t, sock, done, 3*idleGrace)
	close(proceed)
	daemonGone(t, sock, done, 5*time.Second)
}

// A Codex thread that is not loaded is retried on every sweep while the daemon is
// up, is no reason to stay up, and is retried again by the next daemon.
func TestDaemonAutoSleepsWithUnloadedCodexThreadAndRetriesAfterRestart(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	old := codexDeliver
	t.Cleanup(func() { codexDeliver = old })
	var attempts atomic.Int32
	codexDeliver = func(id, text string) error { attempts.Add(1); return codex.ErrNotLoaded }
	waitAttempts := func(want int32) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); attempts.Load() < want; {
			if time.Now().After(deadline) {
				t.Fatalf("wake attempts: %d, want at least %d", attempts.Load(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	queueFor(t, sock, "c1", "codex", "still waiting")
	waitAttempts(3) // periodic retries go on while the daemon is up
	daemonGone(t, sock, done, 5*time.Second)

	done = daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	waitAttempts(4) // and resume with the next daemon
	daemonGone(t, sock, done, 5*time.Second)
	if mail, _ := spoolOf(sock).Inbox("c1"); len(mail) != 1 {
		t.Fatalf("mail changed by refused wakes: %+v", mail)
	}
}

// A final save that fails keeps the daemon up until it works.
func TestDaemonAutoStaysWhenTheFinalSaveFails(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	blocker := filepath.Join(filepath.Dir(sock), "spool.json.tmp")
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	daemonStays(t, sock, done, 6*idleGrace)
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	daemonGone(t, sock, done, 5*time.Second)
}

func TestDaemonManualNeverExitsOnIdle(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	done := daemonStartMode(t, sock, false)
	daemonUp(t, sock)
	daemonStays(t, sock, done, 8*idleGrace)
	daemonStop(t, sock, done)
}

// Around the exit moment a visit is served whole or not at all: its send is in the
// spool exactly when it got its answers, and the daemon still goes away.
func TestDaemonAutoBoundaryVisitsAreWhole(t *testing.T) {
	shortIdle(t)
	sock := filepath.Join(daemonDir(t), "s.sock")
	done := daemonStartMode(t, sock, true)
	daemonUp(t, sock)
	box := dialSession(t, sock, "box", "box", "shell", false)
	box.conn.close()
	served, refused := 0, 0
	for i := 0; i < 12; i++ {
		time.Sleep(idleGrace - 60*time.Millisecond)
		nc, err := net.Dial("unix", sock)
		if err != nil {
			break // already gone
		}
		for _, line := range []string{
			"{\"id\":1,\"op\":\"hello\",\"session\":{\"id\":\"peer\",\"name\":\"peer\"}}",
			"{\"id\":2,\"op\":\"send\",\"to\":\"box\",\"text\":\"at the boundary\"}",
		} {
			if _, err := nc.Write([]byte(line + "\n")); err != nil {
				break
			}
		}
		nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		sc := bufio.NewScanner(nc)
		var answers []string
		for len(answers) < 2 && sc.Scan() {
			answers = append(answers, sc.Text())
		}
		nc.Close()
		switch {
		case len(answers) == 2 && !strings.Contains(answers[1], "\"error\""):
			served++
		case len(answers) == 2: // answered, for example rate_limited: nothing was queued
			refused++
		case len(answers) == 0:
			refused++
		default:
			t.Fatalf("visit %d was half served: %q", i, answers)
		}
	}
	daemonGone(t, sock, done, 5*time.Second)
	if served+refused == 0 {
		t.Fatal("no visit reached the boundary")
	}
	if mail, _ := spoolOf(sock).Inbox("box"); len(mail) != served {
		t.Fatalf("%d visits served, %d refused, %d messages kept: a send was lost or duplicated", served, refused, len(mail))
	}
}
