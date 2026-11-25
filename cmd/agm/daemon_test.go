package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// daemonUp waits until the daemon on sock answers.
func daemonUp(t *testing.T, sock string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		c, err := dialBConn(sock, nil)
		if err != nil {
			continue
		}
		err = c.callDL(context.Background(), broker.Request{Op: "protocol"}, nil, fixedDL(time.Second))
		c.close()
		if err == nil {
			return
		}
	}
	t.Fatalf("no daemon answers on %s", sock)
}

// daemonStart runs daemon() as `agm daemon` does, on sock; the channel gets its result.
func daemonStart(t *testing.T, sock string) <-chan error {
	t.Helper()
	t.Setenv("AGM_SOCKET", sock)
	done := make(chan error, 1)
	go func() { done <- daemon() }()
	return done
}

// daemonStop sends the shutdown op, waits for daemon() to return and for the socket
// file to be gone.
func daemonStop(t *testing.T, sock string, done <-chan error) {
	t.Helper()
	c, err := dialBConn(sock, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.callDL(context.Background(), broker.Request{Op: "shutdown"}, nil, fixedDL(time.Second)) // its reply can lose the race against the close
	c.close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("daemon on %s: %v", sock, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("daemon on %s did not stop", sock)
	}
	if _, err := os.Lstat(sock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket %s still there after the daemon stopped: %v", sock, err)
	}
}

// TestDaemonOnePerDirectory runs real daemons (daemon(), in this process) on throwaway
// directories. The spool belongs to the socket's directory, so a daemon on another
// socket there is refused and changes nothing of the first one's; another directory is
// another mesh; and shutdown releases the state while this process lives on, so the
// next daemon in the directory starts and finds the mail.
func TestDaemonOnePerDirectory(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	away, err := os.MkdirTemp("/tmp", "agm-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(away)
	mail := func(sock, what string) {
		t.Helper()
		if got := probeInbox(t, sock, "alice"); len(got) != 1 || got[0] != "queued for alice" {
			t.Errorf("%s: alice's mail is %q", what, got)
		}
	}

	aSock := filepath.Join(dir, "a.sock")
	a := daemonStart(t, aSock)
	daemonUp(t, aSock)
	dialSession(t, aSock, "alice", "alice", "pi", false)
	dialSession(t, aSock, "bob", "bob", "pi", false).send(broker.SendReq{To: "alice", Text: "queued for alice"})
	mail(aSock, "before the second daemon")
	spool := filepath.Join(dir, "spool.json")
	before, err := os.ReadFile(spool)
	if err != nil {
		t.Fatal(err)
	}
	aInfo, err := os.Stat(aSock)
	if err != nil {
		t.Fatal(err)
	}

	// A daemon on another socket in the directory: refused, and nothing of the first one's changes.
	bSock := filepath.Join(dir, "b.sock")
	refused := daemonStart(t, bSock)
	select {
	case err := <-refused:
		if err == nil || !strings.Contains(err.Error(), "another daemon already uses the state in "+dir) {
			t.Errorf("second daemon in the directory: %v", err)
		}
	case <-time.After(3 * time.Second):
		daemonStop(t, bSock, refused)
		t.Error("a second daemon started in the directory")
	}
	if after, err := os.ReadFile(spool); err != nil || !bytes.Equal(after, before) {
		t.Errorf("the second daemon changed %s: %v", spool, err)
	}
	if st, err := os.Stat(aSock); err != nil || !os.SameFile(aInfo, st) {
		t.Errorf("the first daemon's socket changed: %v", err)
	}
	if _, err := os.Lstat(bSock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the refused daemon left its socket behind: %v", err)
	}
	mail(aSock, "after the refusal")

	// Another directory is another mesh: its daemon runs alongside the first.
	cSock := filepath.Join(away, "c.sock")
	c := daemonStart(t, cSock)
	daemonUp(t, cSock)
	daemonStop(t, cSock, c)

	// Shutdown releases the state in this process: the next daemon in the directory
	// starts and inherits the mail.
	daemonStop(t, aSock, a)
	dSock := filepath.Join(dir, "d.sock")
	d := daemonStart(t, dSock)
	daemonUp(t, dSock)
	mail(dSock, "in the next daemon")
	daemonStop(t, dSock, d)
}

// A first list must observe elapsed TTLs without waiting for the maintenance tick.
func TestDaemonRestoresOnlyUnexpiredMailboxes(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-resume-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "d.sock")
	now := time.Now()
	state := map[string]any{
		"sessions": []broker.SessionInfo{
			{ID: "expired-empty", LastSeen: now.Add(-11 * time.Minute)},
			{ID: "expired-mail", LastSeen: now.Add(-25 * time.Hour)},
			{ID: "retained-mail", LastSeen: now.Add(-23 * time.Hour)},
		},
		"mailboxes": map[string][]broker.Message{
			"expired-mail":  {{ID: "expired", Text: "old"}},
			"retained-mail": {{ID: "retained", Text: "still queued"}},
		},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "spool.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	done := daemonStart(t, sock)
	daemonUp(t, sock)
	defer daemonStop(t, sock, done)
	if rows := listOn(t, sock); len(rows) != 1 || rows[0].ID != "retained-mail" || rows[0].Queued != 1 {
		t.Fatalf("first list after restart: %+v", rows)
	}
}

// TestDaemonLoadFailureReleasesLocks: a corrupt spool stops the daemon before it
// serves. That attempt releases its socket and both locks, so the next start on the
// same socket in this process works at once.
func TestDaemonLoadFailureReleasesLocks(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	spool, sock := filepath.Join(dir, "spool.json"), filepath.Join(dir, "a.sock")
	if err := os.WriteFile(spool, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGM_SOCKET", sock)
	if err := daemon(); err == nil || !strings.Contains(err.Error(), "load spool") {
		t.Fatalf("daemon on a corrupt spool: %v", err)
	}
	if _, err := os.Lstat(sock); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("socket left behind by the failed start: %v", err)
	}
	if err := os.Remove(spool); err != nil { // repaired: the next start makes a fresh spool
		t.Fatal(err)
	}
	a := daemonStart(t, sock)
	daemonUp(t, sock)
	daemonStop(t, sock, a)
}
