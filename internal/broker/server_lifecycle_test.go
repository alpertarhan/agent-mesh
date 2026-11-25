package broker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestListenerOwnsLockUntilClose(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-lock-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "s")
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	probe, err := os.OpenFile(sock+".lock", os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	for range 3 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond) // allow the file finalizer to run if ownership is lost
		if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			t.Fatal("live listener lost its exclusive lock after GC")
		}
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("closed listener retained its lock: %v", err)
	}
}

type writingConn struct {
	net.Conn
	writing chan struct{}
}

func (c *writingConn) Write(p []byte) (int, error) {
	select {
	case c.writing <- struct{}{}:
	default:
	}
	return c.Conn.Write(p)
}

func TestSlowReaderDoesNotBlockCancellation(t *testing.T) {
	srv, peer := net.Pipe()
	defer srv.Close()
	defer peer.Close()
	wrapped := &writingConn{Conn: srv, writing: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newBroker(t, nil)
	done := make(chan struct{})
	go func() {
		handle(ctx, wrapped, b)
		close(done)
	}()
	if err := peer.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write([]byte("{\"id\":1,\"op\":\"protocol\"}\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wrapped.writing:
	case <-time.After(time.Second):
		t.Fatal("server did not begin its response")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler remained blocked writing to a slow reader after cancellation")
	}
}

// TestServerRetainsLocksUntilHandlersFinish: after Stop the socket lock and, for a
// daemon listener (ListenDaemon), the state lock stay taken until the in-flight
// handler finished, then both are free for the next daemon.
func TestServerRetainsLocksUntilHandlersFinish(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-drain-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	spool, otherSock := filepath.Join(dir, "spool.json"), filepath.Join(dir, "other")
	sock := filepath.Join(dir, "s")
	ln, err := ListenDaemon(sock, spool)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b := newBroker(t, nil)
	b.ensure("slow").info.PID = 123
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	b.alive = func(int) bool {
		close(entered)
		<-release
		return true
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, ln, b)
		close(done)
	}()
	defer func() { cancel(); unblock(); <-done }()
	peer, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.Write([]byte("{\"id\":1,\"op\":\"list\"}\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request never entered the broker")
	}
	cancel()
	ln.Close()
	if other, err := Listen(sock); err == nil {
		other.Close()
		t.Error("another daemon acquired the socket lock before the old handler finished")
	}
	if other, err := ListenDaemon(otherSock, spool); err == nil {
		other.Close()
		t.Error("another daemon acquired the state lock before the old handler finished")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not finish draining")
	}
	other, err := Listen(sock)
	if err != nil {
		t.Fatalf("socket lock not released after draining: %v", err)
	}
	other.Close()
	other, err = ListenDaemon(otherSock, spool)
	if err != nil {
		t.Fatalf("state lock not released after draining: %v", err)
	}
	other.Close()
}

// TestListenDaemonOnePerDirectory: a daemon on a second socket in the directory is
// refused and leaves no socket behind; a plain listener (a link gate) there is not, as
// it keeps no state; Close releases the state for the next daemon.
func TestListenDaemonOnePerDirectory(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-state-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	spool := filepath.Join(dir, "spool.json")
	ln, err := ListenDaemon(filepath.Join(dir, "a"), spool)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b := filepath.Join(dir, "b")
	if other, err := ListenDaemon(b, spool); err == nil {
		other.Close()
		t.Fatal("a second daemon started in the directory")
	} else if !strings.Contains(err.Error(), "another daemon already uses the state in "+dir) {
		t.Errorf("ListenDaemon: %v, want it to name the directory", err)
	}
	if _, err := os.Lstat(b); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the refused daemon left its socket behind: %v", err)
	}
	if gate, err := Listen(filepath.Join(dir, "link")); err != nil {
		t.Errorf("a plain listener in the directory was refused: %v", err)
	} else {
		gate.Close()
	}
	ln.Close()
	other, err := ListenDaemon(b, spool)
	if err != nil {
		t.Fatalf("state not released by Close: %v", err)
	}
	other.Close()
}

func staleSocket(t *testing.T, path string) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		t.Fatal(err)
	}
}

// listing names dir and each entry with its mode and, for regular files, its content.
func listing(t *testing.T, dir string) string {
	t.Helper()
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, ". %v\n", fi.Mode())
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s %v", e.Name(), info.Mode())
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			b.Write(data)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// TestListenReplacesStaleSocket: a socket left by a dead daemon is replaced, in a
// directory used as it is (the sticky bit survives; a chmod would drop it); a live
// one still stops a second listener.
func TestListenReplacesStaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-stale-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, os.ModeSticky|0o700); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "s")
	staleSocket(t, sock)
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("the new socket does not answer: %v", err)
	}
	nc.Close()
	if other, err := Listen(sock); err == nil {
		other.Close()
		t.Error("a second listener replaced a live one")
	} else if !strings.Contains(err.Error(), "already running") {
		t.Errorf("second Listen: %v, want already running", err)
	}
	if nc, err := net.Dial("unix", sock); err != nil {
		t.Errorf("live socket gone after a refused second Listen: %v", err)
	} else {
		nc.Close()
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode()&(os.ModeSticky|0o777) != os.ModeSticky|0o700 {
		t.Errorf("directory mode %v, %v; want 1700 kept", fi.Mode(), err)
	}
}

// TestListenRefusesAndLeavesAlone: a directory others can reach, or anything but a
// socket at the socket path, stops Listen before it creates or changes anything.
func TestListenRefusesAndLeavesAlone(t *testing.T) {
	file := func(t *testing.T, path string) string {
		t.Helper()
		if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, dir string) (sock, want string)
	}{
		{"directory others can reach", func(t *testing.T, dir string) (string, string) {
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(dir, "s"), "mode 755"
		}},
		{"directory is a file", func(t *testing.T, dir string) (string, string) {
			return file(t, filepath.Join(dir, "d")) + "/s", "is not a directory"
		}},
		{"file at the socket path", func(t *testing.T, dir string) (string, string) {
			return file(t, filepath.Join(dir, "s")), "is not a socket"
		}},
		{"symlink at the socket path", func(t *testing.T, dir string) (string, string) {
			sock := filepath.Join(dir, "s")
			if err := os.Symlink("nowhere", sock); err != nil {
				t.Fatal(err)
			}
			return sock, "is not a socket"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "agm-refuse-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			sock, want := tc.setup(t, dir)
			before := listing(t, dir)
			ln, err := Listen(sock)
			if err == nil {
				ln.Close()
				t.Fatalf("Listen accepted %s", sock)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("Listen: %v, want it to say %q", err, want)
			}
			if after := listing(t, dir); after != before {
				t.Errorf("Listen changed the directory\nbefore:\n%safter:\n%s", before, after)
			}
		})
	}
}

func TestListenCreatesPrivateDirs(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-mk-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ln, err := Listen(filepath.Join(dir, "a", "b", "s"))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	for _, d := range []string{"a", filepath.Join("a", "b")} {
		if fi, err := os.Stat(filepath.Join(dir, d)); err != nil || fi.Mode()&0o077 != 0 {
			t.Errorf("%s: %v, %v; want no access for group or other", d, err, fi)
		}
	}
}
