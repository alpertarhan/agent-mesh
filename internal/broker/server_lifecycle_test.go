package broker

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
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

func TestServerRetainsLockUntilHandlersFinish(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-drain-")
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
		t.Error("another daemon acquired the lock before the old handler finished")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not finish draining")
	}
	other, err := Listen(sock)
	if err != nil {
		t.Fatalf("lock not released after draining: %v", err)
	}
	other.Close()
}
