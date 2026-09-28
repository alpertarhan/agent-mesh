package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// capture runs f with os.Stdout/os.Stderr redirected and returns what it wrote.
func capture(t *testing.T, f func() error) (string, string, error) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	ro, wo, _ := os.Pipe()
	re, we, _ := os.Pipe()
	os.Stdout, os.Stderr = wo, we
	outc, errc := make(chan string), make(chan string)
	go func() { b, _ := io.ReadAll(ro); outc <- string(b) }()
	go func() { b, _ := io.ReadAll(re); errc <- string(b) }()
	err := f()
	wo.Close()
	we.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	return <-outc, <-errc, err
}

// testBroker serves an isolated broker on a short /tmp socket (AGM_SOCKET).
func testBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()
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
	b, _ := broker.New(broker.DefaultLimits(), "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })
	return b, dir
}

func TestAskShowsReplyRefs(t *testing.T) {
	b, dir := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "asker", Name: "asker"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "helper", Name: "helper"}, nil, false, false)
	ref := filepath.Join(dir, "the review ğ.md")
	os.WriteFile(ref, []byte("x"), 0o600)

	// The helper answers with a file reference once the question is queued.
	go func() {
		for range 200 {
			if in, _ := b.Inbox("helper"); len(in) == 1 {
				atts := []broker.Attachment{{Type: "ref", Name: filepath.Base(ref), Path: ref}}
				b.Send("helper", broker.SendReq{ReplyTo: in[0].ID, Text: "see file", Attachments: atts}, nil)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	stdout, stderr, err := capture(t, func() error { return run("asker", "ask", []string{"-timeout", "5s", "helper", "review?"}) })
	if err != nil {
		t.Fatal(err, stderr)
	}
	if stdout != "see file\n" {
		t.Fatalf("stdout %q, want only the reply text", stdout)
	}
	if !strings.Contains(stderr, shellQuote(ref)) || !strings.Contains(stderr, "current content") {
		t.Fatalf("stderr lacks the reference:\n%s", stderr)
	}
	var buf bytes.Buffer
	replyExtras(&buf, &broker.Message{ID: "r1", Text: "t"})
	if buf.Len() != 0 {
		t.Fatalf("extras for a plain reply: %q", buf.String())
	}
}
