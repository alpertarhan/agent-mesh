package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestCLIKeyedSendReceipts(t *testing.T) {
	b, dir := testBroker(t)
	for _, id := range []string{"alice", "bob"} {
		if err := b.Hello(broker.SessionInfo{ID: id, Name: id}, nil, false, false); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"send", "-json", "-receipt", "-key", "job-1", "bob", "PRIVATE body"}
	out, stderr, exit := agm(t, "alice", args...)
	var receipt struct {
		ID, To, Status, At string
		RequestKey         string `json:"request_key"`
	}
	if exit != 0 || stderr != "" || json.Unmarshal([]byte(out), &receipt) != nil {
		t.Fatalf("receipt: %q %q (%d)", out, stderr, exit)
	}
	if receipt.ID == "" || receipt.To != "bob" || receipt.Status != "broker_accepted" || receipt.RequestKey != "job-1" || receipt.At == "" {
		t.Fatalf("invalid receipt: %+v", receipt)
	}
	if strings.Contains(out, "PRIVATE") || strings.Contains(out, `"text"`) {
		t.Fatal("receipt echoed the message body")
	}
	retry, stderr, exit := agm(t, "alice", args...)
	if exit != 0 || retry != out {
		t.Fatalf("retry changed receipt: %q %q (%d)", retry, stderr, exit)
	}
	in, err := b.Inbox("bob")
	if err != nil || len(in) != 1 {
		t.Fatalf("retry queued another message: %d, %v", len(in), err)
	}
	// Output selection is not part of the send payload; default and full JSON stay compatible.
	out, stderr, exit = agm(t, "alice", "send", "-key", "job-1", "bob", "PRIVATE body")
	if exit != 0 || strings.TrimSpace(out) != receipt.ID {
		t.Fatalf("plain keyed output: %q %q", out, stderr)
	}
	out, stderr, exit = agm(t, "alice", "send", "-json", "-key", "job-1", "bob", "PRIVATE body")
	var full broker.Message
	if exit != 0 || json.Unmarshal([]byte(out), &full) != nil || full.Text != "PRIVATE body" || full.ID != receipt.ID {
		t.Fatalf("full JSON changed: %q %q", out, stderr)
	}
	out, stderr, exit = agm(t, "alice", "send", "-receipt", "-key", "job-1", "bob", "PRIVATE body")
	if exit != 0 || !strings.Contains(out, "accepted by broker: "+receipt.ID+" -> bob") || strings.Contains(out, "PRIVATE") {
		t.Fatalf("plain receipt: %q %q", out, stderr)
	}
	out, stderr, exit = agm(t, "alice", "send", "-json", "-key", "job-1", "bob", "changed body")
	if exit != 1 || out != "" || jsonError(t, stderr) != broker.CodeKeyConflict {
		t.Fatalf("conflict: %q %q (%d)", out, stderr, exit)
	}

	question, stderr, exit := agm(t, "alice", "ask", "-no-wait", "-key", "question", "bob", "ready?")
	if exit != 0 {
		t.Fatal(stderr)
	}
	question = strings.TrimSpace(question)
	for range 2 {
		if _, stderr, exit := agm(t, "alice", "ask", "-no-wait", "-receipt", "-key", "question", "bob", "ready?"); exit != 0 {
			t.Fatal(stderr)
		}
		if _, stderr, exit := agm(t, "bob", "reply", "-receipt", "-key", "answer", question, "yes"); exit != 0 {
			t.Fatal(stderr)
		}
	}
	in, err = b.Inbox("alice")
	if err != nil || len(in) != 1 || in[0].Text != "yes" {
		t.Fatalf("reply was duplicated: %+v, %v", in, err)
	}

	body := filepath.Join(dir, "body.txt")
	if err := os.WriteFile(body, []byte("file body\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, stderr, exit := agm(t, "alice", "send-file", "-receipt", "-key", "file", "bob", body); exit != 0 {
			t.Fatal(stderr)
		}
	}
	in, err = b.Inbox("bob")
	if err != nil || len(in) != 3 || in[2].Text != "file body\n" {
		t.Fatalf("keyed asks/files duplicated or changed input: %+v, %v", in, err)
	}
}

func TestCLIReceiptUsageAndCapability(t *testing.T) {
	b, _ := testBroker(t)
	if err := b.Hello(broker.SessionInfo{ID: "bob", Name: "bob"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"send", "-json", "-key", "", "bob", "text"},
		{"ask", "-json", "-key", "key", "bob", "text"},
		{"ask", "-json", "-receipt", "bob", "text"},
		{"ask-file", "-json", "-key", "key", "bob", "does-not-exist"},
		{"send", "-json", "bob", "text", "-key", "key"},
		{"send", "-json", "bob", "text", "-receipt"},
	} {
		out, stderr, exit := agm(t, "alice", args...)
		if exit != 2 || out != "" || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%v: %q %q (%d)", args, out, stderr, exit)
		}
	}
	if in, err := b.Inbox("bob"); err != nil || len(in) != 0 {
		t.Fatalf("invalid flags sent mail: %+v, %v", in, err)
	}
	// Protocol 2 alone does not authorize a silently ignored request_key.
	legacy := &client{proto: broker.Protocol}
	if err := legacy.needIdempotency(); err == nil || !strings.Contains(err.Error(), "lacks idempotent") {
		t.Fatalf("accepted old capability: %v", err)
	}
}
