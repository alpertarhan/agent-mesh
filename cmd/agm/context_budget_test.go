package main

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

func TestHookBatchContextBudget(t *testing.T) {
	messages := make([]*broker.Message, 256)
	for i := range messages {
		messages[i] = &broker.Message{ID: fmt.Sprint(i), From: "peer", Text: strings.Repeat("x", 2000)}
	}
	text, count := formatMail(messages)
	if count == 0 || count >= len(messages) {
		t.Fatalf("budget did not select a nonempty prefix: %d", count)
	}
	if len(text) > 24000 {
		t.Fatalf("hook burst emitted %d UTF-8 bytes; maximum is 24000", len(text))
	}
	if !strings.Contains(text, integrations.Frame()) {
		t.Fatal("budget removed the peer-not-user boundary")
	}
}

func TestHookMessageContextBudget(t *testing.T) {
	m := &broker.Message{ID: "budget-ask", From: "peer", Text: strings.Repeat("😀", 1400), ExpectsReply: true}
	for range 64 {
		m.Attachments = append(m.Attachments, broker.Attachment{
			Type: "snippet", Name: strings.Repeat("label", 60), Content: strings.Repeat("é", 4000),
		})
	}
	for i := range 16 {
		m.Attachments = append(m.Attachments, broker.Attachment{
			Type: "ref", Path: fmt.Sprintf("/tmp/%d/", i) + strings.Repeat("path", 1000),
		})
	}
	text, _ := formatMail([]*broker.Message{m})
	if len(text) > 8000 {
		t.Fatalf("one message emitted %d UTF-8 bytes including attachments; maximum is 8000", len(text))
	}
	if !utf8.ValidString(text) {
		t.Fatal("budget split a UTF-8 rune")
	}
	for _, required := range []string{integrations.Frame(), "show budget-ask", "reply budget-ask"} {
		if !strings.Contains(text, required) {
			t.Fatalf("budget removed required context: %q", required)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "file: ") && !strings.HasSuffix(line, "'") {
			t.Fatalf("path was partially rendered: %q", line)
		}
	}
}

func TestTakeSelectedLeavesUnselectedMailQueued(t *testing.T) {
	b, _ := testBroker(t)
	for _, id := range []string{"sender", "target"} {
		if err := b.Hello(broker.SessionInfo{ID: id, Name: id}, nil, false, false); err != nil {
			t.Fatal(err)
		}
	}
	messages := make([]*broker.Message, 3)
	for i := range messages {
		var err error
		messages[i], err = b.Send("sender", broker.SendReq{To: "target", Text: fmt.Sprint(i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	c, err := session("target", broker.SessionInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.nc.Close()
	var taken []*broker.Message
	req := broker.Request{Op: "take", IDs: []string{messages[1].ID}}
	if err := c.call(req, &taken); err != nil {
		t.Fatal(err)
	}
	if len(taken) != 1 || taken[0].ID != messages[1].ID {
		t.Fatalf("selected take consumed unrelated mail: %+v", taken)
	}
	if err := c.call(req, &taken); err != nil || len(taken) != 0 {
		t.Fatalf("selected mail could be taken twice: %+v, %v", taken, err)
	}
	remaining, err := b.Inbox("target")
	if err != nil || len(remaining) != 2 || remaining[0].ID != messages[0].ID || remaining[1].ID != messages[2].ID {
		t.Fatalf("unselected mail changed order or disappeared: %+v, %v", remaining, err)
	}
}

func TestHookTakeBudgetRetainsSuffixAndRefusesOldCapability(t *testing.T) {
	b, _ := testBroker(t)
	if err := b.Hello(broker.SessionInfo{ID: "target"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	for sender := range 2 {
		id := fmt.Sprintf("sender-%d", sender)
		if err := b.Hello(broker.SessionInfo{ID: id}, nil, false, false); err != nil {
			t.Fatal(err)
		}
		for range 10 {
			if _, err := b.Send(id, broker.SendReq{To: "target", Text: strings.Repeat("x", 2000)}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	c, err := session("target", broker.SessionInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.nc.Close()
	c.proto = broker.Protocol // emulate a protocol-2 daemon without selected-take support
	if text, msgs := take(c); text != "" || len(msgs) != 0 {
		t.Fatal("bounded delivery consumed mail without a negotiated capability")
	}
	before, err := b.Inbox("target")
	if err != nil || len(before) != 20 {
		t.Fatalf("old capability lost mail: %d, %v", len(before), err)
	}
	c.proto = 0 // now negotiate the real daemon's capability
	seen := make(map[string]bool)
	for len(seen) < len(before) {
		text, msgs := take(c)
		if len(msgs) == 0 || len(text) > mailBatchMax {
			t.Fatalf("invalid bounded take: %d messages, %d bytes", len(msgs), len(text))
		}
		for _, m := range msgs {
			if seen[m.ID] {
				t.Fatal("message delivered twice")
			}
			seen[m.ID] = true
			if _, err := b.Show("target", m.ID); err != nil {
				t.Fatalf("full message no longer retrievable: %v", err)
			}
		}
		remaining, err := b.Inbox("target")
		if err != nil || len(remaining)+len(seen) != len(before) {
			t.Fatalf("mail disappeared between deliveries: %d remaining, %d delivered, %v", len(remaining), len(seen), err)
		}
		if len(remaining) > 0 && !strings.Contains(text, queuedMailHint) {
			t.Fatal("missing deferred-mail notice")
		}
	}
}

func TestHookMetadataCannotConsumeBodyBudget(t *testing.T) {
	m := &broker.Message{ID: "metadata", From: "peer", FromName: strings.Repeat("🧪", 4000), Text: "short", ExpectsReply: true}
	text, count := formatMail([]*broker.Message{m})
	if count != 1 || len(text) > mailMessageMax || !utf8.ValidString(text) {
		t.Fatalf("invalid metadata budget: %d messages, %d bytes", count, len(text))
	}
	for _, required := range []string{integrations.Frame(), "show metadata", "reply metadata"} {
		if !strings.Contains(text, required) {
			t.Fatalf("metadata crowded out %q", required)
		}
	}
}
