package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

func TestMessageBody(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	odd := "rev iew ğ'ş.md"
	os.WriteFile(filepath.Join(dir, odd), []byte("x"), 0o600)
	os.WriteFile(filepath.Join(dir, "b.md"), []byte("y"), 0o600)
	os.WriteFile(filepath.Join(dir, "body.txt"), []byte("line1\nçok satır 😀\n"), 0o600)

	// Positional text, including a literal "-", stays as before.
	if text, atts, err := messageBody([]string{"-"}, "", nil, strings.NewReader("stdin")); err != nil || text != "-" || atts != nil {
		t.Fatalf("literal dash: %q %v %v", text, atts, err)
	}
	if text, _, err := messageBody(nil, "body.txt", nil, nil); err != nil || text != "line1\nçok satır 😀\n" {
		t.Fatalf("-file: %q %v", text, err)
	}
	if text, _, err := messageBody(nil, "-", nil, strings.NewReader("from stdin\n")); err != nil || text != "from stdin\n" {
		t.Fatalf("-file -: %q %v", text, err)
	}
	if _, _, err := messageBody([]string{"hi"}, "body.txt", nil, nil); err == nil {
		t.Fatal("text and -file together accepted")
	}
	if _, _, err := messageBody(nil, "", nil, nil); err == nil {
		t.Fatal("empty message accepted")
	}
	os.WriteFile(filepath.Join(dir, "bin"), []byte{0xff, 0xfe}, 0o600)
	if _, _, err := messageBody(nil, "bin", nil, nil); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
	os.WriteFile(filepath.Join(dir, "big"), []byte(strings.Repeat("a", broker.MaxMessage+1)), 0o600)
	if _, _, err := messageBody(nil, "big", nil, nil); err == nil || !strings.Contains(err.Error(), "-ref") {
		t.Fatalf("oversize -file: %v", err)
	}

	// Refs: relative → absolute from cwd, repeated, auto text only when no body.
	text, atts, err := messageBody(nil, "", []string{odd, "b.md", "b.md"}, nil)
	if err != nil || len(atts) != 3 || atts[0].Path != filepath.Join(dir, odd) || atts[0].Name != odd || atts[0].Content != "" || text != "Shared file(s): "+odd+", b.md, b.md" {
		t.Fatalf("refs: %q %+v %v", text, atts, err)
	}
	if text, _, _ := messageBody([]string{"please", "review"}, "", []string{"b.md"}, nil); text != "please review" {
		t.Fatalf("caption: %q", text)
	}
	if _, _, err := messageBody(nil, "", []string{"missing.md"}, nil); err == nil || !strings.Contains(err.Error(), "missing.md") {
		t.Fatalf("missing ref: %v", err)
	}
	if _, _, err := messageBody(nil, "", []string{dir}, nil); err == nil {
		t.Fatal("directory ref accepted")
	}
}

func TestFormatMail(t *testing.T) {
	body := strings.Repeat("é", hookBodyMax) // cut falls inside a 2-byte rune
	out := formatMail([]*broker.Message{
		{ID: "m1", From: "a", FromName: "alice", Text: body, ExpectsReply: true,
			Attachments: []broker.Attachment{{Type: "ref", Name: "x y.md", Path: "/tmp/x y.md"}, {Type: "snippet", Name: "s", Content: strings.Repeat("c", 5000)}}},
		{ID: "m2", From: "a", Text: "short", ReplyTo: "m0"},
	})
	if !utf8.ValidString(out) {
		t.Fatal("invalid UTF-8")
	}
	for _, want := range []string{"(truncated)", "show m1", "reply m1", "file: '/tmp/x y.md'", "file-read tool", "attachment snippet", "[REPLY m2 re m0]"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, strings.Repeat("c", 400)) {
		t.Error("attachment content not bounded")
	}
	if strings.Contains(out, "show m2") {
		t.Error("show hint on a short message")
	}
}

func TestShortIDs(t *testing.T) {
	a, b := "0199aaaa-bbbb-7000-8000-000000000001", "0199aaaa-bbbb-7000-8000-000000000002"
	got := shortIDs([]string{a, b, "0199aaab-cccc", "short", "ğğğğğğğğğx", "ğğğğğğğğğy"})
	if got[a] != a || got[b] != b {
		t.Errorf("colliding UUIDv7s must stay full: %v", got)
	}
	if got["0199aaab-cccc"] != "0199aaab" || got["short"] != "short" {
		t.Errorf("%v", got)
	}
	if got["ğğğğğğğğğx"] != "ğğğğğğğğğx" || !utf8.ValidString(got["ğğğğğğğğğy"]) {
		t.Errorf("rune-safe: %v", got)
	}
	for id, p := range got {
		for other := range got {
			if other != id && strings.HasPrefix(other, p) {
				t.Errorf("%q is a prefix of %q too", p, other)
			}
		}
	}
}
