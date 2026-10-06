package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// TestCheckLinkRequest is the gate policy as a table: the op allowlist, the hello
// field stripping, the id/name prefix rules (including values that only gain the
// prefix after CleanLine), the waker harness refusal (including a harness that only
// becomes local after cleaning), the ref refusal, and invalid-op refusals.
func TestCheckLinkRequest(t *testing.T) {
	const host = "srv"
	hello := func(id, name, harness string) *broker.Request {
		return &broker.Request{ID: 7, Op: "hello", Subscribe: true, Wait: true,
			Session: &broker.SessionInfo{
				ID: id, Name: name, Harness: harness, Cwd: "/tmp",
				PID: 99, Pane: "p9", Parent: "mom", Depth: 3,
			}}
	}
	send := func(atts ...broker.Attachment) *broker.Request {
		return &broker.Request{ID: 8, Op: "send",
			SendReq: broker.SendReq{To: "alice", Text: "hi", Attachments: atts}}
	}
	allowed := func(name string, req *broker.Request) {
		t.Helper()
		got, _, rerr := checkLinkRequest(host, req, nil, "")
		if rerr != nil {
			t.Fatalf("%s: refused: %s", name, rerr.Message)
		}
		if got.ID != req.ID || got.Op != req.Op {
			t.Fatalf("%s: rewritten to %+v", name, got)
		}
	}
	refused := func(name string, req *broker.Request, want string) {
		t.Helper()
		if _, _, rerr := checkLinkRequest(host, req, nil, ""); rerr == nil {
			t.Fatalf("%s: allowed", name)
		} else if rerr.Code != broker.CodeBadRequest || !strings.Contains(rerr.Message, "agm link: ") || !strings.Contains(rerr.Message, want) {
			t.Fatalf("%s: wrong refusal: %+v", name, rerr)
		}
	}

	for _, op := range []string{"protocol", "list", "resolve", "inbox", "ack", "take", "history", "show", "wait", "bye"} {
		allowed("op "+op, &broker.Request{ID: 1, Op: op})
	}
	for _, op := range []string{"shutdown", "spawn", "requeue", "bogus", ""} {
		refused("op "+op, &broker.Request{ID: 1, Op: op}, "not allowed")
	}

	// hello is rebuilt from id, name, harness and cwd only; subscribe/wait survive.
	got, _, rerr := checkLinkRequest(host, hello("srv/x", "srv/x", "shell"), nil, "")
	if rerr != nil {
		t.Fatalf("hello: %s", rerr.Message)
	}
	if s := got.Session; s.PID != 0 || s.Pane != "" || s.Parent != "" || s.Depth != 0 {
		t.Fatalf("hello keeps stripped fields: %+v", s)
	}
	if s := got.Session; s.ID != "srv/x" || s.Name != "srv/x" || s.Harness != "shell" || s.Cwd != "/tmp" {
		t.Fatalf("hello drops kept fields: %+v", s)
	}
	if !got.Subscribe || !got.Wait {
		t.Fatalf("hello drops subscribe/wait: %+v", got)
	}

	refused("hello without session", &broker.Request{ID: 1, Op: "hello"}, "needs a session")
	allowed("empty name defaults to the id", hello("srv/x", "", "shell"))
	if got, _, _ := checkLinkRequest(host, hello("srv/x", "", "shell"), nil, ""); got.Session.Name != "srv/x" {
		t.Fatalf("empty name: forwarded as %q", got.Session.Name)
	}
	allowed("prefixed name", hello("srv/x", "srv/team", "shell"))
	allowed("name gains prefix after clean", hello("srv/x", "\nsrv/x", "shell"))
	allowed("long name still prefixed", hello("srv/x", "srv/"+strings.Repeat("x", 100), "shell"))
	refused("local id", hello("bob", "srv/bob", "shell"), "must start with")
	refused("bare prefix id", hello("srv/", "srv/", "shell"), "must start with")
	refused("lookalike prefix", hello("srvx/y", "srvx/y", "shell"), "must start with")
	refused("unprefixed name", hello("srv/y", "bob", "shell"), "must start with")

	allowed("no harness", hello("srv/x", "", ""))
	allowed("openclaw harness", hello("srv/x", "", "openclaw"))
	allowed("codexpad harness", hello("srv/x", "", "codexpad"))
	for _, h := range []string{"codex", "crush", "agy"} {
		refused("harness "+h, hello("srv/x", "", h), "woken locally")
	}
	// The daemon stores the cleaned harness and wakes on that: a raw check would let
	// "codex\n" through as stored "codex".
	for _, h := range []string{" codex", "codex\n", "codex\x7f", "\u200bcodex", "codex\u200b"} {
		refused("dirty harness "+quoted(h), hello("srv/x", "", h), "woken locally")
	}

	allowed("plain send", send())
	allowed("content attachment", send(broker.Attachment{Type: "snippet", Name: "a", Content: "x"}))
	refused("ref attachment", send(broker.Attachment{Type: "ref", Name: "f", Path: "/x"}), "do not cross hosts")
	refused("ref beside content", send(
		broker.Attachment{Type: "snippet", Name: "a", Content: "x"},
		broker.Attachment{Type: "ref", Name: "f", Path: "/x"},
	), "do not cross hosts")
}

func quoted(s string) string { return strings.ReplaceAll(s, "\n", "\\n") }

// linkRemote dials a gate under test and speaks raw protocol on it.
type linkRemote struct {
	t  *testing.T
	nc net.Conn
	sc *bufio.Scanner
}

func dialLink(t *testing.T, sock string) *linkRemote {
	t.Helper()
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	nc.SetReadDeadline(time.Now().Add(15 * time.Second))
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	return &linkRemote{t: t, nc: nc, sc: sc}
}

func (r *linkRemote) say(v any) {
	r.t.Helper()
	if err := json.NewEncoder(r.nc).Encode(v); err != nil {
		r.t.Fatal(err)
	}
}

// sayRaw writes s verbatim: for lines no Go encoder can produce (raw U+2028).
func (r *linkRemote) sayRaw(s string) {
	r.t.Helper()
	if _, err := io.WriteString(r.nc, s); err != nil {
		r.t.Fatal(err)
	}
}

// sayUnescaped writes v as JSON without HTML escaping: the raw line keeps about
// the original size, which is what a real client (or the gate) puts on the wire.
func (r *linkRemote) sayUnescaped(v any) {
	r.t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.nc.Write(buf.Bytes()); err != nil {
		r.t.Fatal(err)
	}
}

func (r *linkRemote) frame() map[string]any {
	r.t.Helper()
	if !r.sc.Scan() {
		r.t.Fatalf("gate closed the connection: %v", r.sc.Err())
	}
	var f map[string]any
	if err := json.Unmarshal(r.sc.Bytes(), &f); err != nil {
		r.t.Fatal(err)
	}
	return f
}

func (r *linkRemote) ok(id float64) map[string]any {
	r.t.Helper()
	f := r.frame()
	if f["id"] != id || f["error"] != nil {
		r.t.Fatalf("want ok response %v, got %v", id, f)
	}
	return f
}

func (r *linkRemote) refused(id float64, want string) {
	r.t.Helper()
	f := r.frame()
	e, _ := f["error"].(map[string]any)
	if f["id"] != id || e == nil || e["code"] != broker.CodeBadRequest || !strings.Contains(e["message"].(string), "agm link: ") || !strings.Contains(e["message"].(string), want) {
		r.t.Fatalf("want agm link refusal mentioning %q, got %v", want, f)
	}
}

// nextEvent reads until a pushed message with the wanted text arrives.
func (r *linkRemote) nextEvent(want string) map[string]any {
	r.t.Helper()
	for {
		f := r.frame()
		if f["event"] != "message" {
			continue
		}
		m, _ := f["message"].(map[string]any)
		if m["text"] == want {
			return m
		}
	}
}

// TestLinkEndToEnd runs a real daemon and the gate on temp sockets (no ssh) and
// checks the foreign side of the policy: stripped hello fields, the three refusal
// classes, two-way delivery through the gate, the ref refusal, and invalid JSON.
func TestLinkEndToEnd(t *testing.T) {
	b, _ := testBroker(t) // AGM_SOCKET: the real daemon under test
	_ = b
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	linkSock := filepath.Join(dir, "l.sock")
	lln, err := broker.Listen(linkSock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lln.Close() })
	go serveLink(context.Background(), lln, "srv", nil)

	ok := func(stdout, stderr string, code int) string {
		t.Helper()
		if code != 0 {
			t.Fatalf("exit %d: %q %q", code, stdout, stderr)
		}
		return strings.TrimSpace(stdout)
	}
	ok(agm(t, "alice-1", "hello", "-name", "alice", "-harness", "shell"))

	r := dialLink(t, linkSock)

	// 1. hello with pid, pane and parent set: list shows none of them.
	r.say(map[string]any{"id": 1, "op": "hello", "subscribe": true,
		"session": map[string]any{"id": "srv/x", "name": "srv/x", "harness": "shell", "cwd": "/tmp",
			"pid": 12345, "pane": "p9", "parent": "mom"}})
	r.ok(1)
	r.say(map[string]any{"id": 2, "op": "list"})
	var list []broker.SessionInfo
	if data, err := json.Marshal(r.ok(2)["result"]); err != nil || json.Unmarshal(data, &list) != nil {
		t.Fatalf("list: %v", r.ok(2))
	}
	found := false
	for _, s := range list {
		if s.ID == "srv/x" {
			found = true
			if s.PID != 0 || s.Pane != "" || s.Parent != "" {
				t.Fatalf("stripped fields leak: %+v", s)
			}
		}
	}
	if !found {
		t.Fatalf("srv/x not listed: %+v", list)
	}

	// 2. hello refusals: a local id, an unprefixed name, a locally-woken harness.
	r.say(map[string]any{"id": 3, "op": "hello", "session": map[string]any{"id": "bob"}})
	r.refused(3, "must start with")
	r.say(map[string]any{"id": 4, "op": "hello", "session": map[string]any{"id": "srv/y", "name": "bob"}})
	r.refused(4, "must start with")
	r.say(map[string]any{"id": 5, "op": "hello", "session": map[string]any{"id": "srv/z", "name": "srv/z", "harness": "codex"}})
	r.refused(5, "woken locally")

	// 3. op refusals, then the daemon still answers.
	r.say(map[string]any{"id": 6, "op": "shutdown"})
	r.refused(6, "not allowed")
	r.say(map[string]any{"id": 7, "op": "spawn", "session": map[string]any{"name": "x"}})
	r.refused(7, "not allowed")
	r.say(map[string]any{"id": 8, "op": "requeue", "messages": []any{}})
	r.refused(8, "not allowed")
	r.say(map[string]any{"id": 9, "op": "protocol"})
	if res, _ := r.ok(9)["result"].(map[string]any); res["protocol"] != float64(broker.Protocol) {
		t.Fatalf("protocol: %v", res)
	}

	// 4. two-way: remote to local, local to remote (pushed through the gate),
	// and a local no-wait ask answered from the remote side.
	r.say(map[string]any{"id": 10, "op": "send", "to": "alice", "text": "knock knock"})
	m := r.ok(10)["result"].(map[string]any)
	if m["to"] != "alice-1" || m["from"] != "srv/x" {
		t.Fatalf("remote send: %v", m)
	}
	if out := ok(agm(t, "alice-1", "inbox")); !strings.Contains(out, "knock knock") {
		t.Fatalf("local inbox: %q", out)
	}
	ok(agm(t, "alice-1", "send", "srv/x", "hello remote"))
	if m := r.nextEvent("hello remote"); m["from"] != "alice-1" {
		t.Fatalf("pushed: %v", m)
	}
	q := ok(agm(t, "alice-1", "ask", "-no-wait", "srv/x", "a question"))
	qm := r.nextEvent("a question")
	r.say(map[string]any{"id": 11, "op": "send", "reply_to": qm["id"], "text": "an answer"})
	r.ok(11)
	if out := ok(agm(t, "alice-1", "wait", "-timeout", "5s", "-reply-to", q)); out != "an answer" {
		t.Fatalf("local wait: %q", out)
	}

	// 5. a ref attachment is refused; a content one passes.
	r.say(map[string]any{"id": 12, "op": "send", "to": "alice", "text": "f",
		"attachments": []any{map[string]any{"type": "ref", "name": "f", "path": "/x"}}})
	r.refused(12, "do not cross hosts")
	r.say(map[string]any{"id": 13, "op": "send", "to": "alice", "text": "with snippet",
		"attachments": []any{map[string]any{"type": "snippet", "name": "a", "content": "x"}}})
	r.ok(13)
	if out := ok(agm(t, "alice-1", "inbox")); !strings.Contains(out, "with snippet") {
		t.Fatalf("snippet send: %q", out)
	}

	// 6. invalid JSON gets bad_request with id 0, and the stream stays usable.
	if _, err := r.nc.Write([]byte("this is not json\n")); err != nil {
		t.Fatal(err)
	}
	r.refused(0, "invalid json")
	r.say(map[string]any{"id": 14, "op": "protocol"})
	r.ok(14)
}

// TestLinkNames checks DEST validation and the NAME default: an alias doubles as
// the prefix, while user@host needs an explicit -name.
func TestLinkNames(t *testing.T) {
	if name, err := linkNames("myhost", ""); err != nil || name != "myhost" {
		t.Fatalf("alias default: %q %v", name, err)
	}
	if name, err := linkNames("ops@203.0.113.7", "srv1"); err != nil || name != "srv1" {
		t.Fatalf("explicit name: %q %v", name, err)
	}
	if name, err := linkNames("ok", strings.Repeat("n", 62)); err != nil || name != strings.Repeat("n", 62) {
		t.Fatalf("62-char name: %q %v", name, err)
	}
	if _, err := linkNames("ok", strings.Repeat("n", 63)); err == nil {
		t.Fatal("63-char name accepted")
	}
	if _, err := linkNames("ops@203.0.113.7", ""); err == nil {
		t.Fatal("user@host without -name accepted")
	}
	for _, dest := range []string{"-x", "a b", "a;b", "", strings.Repeat("d", 256)} {
		if _, err := linkNames(dest, "s"); err == nil {
			t.Fatalf("bad dest %q accepted", dest)
		}
	}
	if _, err := linkNames("ok", "-x"); err == nil {
		t.Fatal("bad name accepted")
	}
}

// TestLinkPreStep checks the pre-step command: dir and path quoted (never raw),
// stale socket removed, absolute dir printed.
func TestLinkPreStep(t *testing.T) {
	script, base := linkPreStep(".agent-mesh/link.sock", linkOpts{})
	if base != "link.sock" {
		t.Fatalf("base: %q", base)
	}
	want := "umask 077 && mkdir -p -- .agent-mesh && rm -f -- .agent-mesh/link.sock && cd -- .agent-mesh && pwd -P"
	if script != want {
		t.Fatalf("script:\n%s", script)
	}
	script, base = linkPreStep("my dir/o'clock.sock", linkOpts{})
	if base != "o'clock.sock" {
		t.Fatalf("base: %q", base)
	}
	want = "umask 077 && mkdir -p -- 'my dir' && rm -f -- 'my dir/o'\\''clock.sock' && cd -- 'my dir' && pwd -P"
	if script != want {
		t.Fatalf("script:\n%s", script)
	}
	if _, base := linkPreStep("a/", linkOpts{}); base != "" {
		t.Fatalf("directory base: %q", base)
	}
}

// fakeSSH writes a recording ssh into dir and returns its log path. The fake
// records argv, fails -R fast (so the loop retries), and answers the pre-step
// from the environment: AGM_FAKE_PRESTEP (default /fake/home/.agent-mesh) on
// stdout, AGM_FAKE_PRESTEP_STDERR on stderr, AGM_FAKE_PRESTEP_EXIT as the status.
func fakeSSH(t *testing.T, dir string) string {
	t.Helper()
	logPath := filepath.Join(dir, "ssh.log")
	script := "#!/bin/sh\n" +
		"printf '<%s>' \"$@\" >>\"" + logPath + "\"; echo >>\"" + logPath + "\"\n" +
		"case \" $* \" in\n" +
		"(*' -R '*) exit 1;;\n" + // the forward fails fast, so the loop retries
		"esac\n" +
		"if [ -n \"$AGM_FAKE_PRESTEP_STDERR\" ]; then printf '%s' \"$AGM_FAKE_PRESTEP_STDERR\" >&2; fi\n" +
		"b=${AGM_FAKE_PRESTEP_BYTES:-0}\n" +
		"if [ \"$b\" -gt 0 ]; then yes | tr '\\n' y | head -c \"$b\"; echo; fi\n" +
		"printf '%s\\n' \"${AGM_FAKE_PRESTEP:-/fake/home/.agent-mesh}\"\n" +
		"exit \"${AGM_FAKE_PRESTEP_EXIT:-0}\"\n"
	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logPath
}

// TestLinkSSHCommand runs the reconnect loop against a fake ssh on PATH: it checks
// the pre-step and -R command lines, that a failing ssh is restarted, and that
// cancel stops the loop.
func TestLinkSSHCommand(t *testing.T) {
	dir := t.TempDir()
	logPath := fakeSSH(t, dir)
	sleeps := 0
	old := linkSleep
	linkSleep = func(time.Duration, context.Context) bool { sleeps++; return true }
	defer func() { linkSleep = old }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	script, base := linkPreStep(linkRemoteSock, linkOpts{})
	go func() {
		defer close(done)
		runLinkSSH(ctx, "ops@203.0.113.7", script, base, "/tmp/local.sock", "", false)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		raw, _ := os.ReadFile(logPath)
		if strings.Count(string(raw), "<-R>") >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("no second -R attempt: %q", raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runLinkSSH did not stop after cancel")
	}
	if sleeps == 0 {
		t.Fatal("no backoff sleep between attempts")
	}
	raw, _ := os.ReadFile(logPath)
	wantPre := "<-o><BatchMode=yes><-o><ConnectTimeout=15><--><ops@203.0.113.7><umask 077 && mkdir -p -- .agent-mesh && rm -f -- .agent-mesh/link.sock && cd -- .agent-mesh && pwd -P>"
	wantR := "<-N><-T><-o><BatchMode=yes><-o><ExitOnForwardFailure=yes><-o><ServerAliveInterval=15><-o><ServerAliveCountMax=3><-R></fake/home/.agent-mesh/link.sock:/tmp/local.sock><--><ops@203.0.113.7>"
	if !strings.Contains(string(raw), wantPre) {
		t.Fatalf("pre-step line:\n%s", raw)
	}
	if !strings.Contains(string(raw), wantR) {
		t.Fatalf("-R line:\n%s", raw)
	}
}

// TestLinkCommandValidation exercises link() argument handling without ssh: every
// case fails before listening or dialing.
func TestLinkCommandValidation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGM_SOCKET", filepath.Join(dir, "mesh.sock"))
	// A validation regression would start a real link (and a real ssh): fail fast
	// on ssh, and SIGINT the suite back if the command still runs after 5 s.
	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	bad := func(args ...string) string {
		t.Helper()
		timer := time.AfterFunc(5*time.Second, func() {
			_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
		})
		defer timer.Stop()
		_, stderr, code := agm(t, "", args...)
		if code == 0 {
			t.Fatalf("%v: unexpectedly started a link", args)
		}
		return stderr
	}
	if out := bad("link", "-x"); !strings.Contains(out, "agm: link") {
		t.Fatalf("bad dest: %q", out)
	}
	if out := bad("link", "a;b"); !strings.Contains(out, "link DEST") {
		t.Fatalf("bad dest chars: %q", out)
	}
	if out := bad("link", "ops@203.0.113.7"); !strings.Contains(out, "-name is required") {
		t.Fatalf("missing name: %q", out)
	}
	if out := bad("link", "-name", "srv1", "-remote-socket", "a/", "myhost"); !strings.Contains(out, "no shell or ssh syntax") {
		t.Fatalf("directory socket: %q", out)
	}
	if out := bad("link", "-name", "srv1", "-remote-socket", "a:b.sock", "myhost"); !strings.Contains(out, "no shell or ssh syntax") {
		t.Fatalf("colon socket: %q", out)
	}
	held, err := broker.Listen(filepath.Join(dir, "link-srv.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if out := bad("link", "-name", "srv", "myhost"); !strings.Contains(out, "a link is already running for srv") {
		t.Fatalf("double link: %q", out)
	}
}

// TestLinkGateFrameSize: re-encoding must not push a valid request over MaxFrame.
// A send with 200 000 `<` (200 044 B raw) reaches the daemon, which answers too_large
// exactly as for a direct client, and the connection stays usable. A send with
// 200 000 U+2028 (600 KB raw, ~1.2 MB encoded: Go always escapes them) is refused
// by the gate itself with too_large.
func TestLinkGateFrameSize(t *testing.T) {
	b, _ := testBroker(t) // AGM_SOCKET: the real daemon under test
	b.Hello(broker.SessionInfo{ID: "peer-1", Name: "peer"}, nil, false, false)
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	linkSock := filepath.Join(dir, "l.sock")
	lln, err := broker.Listen(linkSock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lln.Close() })
	go serveLink(context.Background(), lln, "srv", nil)
	tooLarge := func(f map[string]any, prefix bool) {
		t.Helper()
		e, _ := f["error"].(map[string]any)
		msg, _ := e["message"].(string)
		if e == nil || e["code"] != broker.CodeTooLarge {
			t.Fatalf("want too_large, got %v", f)
		}
		if strings.Contains(msg, "agm link: ") == !prefix {
			t.Fatalf("want prefix=%v, got %q", prefix, msg)
		}
	}

	// The direct client baseline: the daemon answers too_large.
	d := dialLink(t, os.Getenv("AGM_SOCKET"))
	d.say(map[string]any{"id": 1, "op": "hello", "session": map[string]any{"id": "direct-1"}})
	d.ok(1)
	d.sayUnescaped(map[string]any{"id": 2, "op": "send", "to": "peer", "text": strings.Repeat("<", 200000)})
	tooLarge(d.frame(), false)

	// Through the gate: the same too_large, passed through, and the stream lives.
	r := dialLink(t, linkSock)
	r.say(map[string]any{"id": 1, "op": "hello", "session": map[string]any{"id": "srv/big"}})
	r.ok(1)
	r.sayUnescaped(map[string]any{"id": 2, "op": "send", "to": "peer", "text": strings.Repeat("<", 200000)})
	tooLarge(r.frame(), false)
	r.say(map[string]any{"id": 3, "op": "protocol"})
	r.ok(3)

	// U+2028 as raw UTF-8 parses on the gate (600 KB), but still grows when the gate
	// re-encodes it (Go escapes U+2028/U+2029 even with SetEscapeHTML(false)), so
	// the gate refuses it itself. No Go encoder emits raw U+2028: build the line.
	r.sayRaw(`{"id":4,"op":"send","to":"peer","text":"` + strings.Repeat("\u2028", 200000) + `"}` + "\n")
	f := r.frame()
	tooLarge(f, true)
	if !strings.Contains(f["error"].(map[string]any)["message"].(string), "after re-encoding") {
		t.Fatalf("want the re-encoding refusal, got %v", f)
	}
	r.say(map[string]any{"id": 5, "op": "protocol"})
	r.ok(5)
}

// TestLinkConnCap: past maxLinkConns concurrent remote connections, the gate closes
// the new connection without reading it.
func TestLinkConnCap(t *testing.T) {
	testBroker(t)
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	linkSock := filepath.Join(dir, "l.sock")
	lln, err := broker.Listen(linkSock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lln.Close() })
	go serveLink(context.Background(), lln, "srv", nil)
	var held []net.Conn
	t.Cleanup(func() {
		for _, nc := range held {
			nc.Close()
		}
	})
	for range maxLinkConns {
		nc, err := net.Dial("unix", linkSock)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, nc)
	}
	time.Sleep(500 * time.Millisecond) // let the gate accept all 32
	extra, err := net.Dial("unix", linkSock)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	extra.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := bufio.NewReader(extra).ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("over-cap connection not closed: %v", err)
	} else {
		t.Logf("over-cap connection closed: %v", err)
	}

	// A released slot serves again: close one held connection, and a new hello
	// on a fresh connection is answered (fails if slots never release).
	held[0].Close()
	time.Sleep(500 * time.Millisecond)
	r := dialLink(t, linkSock)
	r.say(map[string]any{"id": 1, "op": "hello", "session": map[string]any{"id": "srv/back"}})
	r.ok(1)
}

// TestLinkRefusesBadServerDir runs the real pre-step path against a fake ssh: dirs
// with ssh syntax (${VAR}, % tokens, backslash, brackets, space, ESC) are refused
// before any -R starts; a clean dir builds the right -R remote path.
func TestLinkRefusesBadServerDir(t *testing.T) {
	dir := t.TempDir()
	logPath := fakeSSH(t, dir)
	script, base := linkPreStep(linkRemoteSock, linkOpts{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session := func(output string) bool {
		t.Helper()
		os.WriteFile(logPath, nil, 0o600)
		t.Setenv("AGM_FAKE_PRESTEP", output)
		linkSession(ctx, "myhost", script, base, "/tmp/local.sock", "", false)
		raw, _ := os.ReadFile(logPath)
		return strings.Contains(string(raw), "<-R>")
	}
	for _, bad := range []string{"/tmp/${X}", "/tmp/%u", `/a\b`, "/a:b", "/[a]", "/a b", "/a\x1b[2J", "relative/dir"} {
		if session(bad) {
			t.Fatalf("dir %q started -R", bad)
		}
	}
	os.WriteFile(logPath, nil, 0o600)
	t.Setenv("AGM_FAKE_PRESTEP", "/home/ops/.agent-mesh")
	linkSession(ctx, "myhost", script, base, "/tmp/local.sock", "", false)
	raw, _ := os.ReadFile(logPath)
	if want := "<-R></home/ops/.agent-mesh/link.sock:/tmp/local.sock>"; !strings.Contains(string(raw), want) {
		t.Fatalf("no right -R:\n%s", raw)
	}
}

// TestLinkHomeOutput checks the pre-step parse through the fake ssh: banner lines
// are skipped for the last line, a failed pre-step reports cleaned stderr, and
// 1 MiB of chatter still parses.
func TestLinkHomeOutput(t *testing.T) {
	dir := t.TempDir()
	fakeSSH(t, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	script, _ := linkPreStep(linkRemoteSock, linkOpts{})

	t.Setenv("AGM_FAKE_PRESTEP", "motd line\n/home/ops/.agent-mesh")
	if home, _, _, _, err := linkHome(ctx, "myhost", script, false); err != nil || home != "/home/ops/.agent-mesh" {
		t.Fatalf("banner: %q %v", home, err)
	}

	t.Setenv("AGM_FAKE_PRESTEP_EXIT", "1")
	t.Setenv("AGM_FAKE_PRESTEP_STDERR", "\x1b]0;pwned\x07Permission denied (publickey)")
	_, _, _, _, err := linkHome(ctx, "myhost", script, false)
	if err == nil || !strings.Contains(err.Error(), "Permission denied") || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("stderr: %v", err)
	}

	t.Setenv("AGM_FAKE_PRESTEP_EXIT", "0")
	t.Setenv("AGM_FAKE_PRESTEP_STDERR", "")
	t.Setenv("AGM_FAKE_PRESTEP", "/home/ops/.agent-mesh")
	t.Setenv("AGM_FAKE_PRESTEP_BYTES", strconv.Itoa(1<<20))
	if home, _, _, _, err := linkHome(ctx, "myhost", script, false); err != nil || home != "/home/ops/.agent-mesh" {
		t.Fatalf("1MiB: %q %v", home, err)
	}
}

// TestTailBuffer pins the stdout bound: only the last n bytes survive.
func TestTailBuffer(t *testing.T) {
	var tb tailBuffer
	tb.n = 4
	tb.Write([]byte("abcdef"))
	tb.Write([]byte("gh"))
	if string(tb.buf) != "efgh" {
		t.Fatalf("%q", tb.buf)
	}
}

// TestLinkGateFrameBoundary pins the M1 boundary to the byte: padding mixes U+2028
// (6 bytes re-encoded) with "a" (1 byte) for re-encoded sizes MaxFrame-1, MaxFrame
// and MaxFrame+1. The daemon answers the first two with too_large; the gate
// refuses the third itself. The raw line carries raw UTF-8 U+2028, so it is built
// by hand from the gate's own encoding.
func TestLinkGateFrameBoundary(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "peer-1", Name: "peer"}, nil, false, false)
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	linkSock := filepath.Join(dir, "l.sock")
	lln, err := broker.Listen(linkSock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lln.Close() })
	go serveLink(context.Background(), lln, "srv", nil)

	// C: the gate's encoding of the same request with empty text (quotes+newline
	// included). The gate parses the wire line back into this struct, so its
	// re-encoding is byte-identical to wire0.
	var wire0 bytes.Buffer
	enc0 := json.NewEncoder(&wire0)
	enc0.SetEscapeHTML(false)
	probe := broker.Request{ID: 7, Op: "send", SendReq: broker.SendReq{To: "peer", Text: ""}}
	if err := enc0.Encode(&probe); err != nil {
		t.Fatal(err)
	}
	C := wire0.Len()
	r := dialLink(t, linkSock)
	r.say(map[string]any{"id": 1, "op": "hello", "session": map[string]any{"id": "srv/b"}})
	r.ok(1)
	for i, target := range []int{broker.MaxFrame - 1, broker.MaxFrame, broker.MaxFrame + 1} {
		// Exact padding: m "a" plus U+2028 runes. The wire line swaps each \u2028
		// escape for 3 raw bytes; the gate's re-encoding of the parsed request is
		// what must measure target, so verify with the gate's own encoder.
		const k = 1000
		m := target - C + 2 - 6*k
		text := strings.Repeat("\u2028", k) + strings.Repeat("a", m)
		var w0 bytes.Buffer
		enc := json.NewEncoder(&w0)
		enc.SetEscapeHTML(false)
		req := broker.Request{ID: int64(7 + i), Op: "send", SendReq: broker.SendReq{To: "peer", Text: text}}
		if err := enc.Encode(&req); err != nil {
			t.Fatal(err)
		}
		if d := target - w0.Len(); d != 0 { // one empirical correction pass
			text = strings.Repeat("\u2028", k) + strings.Repeat("a", m+d)
			req.SendReq.Text = text
			w0.Reset()
			if err := enc.Encode(&req); err != nil {
				t.Fatal(err)
			}
		}
		if w0.Len() != target {
			t.Fatalf("re-encoded size %d, want %d", w0.Len(), target)
		}
		wire := strings.ReplaceAll(w0.String(), `\u2028`, "\u2028")
		if len(wire) >= broker.MaxFrame {
			t.Fatalf("raw line does not fit: %d", len(wire))
		}
		r.sayRaw(wire)
		f := r.frame()
		e, _ := f["error"].(map[string]any)
		msg, _ := e["message"].(string)
		if e == nil || e["code"] != broker.CodeTooLarge {
			t.Fatalf("size %d: want too_large, got %v", target, f)
		}
		if hasPrefix := strings.Contains(msg, "agm link: "); hasPrefix == (target <= broker.MaxFrame) {
			t.Fatalf("size %d: wrong owner: %q", target, msg)
		}
		r.say(map[string]any{"id": 100 + i, "op": "protocol"})
		r.ok(float64(100 + i))
	}
}

// TestLinkPreStepTimeout pins N8: with a short linkPreStepTimeout and a pre-step
// that hangs in the foreground (`sleep 30 & wait`, no exit: the timeout must kill
// the shell), linkHome returns in about timeout + WaitDelay, not the child's
// lifetime, and not never.
func TestLinkPreStepTimeout(t *testing.T) {
	dir := t.TempDir()
	logPath := fakeSSH(t, dir)
	_ = logPath
	// The fake hangs in the foreground: the timeout kills the shell, and WaitDelay
	// ends the pipe wait that the lingering sleep child would otherwise stretch.
	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte("#!/bin/sh\nsleep 30 & wait\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := linkPreStepTimeout
	linkPreStepTimeout = 2 * time.Second
	defer func() { linkPreStepTimeout = old }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, _, _, _, err := linkHome(ctx, "myhost", "true", false)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("pre-step unexpectedly succeeded")
	}
	// The timeout is 2 s and WaitDelay 3 s: anything near 30 s means Wait hung on
	// the pipes; anything past timeout+WaitDelay+slack means the child stretched it.
	if elapsed > 8*time.Second {
		t.Fatalf("linkHome took %v; the pipe wait outlived the timeout", elapsed)
	}
	t.Logf("linkHome returned after %v: %v", elapsed, err)
}

// --- 3c.2b: the -bridge wiring -------------------------------------------------

// T10: parsePreStep against pre-step output shapes. The parse reads from the
// end (status json, version line, dir) and tolerates anything before them;
// each malformed shape gets its own clear error.
func TestLinkParsePreStep(t *testing.T) {
	dir := "/home/ops/.agent-mesh"
	ver := "v0.4.1"
	js := `{"daemon":{"running":true,"socket":"/home/ops/.agent-mesh/mesh.sock"},"targets":[]}`
	sock := "/home/ops/.agent-mesh/mesh.sock"
	for _, tc := range []struct {
		name    string
		out     string
		home    string
		version string
		socket  string
		errSub  string
	}{
		{name: "good", out: dir + "\n" + ver + "\n" + js + "\n", home: dir, version: ver, socket: sock},
		{name: "rc noise before the answers", out: "motd: welcome\nlast login: yesterday\n" + dir + "\n" + ver + "\n" + js + "\n", home: dir, version: ver, socket: sock},
		{name: "version line is an error", out: dir + "\nagm: not found\n" + js + "\n", home: dir, version: "agm: not found", socket: sock},
		{name: "missing agm, one line only", out: dir + "\n", errSub: "too short"},
		{name: "empty output", out: "", errSub: "too short"},
		{name: "garbage json", out: dir + "\n" + ver + "\nnot json at all\n", errSub: "status -json"},
		{name: "short json", out: dir + "\n" + ver + "\n{}\n", errSub: "not absolute"},
		{name: "relative socket", out: dir + "\n" + ver + `` + "\n" + `{"daemon":{"socket":"agent-mesh/mesh.sock"}}` + "\n", errSub: "not absolute"},
		{name: "bad charset in socket", out: dir + "\n" + ver + "\n" + `{"daemon":{"socket":"/home/o ps/mesh.sock"}}` + "\n", errSub: "not a plain path"},
		{name: "dir not absolute", out: "home/ops\n" + ver + "\n" + js + "\n", errSub: "unexpected pre-step output"},
		{name: "trailing blank lines", out: dir + "\n" + ver + "\n" + js + "\n\n\n", home: dir, version: ver, socket: sock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePreStep([]byte(tc.out))
			if tc.errSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSub) {
					t.Fatalf("want error containing %q, got %v", tc.errSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.home != tc.home || got.version != tc.version || got.socket != tc.socket {
				t.Fatalf("got %+v, want {%s %s %s}", got, tc.home, tc.version, tc.socket)
			}
		})
	}
}

// T10: the ssh argument vector. The -R gate forward always; -bridge adds the
// -L daemon forward and StreamLocalBindUnlink on the same connection.
func TestLinkSSHArgs(t *testing.T) {
	base := []string{"-N", "-T", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3"}
	if got := linkSSHArgs("srv", "/h/link.sock", "/l/link-srv.sock", "", "", false); !slices.Equal(got,
		append(slices.Clone(base), "-R", "/h/link.sock:/l/link-srv.sock", "--", "srv")) {
		t.Fatalf("plain: %v", got)
	}
	want := append(slices.Clone(base),
		"-R", "/h/link.sock:/l/link-srv.sock",
		"-o", "StreamLocalBindUnlink=yes",
		"-L", "/l/link-srv-remote.sock:/h/mesh.sock",
		"--", "srv")
	if got := linkSSHArgs("srv", "/h/link.sock", "/l/link-srv.sock", "/l/link-srv-remote.sock", "/h/mesh.sock", true); !slices.Equal(got, want) {
		t.Fatalf("bridge: %v", got)
	}
}

// T10: the -local-name default and its validation, and the sun_path checks.
func TestLinkLocalAndSockChecks(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"foo.bar.local", "foo"}, {"foo", "foo"}, {"", ""},
	} {
		if got := shortHostName(tc.in); got != tc.want {
			t.Errorf("shortHostName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if err := checkSockPath(strings.Repeat("x", 50)); err != nil {
		t.Errorf("a 50-byte path refused: %v", err)
	}
	if err := checkSockPath(strings.Repeat("x", 200)); err == nil || !strings.Contains(err.Error(), "unix socket path is limited") {
		t.Errorf("a 200-byte path not clearly refused: %v", err)
	}
	max := 103
	if runtime.GOOS == "linux" {
		max = 107
	}
	if err := checkSockPath(strings.Repeat("x", max)); err != nil {
		t.Errorf("the per-OS maximum (%d) refused: %v", max, err)
	}
	if err := checkSockPath(strings.Repeat("x", max+1)); err == nil {
		t.Errorf("one past the per-OS maximum accepted")
	}
}

// TestLinkLifecycleHelper is the child side of the lifecycle probe below: it
// runs the real CLI entry with the args the parent passed after --.
func TestLinkLifecycleHelper(t *testing.T) {
	if os.Getenv("AGM_TEST_LINK") != "1" {
		t.Skip("helper process")
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			if err := run("", "link", args[i+2:]); err != nil { // args[i+1] is "link"
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	os.Exit(2)
}

// T10b (review B1): the positive CLI lifecycle, plain and -bridge, through a
// fake ssh on PATH and an isolated HOME and socket: the link must actually
// reach ssh (the gate listener must not block the startup path), and SIGTERM
// must end it cleanly within 3 s.
func TestLinkStartsSSHAndStopsOnSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals")
	}
	for _, bridge := range []bool{false, true} {
		label := "plain"
		args := []string{"link", "-name", "srv", "review-host"}
		if bridge {
			label = "bridge"
			args = []string{"link", "-name", "srv", "-bridge", "-local-name", "laptop", "review-host"}
		}
		t.Run(label, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "agm-ll")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) }) // short: sun_path limits
			bin := filepath.Join(dir, "bin")
			if err := os.MkdirAll(bin, 0o755); err != nil {
				t.Fatal(err)
			}
			sshLog := filepath.Join(dir, "ssh.log")
			script := "#!/bin/sh\n" +
				"printf '%s\\n' \"$@\" >> " + shellQuote(sshLog) + "\n" +
				"case \" $* \" in (*' -N '*) exit 1;; esac\n" +
				"if [ \"$AGM_TEST_BRIDGE\" = 1 ]; then\n" +
				"printf '%s\\n' /tmp/agm-remote dev '{\"daemon\":{\"running\":true,\"socket\":\"/tmp/agm-remote/mesh.sock\"}}'\n" +
				"else\nprintf '%s\\n' /tmp/agm-remote\nfi\n"
			if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			sock := filepath.Join(dir, "m.sock")
			cmd := exec.Command(os.Args[0], "-test.run=TestLinkLifecycleHelper", "--")
			cmd.Args = append(cmd.Args, args...)
			cmd.Env = append(os.Environ(),
				"AGM_TEST_LINK=1",
				"AGM_TEST_BRIDGE="+map[bool]string{true: "1", false: "0"}[bridge],
				"HOME="+dir,
				"AGM_SOCKET="+sock,
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			)
			cmd.Stdout = io.Discard
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			// wait for the FORWARD invocation (the second ssh call): the log
			// grows the pre-step line first, then -R (and -L with -bridge)
			want := "-R"
			if bridge {
				want += " -L"
			}
			forwarded := false
			for i := 0; i < 160 && !forwarded; i++ {
				time.Sleep(25 * time.Millisecond)
				data, _ := os.ReadFile(sshLog)
				txt := strings.ReplaceAll(string(data), "\n", " ")
				if strings.Contains(txt, "-R") && (!bridge || strings.Contains(txt, "-L")) {
					forwarded = true
				}
				select {
				case <-done:
					t.Fatalf("the link exited before the forward came up (startup blocked)")
				default:
				}
			}
			if !forwarded {
				cmd.Process.Kill()
				t.Fatal("the ssh forward was never invoked: the gate listener blocks the startup path")
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				cmd.Process.Kill()
				t.Fatal("SIGTERM did not end the link within 3 s")
			}
			if ec := cmd.ProcessState.ExitCode(); ec != 0 {
				t.Fatalf("the link exited %d on SIGTERM, want 0", ec)
			}
			data, _ := os.ReadFile(sshLog)
			if !strings.Contains(string(data), "-R") {
				t.Errorf("ssh args lack the gate forward: %q", string(data))
			}
			if bridge && !strings.Contains(string(data), "-L") {
				t.Errorf("bridge ssh args lack the -L forward: %q", string(data))
			}
			if !bridge && strings.Contains(string(data), "-L") {
				t.Errorf("plain link got a -L forward: %q", string(data))
			}
		})
	}
}

// T10c/T10d (review B6/B5): a persisting pre-step failure logs once per
// change, not once per retry; and the remote version line is sanitized and
// bounded before it reaches the log.
func TestLinkPreStepFailureLogsOnce(t *testing.T) {
	linkErrMu.Lock()
	linkErrStr = map[string]string{} // the log-once state is per-process: forget it between -count runs
	linkErrMu.Unlock()
	fakeSSH(t, t.TempDir())
	t.Setenv("AGM_FAKE_PRESTEP_EXIT", "127")
	t.Setenv("AGM_FAKE_PRESTEP_STDERR", "agm: command not found")
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(old) })
	for range 2 {
		linkSession(
			context.Background(), "review-missing-agm", "true", "link.sock",
			"/tmp/review-local.sock", "/tmp/review-remote.sock", true,
		)
	}
	if n := strings.Count(logs.String(), "pre-step:"); n != 1 {
		t.Fatalf("same pre-step error logged %d times; expected 1; log=%q", n, logs.String())
	}
}

func TestLinkVersionLogClean(t *testing.T) {
	linkVersionMu.Lock()
	linkVersions = map[string]string{} // the version log is once per process: forget it between -count runs
	linkVersionMu.Unlock()
	fakeSSH(t, t.TempDir())
	t.Setenv("AGM_FAKE_PRESTEP", "/tmp/remote\nv0.5.0\x1b[2J\n"+
		`{"daemon":{"running":true,"socket":"/tmp/remote/mesh.sock"}}`)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(old) })
	linkSession(
		context.Background(), "review-version", "true", "link.sock",
		"/tmp/review-local.sock", "/tmp/review-remote.sock", true,
	)
	if strings.Contains(logs.String(), "\x1b") {
		t.Fatalf("untrusted version control sequence reached log: %q", logs.String())
	}
	if !strings.Contains(logs.String(), broker.CleanLine("v0.5.0\x1b[2J", 300)) {
		t.Fatal("version log missing")
	}
}

// T10e (late review, gate-only recovery): DEST's agm answers garbage first,
// so the link comes up -R only with the bridge waiting; the pre-step poll
// notices when DEST's agm answers properly, tears the -R-only session down,
// and the next session carries both forwards. The fake ssh stays alive (a
// long-lived -R), which is what the old code could never recover from.
func TestLinkGateOnlyRecoversWhenAgmReturns(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agm-gr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) }) // short: sun_path limits
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	sshLog := filepath.Join(dir, "ssh.log")
	prestep := filepath.Join(dir, "prestep")
	// dir only: the gate can come up, but DEST's agm did not answer
	if err := os.WriteFile(prestep, []byte("/tmp/agm-remote\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" >> " + shellQuote(sshLog) + "\n" +
		"case \" $* \" in (*' -N '*) exec sleep 30;; esac\n" +
		"cat " + shellQuote(prestep) + "\n"
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	savedPATH := os.Getenv("PATH")
	t.Setenv("PATH", bin+string(os.PathListSeparator)+savedPATH)
	logs := filepath.Join(dir, "link.log") // silence linkSession's log chatter
	f, err := os.OpenFile(logs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	oldW := log.Writer()
	log.SetOutput(f)
	t.Cleanup(func() { log.SetOutput(oldW); f.Close() })

	script2, base := linkPreStep(linkRemoteSock, linkOpts{bridge: true, remoteAgm: "agm"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		linkSession(ctx, "agm-gone", script2, base, "/tmp/agm-gate.sock", "/tmp/agm-far.sock", true)
	}()
	// the gate-only forward is up: -R without -L
	gateOnly := false
	for i := 0; i < 120 && !gateOnly; i++ {
		time.Sleep(50 * time.Millisecond)
		data, _ := os.ReadFile(sshLog)
		txt := strings.ReplaceAll(string(data), "\n", " ")
		gateOnly = strings.Contains(txt, "-R") && !strings.Contains(txt, "-L")
	}
	if !gateOnly {
		t.Fatal("the -R-only gate session never came up")
	}
	select {
	case <-done:
		t.Fatal("linkSession returned before the pre-step poll could recover it")
	default:
	}
	// DEST's agm is fixed; the poll (5 s) must tear the -R session down and return
	if err := os.WriteFile(prestep, []byte("/tmp/agm-remote\nv0.5.0\n{\"daemon\":{\"running\":true,\"socket\":\"/tmp/agm-remote/mesh.sock\"}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("a healthy pre-step never ended the gate-only session: the -L retry cannot happen while ssh -R lives")
	}
	// and the next session runs with both forwards (async: the fake ssh
	// sleeps 30 s, and only the log line matters here)
	go linkSession(ctx, "agm-back", script2, base, "/tmp/agm-gate.sock", "/tmp/agm-far.sock", true)
	gotL := false
	for i := 0; i < 100 && !gotL; i++ {
		time.Sleep(50 * time.Millisecond)
		data, _ := os.ReadFile(sshLog)
		gotL = strings.Contains(strings.ReplaceAll(string(data), "\n", " "), "-L")
	}
	if !gotL {
		t.Fatal("no -L forward after recovery")
	}
	cancel()
}
