package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

// agm runs one CLI command in-process; errOut is what main would print for its error.
func agm(t *testing.T, as string, args ...string) (stdout, errOut string, code int) {
	t.Helper()
	out, stderr, err := capture(t, func() error { return run(as, args[0], args[1:]) })
	var ec exitCode
	switch {
	case errors.As(err, &ec):
		code = int(ec)
	case err != nil:
		var buf bytes.Buffer
		code = reportError(err, &buf)
		stderr += buf.String()
	}
	return out, stderr, code
}

func jsonError(t *testing.T, stderr string) (code string) {
	t.Helper()
	var e struct {
		Error struct{ Code, Message string }
	}
	if err := json.Unmarshal([]byte(stderr), &e); err != nil || e.Error.Code == "" || e.Error.Message == "" {
		t.Fatalf("stderr is not one JSON error: %q", stderr)
	}
	return e.Error.Code
}

func TestCLIJSONAndFlags(t *testing.T) {
	b, dir := testBroker(t)
	t.Chdir(dir)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)
	os.WriteFile(filepath.Join(dir, "-json"), []byte("x"), 0o600)
	lastText := func() broker.Message {
		in, _ := b.Inbox("bob-1")
		return *in[len(in)-1]
	}

	// Default stdout unchanged: the id only.
	out, _, code := agm(t, "alice-1", "send", "bob", "hi")
	if code != 0 || strings.TrimSpace(out) != lastText().ID {
		t.Fatalf("send: %q %d", out, code)
	}
	// -json: the Message.
	out, _, _ = agm(t, "alice-1", "send", "-json", "bob", "hi")
	var m broker.Message
	if json.Unmarshal([]byte(out), &m) != nil || m.ID != lastText().ID || m.Text != "hi" || m.To != "bob-1" {
		t.Fatalf("send -json: %q", out)
	}
	// "-json" after -- (before the target), or as a flag value, is not the flag.
	// A bare "-json" after the target is a usage error (see TestTrailingFlagGuard).
	for _, args := range [][]string{{"send", "--", "bob", "-json"}, {"send", "-ref", "-json", "bob"}} {
		out, stderr, code := agm(t, "alice-1", args...)
		if code != 0 || strings.HasPrefix(out, "{") {
			t.Fatalf("%v: %q %q", args, out, stderr)
		}
	}
	if got := lastText(); len(got.Attachments) != 1 || got.Attachments[0].Path != filepath.Join(dir, "-json") {
		t.Fatalf("-ref -json: %+v", got)
	}
	out, _, _ = agm(t, "alice-1", "send", "-json", "--", "bob", "-json")
	if json.Unmarshal([]byte(out), &m) != nil || m.Text != "-json" {
		t.Fatalf("-json -- bob -json: %q", out)
	}

	// Errors: JSON only on stderr when -json was parsed; usage exits 2.
	for _, args := range [][]string{{"send", "-json", "-bogus", "bob", "hi"}, {"ask", "-json", "-timeout", "nope", "bob", "hi"}, {"send", "-json"}} {
		out, stderr, code := agm(t, "alice-1", args...)
		if out != "" || code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%v: %q %q %d", args, out, stderr, code)
		}
	}
	_, stderr, code := agm(t, "alice-1", "send", "-json", "nobody", "hi")
	if code != 1 || jsonError(t, stderr) != broker.CodeUnknownTarget {
		t.Fatalf("unknown target: %q", stderr)
	}
	_, stderr, _ = agm(t, "alice-1", "send", "-json", "-ref", "missing.md", "bob")
	if jsonError(t, stderr) != codeInput {
		t.Fatalf("missing ref: %q", stderr)
	}
	// -json does not leak into the next command.
	_, stderr, _ = agm(t, "alice-1", "send", "nobody", "hi")
	if !strings.HasPrefix(stderr, "agm: unknown_target") {
		t.Fatalf("human error after a JSON one: %q", stderr)
	}
	if _, stderr, code := agm(t, "alice-1", "send", "-h"); code != 0 || !strings.Contains(stderr, "-json") {
		t.Fatalf("-h: %d %q", code, stderr)
	}
}

func TestWhoamiResolve(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice", Harness: "shell"}, nil, false, false)
	before := b.List()

	out, _, code := agm(t, "alice-1", "whoami", "-json")
	var who whoami
	if code != 0 || json.Unmarshal([]byte(out), &who) != nil || who.ID != "alice-1" || !who.Registered || who.Session.Name != "alice" || who.Source != asSource {
		t.Fatalf("whoami: %q", out)
	}
	out, _, _ = agm(t, "ghost-9", "whoami", "-json")
	who = whoami{}
	if json.Unmarshal([]byte(out), &who) != nil || who.Registered || who.Session != nil {
		t.Fatalf("unregistered: %q", out)
	}
	for _, env := range []string{"AGM_SESSION", "CODEX_THREAD_ID", "ANTIGRAVITY_CONVERSATION_ID"} {
		t.Setenv(env, "")
	}
	if _, stderr, code := agm(t, "", "whoami", "-json"); code != 1 || jsonError(t, stderr) != codeNoIdentity {
		t.Fatalf("no identity: %q", stderr)
	}

	out, _, _ = agm(t, "", "resolve", "-json", "ALICE@shell")
	var info broker.SessionInfo
	if json.Unmarshal([]byte(out), &info) != nil || info.ID != "alice-1" {
		t.Fatalf("resolve: %q", out)
	}
	// Human output shows the full id even when ids share a long prefix.
	b.Hello(broker.SessionInfo{ID: "01a0e8cf-1111-7000-8000-000000000001", Name: "twin1"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "01a0e8cf-1111-7000-8000-000000000002", Name: "twin2"}, nil, false, false)
	if out, _, _ := agm(t, "", "resolve", "twin2"); !strings.Contains(out, "session  01a0e8cf-1111-7000-8000-000000000002\n") {
		t.Fatalf("resolve human: %q", out)
	}
	if _, stderr, _ := agm(t, "", "resolve", "-json", "nobody"); jsonError(t, stderr) != broker.CodeUnknownTarget {
		t.Fatalf("resolve unknown: %q", stderr)
	}
	after := b.List()
	if len(after) != 3 || !after[2].LastSeen.Equal(before[0].LastSeen) || after[2].ID != "alice-1" {
		t.Fatalf("whoami/resolve changed sessions: %+v", after)
	}
}

func TestInboxAckNeedsOutput(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)
	b.Send("alice-1", broker.SendReq{To: "bob", Text: "keep me"}, nil)
	for _, args := range [][]string{{"inbox", "-ack"}, {"inbox", "-ack", "-json"}} {
		r, w, _ := os.Pipe()
		old := os.Stdout
		os.Stdout = r // read end: every write fails
		err := run("bob-1", args[0], args[1:])
		os.Stdout = old
		r.Close()
		w.Close()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Code != codeOutput {
			t.Fatalf("%v: %v", args, err)
		}
		if in, _ := b.Inbox("bob-1"); len(in) != 1 {
			t.Fatalf("%v acked without output", args)
		}
	}
}

func TestStatusJSONNoDaemon(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	sock := filepath.Join(dir, "none", "s.sock")
	t.Setenv("AGM_SOCKET", sock)
	out, _, code := agm(t, "", "status", "-json", "pi", "claude")
	var st statusJSON
	if code != 0 || json.Unmarshal([]byte(out), &st) != nil || st.Daemon.Running || st.Daemon.Socket != sock || len(st.Targets) != 2 || st.Targets[0].Name != "pi" || st.Targets[0].Delivery == "" {
		t.Fatalf("status -json: %q", out)
	}
	if _, err := os.Stat(filepath.Dir(sock)); !os.IsNotExist(err) {
		t.Fatal("status started a daemon")
	}
	if _, stderr, code := agm(t, "", "status", "-json", "nope"); code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("unknown harness: %q", stderr)
	}
}

func TestNoExtraArgs(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	for _, args := range [][]string{{"list", "-json", "x"}, {"hello", "x"}, {"history", "-json", "bob"}, {"inbox", "-json", "ack"}, {"whoami", "-json", "x"}} {
		out, stderr, code := agm(t, "alice-1", args...)
		if out != "" || code != 2 || (args[1] == "-json" && jsonError(t, stderr) != codeUsage) {
			t.Fatalf("%v: %q %q %d", args, out, stderr, code)
		}
	}
}

func TestAskNoWaitAndWait(t *testing.T) {
	b, dir := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)
	ref := filepath.Join(dir, "r.md")
	os.WriteFile(ref, []byte("x"), 0o600)

	// -no-wait prints the id at once (plain) or the Message (-json).
	out, _, code := agm(t, "alice-1", "ask", "-no-wait", "bob", "q1?")
	q1 := strings.TrimSpace(out)
	if code != 0 || len(q1) != 16 {
		t.Fatalf("ask -no-wait: %q", out)
	}
	out, _, _ = agm(t, "alice-1", "ask", "-json", "-no-wait", "bob", "q2?")
	var q2 broker.Message
	if json.Unmarshal([]byte(out), &q2) != nil || !q2.ExpectsReply {
		t.Fatalf("ask -json -no-wait: %q", out)
	}
	// The receiver still sees a question with reply instructions.
	in, _ := b.Inbox("bob-1")
	if hint := formatMail(in); !strings.Contains(hint, "asked for a reply") || strings.Contains(hint, "blocked") {
		t.Fatalf("hint: %s", hint)
	}

	// Early answer (before wait), then acked by an adapter: wait still finds it in history.
	b.Send("bob-1", broker.SendReq{ReplyTo: q1, Text: "early", Attachments: []broker.Attachment{{Type: "ref", Name: "r.md", Path: ref}}}, nil)
	b.Send("bob-1", broker.SendReq{To: "alice", Text: "unrelated"}, nil)
	in, _ = b.Inbox("alice-1")
	b.Ack("alice-1", []string{in[0].ID})
	out, stderr, code := agm(t, "alice-1", "wait", "-reply-to", q1)
	if code != 0 || out != "early\n" || !strings.Contains(stderr, shellQuote(ref)) {
		t.Fatalf("wait (early): %q %q %d", out, stderr, code)
	}

	// Live answer while waiting; JSON = complete message; unrelated mail untouched.
	go func() {
		time.Sleep(200 * time.Millisecond)
		b.Send("bob-1", broker.SendReq{ReplyTo: q2.ID, Text: "live"}, nil)
	}()
	out, _, code = agm(t, "alice-1", "wait", "-json", "-timeout", "5s", "-reply-to", q2.ID)
	var r broker.Message
	if code != 0 || json.Unmarshal([]byte(out), &r) != nil || r.Text != "live" || r.ReplyTo != q2.ID {
		t.Fatalf("wait (live): %q", out)
	}
	if in, _ := b.Inbox("alice-1"); len(in) != 2 { // "unrelated" + the live reply, nothing acked
		t.Fatalf("wait changed the mailbox: %d", len(in))
	}

	// Timeout: coded, nothing removed; another session's question is refused.
	q3, _ := b.Send("alice-1", broker.SendReq{To: "bob", Text: "q3?", ExpectsReply: true, NoWait: true}, nil)
	_, stderr, code = agm(t, "alice-1", "wait", "-json", "-timeout", "100ms", "-reply-to", q3.ID)
	if code != 1 || jsonError(t, stderr) != codeTimeout {
		t.Fatalf("timeout: %q", stderr)
	}
	if _, stderr, _ := agm(t, "bob-1", "wait", "-json", "-reply-to", q3.ID); jsonError(t, stderr) != broker.CodeUnknownMsg {
		t.Fatalf("foreign: %q", stderr)
	}
	if _, stderr, code := agm(t, "alice-1", "wait", "-json"); code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("usage: %q", stderr)
	}
	if _, stderr, _ := agm(t, "alice-1", "send", "-json", "-no-wait", "bob", "x"); jsonError(t, stderr) != codeUsage {
		t.Fatalf("send -no-wait: %q", stderr)
	}
}

func TestWaitDeadline(t *testing.T) {
	// A daemon that accepts and never answers: the whole wait must still end on time.
	var stallHello atomic.Bool
	dir, _ := os.MkdirTemp("/tmp", "agm")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	t.Setenv("AGM_SOCKET", sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { // answer hello only (unless stallHello), then stay silent
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					var req broker.Request
					json.Unmarshal(sc.Bytes(), &req)
					if req.Op == "protocol" && !stallHello.Load() { // so the stall is the wait itself
						fmt.Fprintf(c, `{"id":%d,"result":{"protocol":%d}}`+"\n", req.ID, broker.Protocol)
					}
					if req.Op == "hello" && !stallHello.Load() {
						fmt.Fprintf(c, `{"id":%d,"result":{"id":"a"}}`+"\n", req.ID)
					}
				}
			}()
		}
	}()
	start := time.Now()
	_, stderr, code := agm(t, "alice-1", "wait", "-json", "-timeout", "300ms", "-reply-to", "q")
	if code != 1 || jsonError(t, stderr) != codeTimeout || time.Since(start) > 3*time.Second {
		t.Fatalf("stuck wait RPC: %q %d %s", stderr, code, time.Since(start))
	}
	stallHello.Store(true) // not even hello is answered
	start = time.Now()
	_, stderr, code = agm(t, "alice-1", "wait", "-json", "-timeout", "300ms", "-reply-to", "q")
	if code != 1 || jsonError(t, stderr) != codeTimeout || time.Since(start) > 3*time.Second {
		t.Fatalf("stuck hello: %q %d %s", stderr, code, time.Since(start))
	}
	if !rpcDeadline.IsZero() {
		if err := run("alice-1", "version", nil); err != nil || !rpcDeadline.IsZero() {
			t.Fatal("wait deadline leaks into the next command")
		}
	}
	for _, args := range [][]string{{"wait", "-json", "-timeout", "0s", "-reply-to", "q"}, {"ask", "-json", "-timeout", "-1s", "bob", "hi"}} {
		if _, stderr, code := agm(t, "alice-1", args...); code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%v: %q", args, stderr)
		}
	}
}

func TestHistoryFiltersAndAck(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "carol-1", Name: "carol"}, nil, false, false)
	q, _ := b.Send("alice-1", broker.SendReq{To: "bob", Text: "q"}, nil)
	r, _ := b.Send("bob-1", broker.SendReq{ReplyTo: q.ID, Text: "r"}, nil)
	c, _ := b.Send("carol-1", broker.SendReq{To: "alice", Text: "from carol"}, nil)

	out, _, code := agm(t, "alice-1", "history", "-json", "-with", "bob", "-thread", r.ID)
	var h []broker.Summary
	if code != 0 || json.Unmarshal([]byte(out), &h) != nil || len(h) != 2 || h[0].ID != q.ID || h[1].ID != r.ID {
		t.Fatalf("history filters: %q", out)
	}
	if out, _, _ := agm(t, "alice-1", "history", "-with", "carol"); !strings.Contains(out, "from carol") || strings.Contains(out, "q\n") {
		t.Fatalf("history -with plain: %q", out)
	}
	if _, stderr, _ := agm(t, "carol-1", "history", "-json", "-thread", q.ID); jsonError(t, stderr) != broker.CodeUnknownMsg {
		t.Fatalf("foreign thread: %q", stderr)
	}

	// ack: full ids only, deduplicated, own mailbox only, idempotent.
	out, _, code = agm(t, "alice-1", "ack", "-json", r.ID, r.ID, q.ID)
	var res ackResult
	if code != 0 || json.Unmarshal([]byte(out), &res) != nil || len(res.Acked) != 1 || res.Acked[0] != r.ID || len(res.NotQueued) != 1 || res.NotQueued[0] != q.ID {
		t.Fatalf("ack -json: %q", out)
	}
	if in, _ := b.Inbox("alice-1"); len(in) != 1 || in[0].ID != c.ID {
		t.Fatalf("ack removed the wrong mail: %v", in)
	}
	out, stderr, code := agm(t, "alice-1", "ack", r.ID)
	if code != 0 || out != "" || stderr != "not queued: "+r.ID+"\n" {
		t.Fatalf("ack again: %q %q %d", out, stderr, code)
	}
	for _, args := range [][]string{{"ack", "-json"}, {"ack", "-json", r.ID[:8]}, {"ack", "-json", strings.ToUpper(r.ID)}} {
		if out, stderr, code := agm(t, "alice-1", args...); out != "" || code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%v: %q %q", args, out, stderr)
		}
	}
	if in, _ := b.Inbox("bob-1"); len(in) != 0 { // bob's own queue (the question) untouched by alice
		if _, stderr, _ := agm(t, "alice-1", "ack", in[0].ID); !strings.Contains(stderr, "not queued") {
			t.Fatalf("acked someone else's mail: %q", stderr)
		}
		if in2, _ := b.Inbox("bob-1"); len(in2) != len(in) {
			t.Fatal("bob's mailbox changed")
		}
	}
}

// oldDaemon serves the pre-protocol-2 wire: known ops answer, others are "unknown op".
// It records every op it receives.
func oldDaemon(t *testing.T) (take func() []string) {
	t.Helper()
	dir, _ := os.MkdirTemp("/tmp", "agm")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s.sock")
	t.Setenv("AGM_SOCKET", sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var mu sync.Mutex
	var ops []string
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				helloed := false
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					var req broker.Request
					json.Unmarshal(sc.Bytes(), &req)
					mu.Lock()
					ops = append(ops, req.Op)
					mu.Unlock()
					res := map[string]string{
						"hello": `{"id":"alice-1"}`, "list": `[]`, "inbox": `[]`, "ack": `null`,
						"send": `{"id":"0123456789abcdef","from":"alice-1","to":"bob-1","text":"hi"}`,
					}[req.Op]
					if res == "" && !helloed {
						fmt.Fprintf(c, `{"id":%d,"error":{"code":"not_registered","message":"say hello first"}}`+"\n", req.ID)
						continue
					}
					helloed = helloed || req.Op == "hello"
					if res == "" {
						fmt.Fprintf(c, `{"id":%d,"error":{"code":"bad_request","message":"unknown op"}}`+"\n", req.ID)
					} else {
						fmt.Fprintf(c, `{"id":%d,"result":%s}`+"\n", req.ID, res)
					}
				}
			}()
		}
	}()
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		o := ops
		ops = nil
		return o
	}
}

func TestOldDaemonRefused(t *testing.T) {
	takeOps := oldDaemon(t)
	dir := t.TempDir()
	ref := filepath.Join(dir, "r.md")
	os.WriteFile(ref, []byte("x"), 0o600)
	exe, _ := os.Executable()
	for _, args := range [][]string{
		{"send", "-json", "-ref", ref, "bob", "see"},
		{"ask", "-json", "-no-wait", "bob", "q?"},
		{"wait", "-json", "-reply-to", "0123456789abcdef"},
		{"history", "-json", "-with", "bob"},
		{"history", "-json"},
		{"show", "-json", "0123456789abcdef"},
		{"ack", "-json", "0123456789abcdef"},
		{"resolve", "-json", "bob"},
	} {
		takeOps()
		out, stderr, code := agm(t, "alice-1", args...)
		var e struct {
			Error struct{ Code, Message string }
		}
		json.Unmarshal([]byte(stderr), &e)
		if out != "" || code != 1 || e.Error.Code != codeOutdated || !strings.Contains(e.Error.Message, shellQuote(exe)+" restart") {
			t.Fatalf("%v: %q %q", args, out, stderr)
		}
		for _, op := range takeOps() {
			if op != "hello" && op != "protocol" && op != "list" {
				t.Fatalf("%v sent %q to an old daemon", args, op)
			}
		}
	}
	// Legacy commands keep working.
	for _, args := range [][]string{{"send", "bob", "hi"}, {"inbox", "-ack"}, {"list"}} {
		if out, stderr, code := agm(t, "alice-1", args...); code != 0 {
			t.Fatalf("%v: %q %q", args, out, stderr)
		}
	}
}

func TestTrailingFlagGuard(t *testing.T) {
	b, dir := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)
	os.WriteFile(filepath.Join(dir, "bob"), []byte("x"), 0o600)
	t.Chdir(dir)
	queued := func() int {
		in, _ := b.Inbox("bob-1")
		return len(in)
	}

	// A word after the target matching one of the command's flags is a usage
	// error (exit 2) and queues nothing.
	for _, args := range [][]string{
		{"send", "bob", "hi", "-json"},
		{"send", "bob", "-json"},
		{"send", "bob", "hi", "-ref"},
		{"ask", "bob", "Ready to merge?", "-no-wait"},
		{"ask", "-timeout", "2s", "bob", "Ready?", "-no-wait"},
		{"reply", "0123456789abcdef", "-json"},
	} {
		out, stderr, code := agm(t, "alice-1", args...)
		if out != "" || code != 2 || !strings.Contains(stderr, "is a flag") || !strings.Contains(stderr, "--") {
			t.Fatalf("%v: %q %q %d", args, out, stderr, code)
		}
	}
	if queued() != 0 {
		t.Fatal("guarded commands queued mail")
	}
	// JSON mode reports the same failure as a usage error object.
	if _, stderr, code := agm(t, "alice-1", "send", "-json", "bob", "-json"); code != 2 || jsonError(t, stderr) != codeUsage {
		t.Fatalf("json guard: %q", stderr)
	}

	// `--` before the target escapes back to text.
	if out, stderr, code := agm(t, "alice-1", "send", "--", "bob", "-json"); code != 0 {
		t.Fatalf("escape: %q %q %d", out, stderr, code)
	}
	in, _ := b.Inbox("bob-1")
	if len(in) != 1 || in[0].Text != "-json" {
		t.Fatalf("escape text: %+v", in)
	}
	b.Ack("bob-1", []string{in[0].ID})

	// `--` before the target escapes even when a flag value equals the target.
	if _, stderr, code := agm(t, "alice-1", "send", "-ref", "bob", "--", "bob", "hi", "-json"); code != 0 {
		t.Fatalf("ref-value escape: %q %d", stderr, code)
	}
	in, _ = b.Inbox("bob-1")
	if len(in) != 1 || in[0].Text != "hi -json" || len(in[0].Attachments) != 1 {
		t.Fatalf("ref-value escape text: %+v", in)
	}
	b.Ack("bob-1", []string{in[0].ID})

	// Words that are not this command's flags stay text.
	for _, args := range [][]string{
		{"send", "bob", "-v"},
		{"send", "bob", "-"},
		{"send", "bob", "-x=1"},
		{"send", "bob", "hi", "--"},
	} {
		if _, stderr, code := agm(t, "alice-1", args...); code != 0 {
			t.Fatalf("%v: %q %d", args, stderr, code)
		}
	}
	in, _ = b.Inbox("bob-1")
	var texts []string
	for _, m := range in {
		texts = append(texts, m.Text)
	}
	for _, want := range []string{"-v", "-", "-x=1", "hi --"} {
		if !slices.Contains(texts, want) {
			t.Fatalf("dash text missing %q: %q", want, texts)
		}
	}

	// The *-file verbs reject extra positionals with the same hint.
	if _, stderr, code := agm(t, "alice-1", "send-file", "bob"); code != 2 || !strings.Contains(stderr, "flags go before") {
		t.Fatalf("file arity: %q %d", stderr, code)
	}
}

func TestReplyCorrelation(t *testing.T) {
	b, _ := testBroker(t)
	b.Hello(broker.SessionInfo{ID: "alice-1", Name: "alice"}, nil, false, false)
	b.Hello(broker.SessionInfo{ID: "bob-1", Name: "bob"}, nil, false, false)

	out, _, code := agm(t, "alice-1", "ask", "-no-wait", "bob", "q?")
	q := strings.TrimSpace(out)
	if code != 0 {
		t.Fatalf("ask: %q", out)
	}
	r, _ := b.Send("bob-1", broker.SendReq{ReplyTo: q, Text: "a"}, nil)

	// Inbox and hook delivery head REPLY headers with the question id.
	out, _, code = agm(t, "alice-1", "inbox")
	if code != 0 || !strings.Contains(out, "[REPLY "+r.ID+" re "+q+"]") || !strings.Contains(out, "[agent-mesh] 1 message(s) from other agents. "+integrations.Frame()) {
		t.Fatalf("inbox: %q", out)
	}
	in, _ := b.Inbox("alice-1")
	if mail := formatMail(in); !strings.Contains(mail, "[REPLY "+r.ID+" re "+q+"]") {
		t.Fatalf("hook mail: %s", mail)
	}
	// Plain messages keep the short header.
	fyi, _ := b.Send("bob-1", broker.SendReq{To: "alice", Text: "fyi"}, nil)
	in, _ = b.Inbox("alice-1")
	if mail := formatMail(in); !strings.Contains(mail, "[MSG "+fyi.ID+"]") || strings.Contains(mail, "[MSG "+fyi.ID+" re ") {
		t.Fatalf("msg header: %s", mail)
	}
}

// TestTermSafe: OSC 52 (clipboard write), CSI, C1 CSI, NUL, DEL and bidi controls all
// become U+FFFD; \n, \t and ordinary Unicode (emoji included) are kept.
func TestTermSafe(t *testing.T) {
	in := "a\x00b\x1b]52;c;AAA\x07c\x1b[2Jd\u009b31me\x7ff\u202eg\u2066h\td\n😀ğ"
	out := termSafe(in)
	for _, bad := range []string{"\x00", "\x1b", "\x07", "\u009b", "\x7f", "\u202e", "\u2066"} {
		if strings.Contains(out, bad) {
			t.Errorf("still contains %q: %q", bad, out)
		}
	}
	for _, good := range []string{"\n", "\t", "😀", "ğ", "[2J", "]52;c;AAA"} {
		if !strings.Contains(out, good) {
			t.Errorf("lost %q: %q", good, out)
		}
	}
	if c := strings.Count(out, "\uFFFD"); c != 8 {
		t.Errorf("%d replacement markers, want 8: %q", c, out)
	}
}

// TestPrintInboxTerminalSafe: on a terminal, no control rune of peer text reaches the
// output; on a pipe (what agents read), the output keeps the peer bytes verbatim.
func TestPrintInboxTerminalSafe(t *testing.T) {
	msgs := []*broker.Message{{
		ID: "0123456789abcdef", From: "peer-1", FromName: "bob\x1b]52;c;AAA\x07", To: "me-1",
		Text:        "hi\x1b[2J\u202ethere",
		Attachments: []broker.Attachment{{Type: "snip\x1b", Name: "n\u009b", Content: "c\x00"}, {Type: "ref", Name: "f", Path: "/tmp/p\x1b[A"}},
		At:          time.Now(),
	}}
	old := stdoutTerm
	defer func() { stdoutTerm = old }()

	stdoutTerm = true
	var term strings.Builder
	printInbox(&term, io.Discard, msgs)
	if strings.IndexFunc(term.String(), func(r rune) bool {
		return (r < 0x20 && r != '\n' && r != '\t') || r == 0x7f || (r >= 0x80 && r <= 0x9f) || (r >= 0x202a && r <= 0x202e)
	}) >= 0 {
		t.Fatalf("terminal output still has control runes: %q", term.String())
	}

	stdoutTerm = false
	var pipe strings.Builder
	printInbox(&pipe, io.Discard, msgs)
	for _, want := range []string{"bob\x1b]52;c;AAA\x07", "hi\x1b[2J\u202ethere", "--- snip\x1b: n\u009b ---", "c\x00", "/tmp/p\x1b[A"} {
		if !strings.Contains(pipe.String(), want) {
			t.Fatalf("pipe output lost peer bytes %q: %q", want, pipe.String())
		}
	}
}
