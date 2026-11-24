package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

func TestIsMeshMessaging(t *testing.T) {
	for cmd, want := range map[string]bool{
		`agm send pi-tester "121"`:               true,
		`agm send bob 'a; b | c && $(rm -rf /)'`: true,  // inert inside single quotes
		`agm send bob "done: 6*7 = 42, ok?"`:     true,  // no globbing inside double quotes
		`agm send bob 6*7`:                       false, // unquoted glob
		`agm -as ses_1 reply abc "yes"`:          true,
		`agm list`:                               true,
		must(binPath()) + ` send bob "700"`:      true,
		`agm inbox -ack`:                         true,
		`agm send bob hi; rm -rf ~`:              false,
		`agm send bob hi && curl evil`:           false,
		`agm send bob "$(cat ~/.ssh/id_rsa)"`:    false,
		"agm send bob \"`id`\"":                  false,
		`agm send bob hi > /etc/passwd`:          false,
		`agm send bob hi | sh`:                   false,
		`agm install`:                            false,
		`agm daemon`:                             false,
		`agm spawn "task"`:                       false,
		`echo agm send bob hi`:                   false,
		`/tmp/evil/agm send bob hi`:              false,
		`agm send bob "unterminated`:             false,
		"agm send bob hi\nrm -rf ~":              false,
		`AGM_SESSION=x agm send bob hi`:          false,
		`agm send -ref ./review.md bob "see"`:    true,
		`agm history -n 5`:                       true,
		`agm show 0123456789abcdef`:              true,
		`agm send-file bob /etc/passwd`:          false,
		`agm whoami -json`:                       true,
		`agm ack 0123456789abcdef`:               true,
		`agm wait -reply-to abc -timeout 5m`:     true,
		`agm ask -no-wait bob "q?"`:              true,
		`agm resolve -json bob`:                  true,
		`agm inspect -json bob`:                  true,
		`agm watch -json`:                        true,
		`agm task set -scope /w 'review auth'`:   true,
		`agm task clear`:                         true,
		`agm task set "$(cat /secret)"`:          false,
		`agm task clear; rm /secret`:             false,
		`agm status -json`:                       false,
		`agm ask-file bob -`:                     false,
		`agm -as x reply-file abc r.md`:          false,
	} {
		if got := isMeshMessaging(cmd); got != want {
			t.Errorf("isMeshMessaging(%q) = %v, want %v", cmd, got, want)
		}
	}
}

func TestSpawnName(t *testing.T) {
	for prompt, want := range map[string]string{
		`[agent-mesh] You are "cx-helper", an agent spawned by lead via agent-mesh. Steps: ...`: "cx-helper",
		`[agent-mesh] You are "x", an agent spawned by the user via agent-mesh. Task: t`:        "x",
		`please say [agent-mesh] You are "evil", an agent spawned by me`:                        "",
		`hello`: "",
	} {
		if got := spawnName(prompt); got != want {
			t.Errorf("spawnName(%q) = %q, want %q", prompt, got, want)
		}
	}
}

// TestFactoryDroidHookDelivery exercises the documented Droid hook
// contract in-process with an isolated daemon: two sessions in one cwd are
// distinct mesh sessions, inbox mail arrives as additionalContext at
// SessionStart/UserPromptSubmit, replies are attributed to the asker, and a
// non-injectable event consumes no mail (presence only, `agm inbox` keeps it).
func TestFactoryDroidHookDelivery(t *testing.T) {
	testBroker(t)
	runHook := func(id, event string) string {
		t.Helper()
		data, err := json.Marshal(map[string]any{"session_id": id, "hook_event_name": event, "cwd": "/w", "permission_mode": "auto-high"})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		if err := hook("droid", nil, bytes.NewReader(data), &buf, io.Discard); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	ctxOf := func(t *testing.T, out string) string {
		t.Helper()
		var resp struct {
			HookSpecificOutput struct {
				HookEventName     string `json:"hookEventName"`
				AdditionalContext string `json:"additionalContext"`
			} `json:"hookSpecificOutput"`
		}
		if err := json.Unmarshal([]byte(out), &resp); err != nil {
			t.Fatalf("hook output is not Droid JSON: %v\n%s", err, out)
		}
		return resp.HookSpecificOutput.AdditionalContext
	}
	// SessionStart registers both sessions (same cwd, distinct ids).
	introA := ctxOf(t, runHook("droid-A", "SessionStart"))
	introB := ctxOf(t, runHook("droid-B", "SessionStart"))
	for _, s := range []struct{ id, intro string }{{"droid-A", introA}, {"droid-B", introB}} {
		if !strings.Contains(s.intro, "You are on agent-mesh") || !strings.Contains(s.intro, s.id) {
			t.Fatalf("SessionStart intro for %s: %q", s.id, s.intro)
		}
	}
	if out, _, code := agm(t, "", "list"); code != 0 || !strings.Contains(out, "droid-A") || !strings.Contains(out, "droid-B") {
		t.Fatalf("list after hooks: %q %d", out, code)
	}

	// Addressed inbound: alice asks only droid-A, and it lands as context.
	q, _, code := agm(t, "alice", "ask", "-no-wait", "droid-A", "how are you?")
	if code != 0 {
		t.Fatalf("ask: %d", code)
	}
	q = strings.TrimSpace(q)
	ctx := ctxOf(t, runHook("droid-A", "UserPromptSubmit"))
	for _, want := range []string{"[ASK " + q, "how are you?", integrations.Frame()} {
		if !strings.Contains(ctx, want) {
			t.Fatalf("UserPromptSubmit context missing %q: %q", want, ctx)
		}
	}

	// droid-B never saw droid-A's mail.
	if outB := runHook("droid-B", "UserPromptSubmit"); strings.Contains(outB, "how are you?") {
		t.Fatalf("mail leaked to the wrong session: %q", outB)
	}

	// Attributed reply: droid-A replies; alice receives the reply to her ask.
	if _, _, code := agm(t, "droid-A", "reply", q, "fine"); code != 0 {
		t.Fatalf("reply: %d", code)
	}
	if out, _, code := agm(t, "alice", "wait", "-timeout", "5s", "-reply-to", q); code != 0 || strings.TrimSpace(out) != "fine" {
		t.Fatalf("attributed reply: %q %d", out, code)
	}

	// Non-injectable event takes no mail: Notification registers presence only.
	if _, _, code := agm(t, "alice", "send", "droid-B", "needle"); code != 0 {
		t.Fatalf("send: %d", code)
	}
	if out := runHook("droid-B", "Notification"); out != "" {
		t.Fatalf("Notification produced context: %q", out)
	}
	if out, _, code := agm(t, "droid-B", "inbox"); code != 0 || !strings.Contains(out, "needle") {
		t.Fatalf("mail consumed by Notification: %q %d", out, code)
	}
}
func must(p string, _ bool) string { return p }

// TestGrokHook runs the Grok Build hook contract (docs.x.ai/build/features/hooks)
// against an isolated broker: hooks leave mail queued and replies use the calling session.
func TestGrokHook(t *testing.T) {
	br, dir := testBroker(t)
	sock := filepath.Join(dir, "s.sock")
	cwd := t.TempDir()
	// fire runs the hook the way Grok Build does: a JSON event on stdin and the same
	// values in the environment. An empty command makes an event without a tool call.
	fire := func(event, session, command string) string {
		for k, v := range map[string]string{"GROK_HOOK_EVENT": event, "GROK_SESSION_ID": session, "GROK_WORKSPACE_ROOT": cwd} {
			t.Setenv(k, v)
		}
		payload := map[string]any{"hookEventName": event, "sessionId": session, "cwd": cwd, "workspaceRoot": cwd}
		if command != "" {
			payload["toolName"], payload["toolInput"] = "Bash", map[string]string{"command": command}
		}
		var in, out strings.Builder
		if err := json.NewEncoder(&in).Encode(payload); err != nil {
			t.Fatal(err)
		}
		if err := hook("grok", nil, strings.NewReader(in.String()), &out, &out); err != nil {
			t.Fatalf("%s %s: %v", event, session, err)
		}
		return out.String()
	}
	byID := func(id string) *broker.SessionInfo {
		return findSession(listOn(t, sock), func(s broker.SessionInfo) bool { return s.ID == id })
	}
	queued := func(id string) (ids []string) {
		in, err := br.Inbox(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range in {
			ids = append(ids, m.ID)
		}
		return ids
	}

	// Two sessions in one directory are two sessions.
	for _, id := range []string{"sess-a", "sess-b"} {
		if out := fire("SessionStart", id, ""); out != "" {
			t.Fatalf("SessionStart %s printed %q: Grok ignores hook output", id, out)
		}
		if s := byID(id); s == nil || s.Harness != "grok" || s.Cwd != cwd {
			t.Fatalf("session %s = %+v, want harness grok in %s", id, s, cwd)
		}
	}
	peer := dialSession(t, sock, "peer", "peer", "pi", false)
	var q, note broker.Message
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "sess-a", Text: "what is 6*7?", ExpectsReply: true}}, &q); err != nil {
		t.Fatal(err)
	}
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "sess-a", Text: "note for a"}}, &note); err != nil {
		t.Fatal(err)
	}
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "sess-b", Text: "for b"}}, new(broker.Message)); err != nil {
		t.Fatal(err)
	}

	// Nothing is printed and nothing is taken: a tool call goes ahead whatever is queued.
	for _, ev := range [][3]string{{"UserPromptSubmit", "sess-a", ""}, {"PreToolUse", "sess-a", "make test"}, {"PreToolUse", "sess-b", "rm -rf build"}} {
		if out := fire(ev[0], ev[1], ev[2]); out != "" {
			t.Fatalf("%s %s %q printed %q: the call must not be answered and the mail stays queued", ev[0], ev[1], ev[2], out)
		}
	}
	a, b := queued("sess-a"), queued("sess-b")
	if len(a) != 2 || len(b) != 1 {
		t.Fatalf("queued after the hooks: sess-a %v, sess-b %v", a, b)
	}

	// The agent reads and answers from its own session, without -as.
	t.Setenv("GROK_SESSION_ID", "sess-a") // a shell command of that session
	in, _, code := agm(t, "", "inbox")
	if code != 0 || !strings.Contains(in, "what is 6*7?") || !strings.Contains(in, "note for a") || strings.Contains(in, "for b") {
		t.Fatalf("inbox = %q (%d): want the mail of sess-a and nothing of sess-b", in, code)
	}
	if a = queued("sess-a"); !slices.Contains(a, q.ID) || !slices.Contains(a, note.ID) {
		t.Fatalf("inbox removed mail: queued %v; only an explicit ack does", a)
	}
	if who, _, code := agm(t, "", "whoami"); code != 0 || !strings.Contains(who, "sess-a") {
		t.Fatalf("whoami = %q (%d)", who, code)
	}
	if _, errOut, code := agm(t, "", "reply", q.ID, "42"); code != 0 {
		t.Fatalf("reply: %s (%d)", errOut, code)
	}
	if m := peer.waitMsg(func(m *broker.Message) bool { return m.ReplyTo == q.ID }, "the reply"); m.From != "sess-a" || m.Text != "42" {
		t.Fatalf("the reply is not attributed to sess-a: %+v", m)
	}
	if _, _, code := agm(t, "", "ack", note.ID); code != 0 || slices.Contains(queued("sess-a"), note.ID) || len(queued("sess-b")) != 1 {
		t.Fatalf("ack: code %d, queued sess-a %v, sess-b %v; want the note gone and sess-b untouched", code, queued("sess-a"), queued("sess-b"))
	}

	// SessionEnd ends that session only.
	if slices.Contains(queued("sess-a"), q.ID) {
		if _, _, code := agm(t, "", "ack", q.ID); code != 0 {
			t.Fatalf("ack %s: code %d", q.ID, code)
		}
	}
	if out := fire("SessionEnd", "sess-a", ""); out != "" || byID("sess-a") != nil {
		t.Fatalf("SessionEnd: out %q, session %+v", out, byID("sess-a"))
	}
	if byID("sess-b") == nil || len(queued("sess-b")) != 1 {
		t.Fatalf("SessionEnd of sess-a touched sess-b: %+v, queued %v", byID("sess-b"), queued("sess-b"))
	}

	// Grok also passes its values in the environment only.
	t.Setenv("GROK_HOOK_EVENT", "SessionStart")
	t.Setenv("GROK_SESSION_ID", "sess-c")
	var out strings.Builder
	if err := hook("grok", nil, strings.NewReader(""), &out, &out); err != nil || out.String() != "" || byID("sess-c") == nil {
		t.Fatalf("environment-only SessionStart: out %q, err %v, session %+v", out.String(), err, byID("sess-c"))
	}
}

// Grok 1.0.92 spells the event session_end on stdin and in the environment and adds
// Claude Code's field names; the fixtures spell it SessionEnd. All three must end the
// session, and mail that is still queued must survive the bye.
func TestGrokSessionEndSpellings(t *testing.T) {
	br, dir := testBroker(t)
	sock := filepath.Join(dir, "s.sock")
	t.Setenv("GROK_WORKSPACE_ROOT", "/repo")
	byID := func(id string) *broker.SessionInfo {
		return findSession(listOn(t, sock), func(s broker.SessionInfo) bool { return s.ID == id })
	}
	queued := func(id string) int {
		in, err := br.Inbox(id)
		if err != nil {
			t.Fatal(err)
		}
		return len(in)
	}
	// fire is one call of the hook; its output must stay empty, whatever the event.
	fire := func(event, stdin string) string {
		t.Setenv("GROK_HOOK_EVENT", event)
		var out strings.Builder
		if err := hook("grok", nil, strings.NewReader(stdin), &out, &out); err != nil || out.String() != "" {
			t.Fatalf("%s: out %q, err %v", event, out.String(), err)
		}
		return out.String()
	}
	// The payload as Grok 1.0.92 hands it over when a headless run ends.
	const real = `{"hookEventName":"session_end","sessionId":"%s","cwd":"/repo","workspaceRoot":"/repo","timestamp":"2026-11-24T20:55:00.627315932+00:00","transcriptPath":"/t/updates.jsonl","permissionMode":"bypassPermissions","reason":"shutdown","hook_event_name":"SessionEnd","session_id":"%s","transcript_path":"/t/updates.jsonl","permission_mode":"bypassPermissions"}`
	for _, tc := range []struct{ name, id, stdin, event string }{
		{"grok 1.0.92", "a", fmt.Sprintf(real, "a", "a"), "session_end"},
		{"as documented", "b", `{"hookEventName":"SessionEnd","sessionId":"b","cwd":"/repo","workspaceRoot":"/repo"}`, "SessionEnd"},
		{"environment only", "c", "", "session_end"},
	} {
		t.Setenv("GROK_SESSION_ID", tc.id)
		fire("session_start", "")
		if byID(tc.id) == nil {
			t.Fatalf("%s: session %s did not register", tc.name, tc.id)
		}
		fire(tc.event, tc.stdin)
		if byID(tc.id) != nil {
			t.Errorf("%s: session %s is still registered after session_end", tc.name, tc.id)
		}
	}

	// Mail that is still queued survives: the session stays as its holder.
	t.Setenv("GROK_SESSION_ID", "d")
	fire("session_start", "")
	peer := dialSession(t, sock, "peer", "peer", "pi", false)
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "d", Text: "for d"}}, new(broker.Message)); err != nil {
		t.Fatal(err)
	}
	fire("session_end", fmt.Sprintf(real, "d", "d"))
	if s := byID("d"); s == nil || s.PID != 0 || queued("d") != 1 {
		t.Fatalf("after session_end with mail: session %+v, queued %d", s, queued("d"))
	}
}

// Grok also runs the hooks Claude Code registers, and marks the processes it spawns as
// hooks with GROK_HOOK_EVENT and GROK_HOOK_NAME. Their payload carries Claude Code's field
// names as well, so that marker is what keeps the Claude adapter out of a Grok session:
// no new registration, no take, no Stop waiter.
func TestClaudeHookSkipsHooksDispatchedByGrok(t *testing.T) {
	br, dir := testBroker(t)
	sock := filepath.Join(dir, "s.sock")
	t.Setenv("GROK_SESSION_ID", "g-1")
	t.Setenv("GROK_WORKSPACE_ROOT", "/repo")
	if err := br.Hello(broker.SessionInfo{ID: "g-1", Name: "grok session", Harness: "grok"}, nil, false, false); err != nil {
		t.Fatal(err)
	}
	peer := dialSession(t, sock, "peer", "peer", "pi", false)
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "g-1", Text: "for the grok session"}}, new(broker.Message)); err != nil {
		t.Fatal(err)
	}
	byID := func(id string) *broker.SessionInfo {
		return findSession(listOn(t, sock), func(s broker.SessionInfo) bool { return s.ID == id })
	}
	// serve runs the Claude adapter on a payload and returns what it printed.
	serve := func(args []string, payload map[string]any) string {
		t.Helper()
		b, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		done := make(chan error, 1)
		go func() { done <- hook("claude", args, bytes.NewReader(b), &out, &out) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%v %v: %v", args, payload["hook_event_name"], err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%v %v: the Claude adapter waited, as it does for Claude Code", args, payload["hook_event_name"])
		}
		return out.String()
	}
	// The payload as Grok 1.0.92 hands it to a Claude Code hook, aliases included.
	for _, tc := range []struct{ event, alias string }{
		{"session_start", "SessionStart"}, {"user_prompt_submit", "UserPromptSubmit"}, {"pre_tool_use", "PreToolUse"},
		{"post_tool_use", "PostToolUse"}, {"stop", "Stop"},
	} {
		for _, args := range [][]string{nil, {"--wait"}} {
			t.Setenv("GROK_HOOK_EVENT", tc.event)
			t.Setenv("GROK_HOOK_NAME", "global/settings:"+tc.event+"[0].hooks[0]")
			payload := map[string]any{"hookEventName": tc.event, "sessionId": "g-1", "cwd": "/repo", "workspaceRoot": "/repo",
				"hook_event_name": tc.alias, "session_id": "g-1", "stop_hook_active": false}
			if tc.alias == "PostToolUse" || tc.alias == "PreToolUse" {
				payload["tool_name"], payload["tool_input"] = "Bash", map[string]string{"command": "make"}
			}
			if out := serve(args, payload); out != "" {
				t.Errorf("%s %v: printed %q", tc.event, args, out)
			}
		}
	}
	if in, _ := br.Inbox("g-1"); len(in) != 1 || in[0].Text != "for the grok session" {
		t.Fatalf("the mail of g-1 was taken: %v", in)
	}
	if s := byID("g-1"); s == nil || s.Name != "grok session" || s.Harness != "grok" {
		t.Fatalf("g-1 was registered again: %+v", s)
	}

	// A Claude Code session started from a Grok tool command inherits GROK_SESSION_ID, and
	// none of the hook markers. It is served as usual.
	t.Setenv("GROK_HOOK_EVENT", "")
	t.Setenv("GROK_HOOK_NAME", "")
	t.Setenv("GROK_SESSION_ID", "c-1")
	if out := serve(nil, map[string]any{"session_id": "c-1", "hook_event_name": "SessionStart", "cwd": "/repo"}); !strings.Contains(out, "as session c-1") {
		t.Fatalf("SessionStart of a Claude Code session printed %q", out)
	}
	if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "c-1", Text: "for claude"}}, new(broker.Message)); err != nil {
		t.Fatal(err)
	}
	out := serve(nil, map[string]any{"session_id": "c-1", "hook_event_name": "PostToolUse", "cwd": "/repo", "tool_name": "Bash", "tool_response": "ok"})
	if !strings.Contains(out, "for claude") {
		t.Fatalf("PostToolUse of a Claude Code session printed %q", out)
	}
	if in, _ := br.Inbox("c-1"); len(in) != 0 {
		t.Fatalf("the mail of c-1 was not taken: %v", in)
	}
	if s := byID("c-1"); s == nil || s.Harness != "claude" {
		t.Fatalf("the Claude Code session is not registered as claude: %+v", s)
	}
}

// The payloads are the ones Muse Code 1.4.25 handed to settings-level hooks.
func TestMuseHook(t *testing.T) {
	br, dir := testBroker(t)
	sock := filepath.Join(dir, "s.sock")
	byID := func(id string) *broker.SessionInfo {
		return findSession(listOn(t, sock), func(s broker.SessionInfo) bool { return s.ID == id })
	}
	queued := func() int {
		in, err := br.Inbox("m-1")
		if err != nil {
			t.Fatal(err)
		}
		return len(in)
	}
	fire := func(stdin string) string {
		var out strings.Builder
		if err := hook("muse", nil, strings.NewReader(stdin), &out, &out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	peer := dialSession(t, sock, "peer", "peer", "pi", false)
	send := func(text string) {
		t.Helper()
		if err := peer.call("send", broker.Request{SendReq: broker.SendReq{To: "m-1", Text: text}}, new(broker.Message)); err != nil {
			t.Fatal(err)
		}
	}
	const rest = `"cwd":"/repo","transcript_path":null,"model":"unknown","permission_mode":"default"`

	// Registration, and the intro as plain text: Muse 1.4.25 passes no hookSpecificOutput on.
	out := fire(`{"hook_event_name": "SessionStart", "source": "startup", "session_id": "m-1", ` + rest + `}`)
	if s := byID("m-1"); s == nil || s.Harness != "muse" || s.Cwd != "/repo" {
		t.Fatalf("session = %+v", s)
	}
	if strings.HasPrefix(out, "{") || !strings.Contains(out, "as session m-1") {
		t.Fatalf("SessionStart answered %q", out)
	}

	// Mail is spoken at the prompt, as plain text, and taken there.
	prompt := `{"hook_event_name": "UserPromptSubmit", "prompt": "hello", "session_id": "m-1", "turn_id": "t", ` + rest + `}`
	if out = fire(prompt); out != "" {
		t.Fatalf("UserPromptSubmit without mail printed %q", out)
	}
	send("first")
	send("second")
	if out = fire(prompt); strings.HasPrefix(out, "{") || !strings.Contains(out, "first") || !strings.Contains(out, "second") || strings.Contains(out, "as session") {
		t.Fatalf("UserPromptSubmit answered %q", out)
	}
	if queued() != 0 {
		t.Fatalf("still queued: %d", queued())
	}

	// From any other event nothing is spoken and no mail is taken. That covers a Stop handler
	// an earlier install left in the settings file: no block, and the mail waits for the next
	// prompt, which delivers it once.
	send("later")
	post := fire(`{"hook_event_name": "PostToolUse", "session_id": "m-1", "tool_name": "Bash", "tool_response": "ok", ` + rest + `}`)
	stop := `{"hook_event_name": "Stop", "last_assistant_message": "echo: hello", "session_id": "m-1", "turn_id": "t", "stop_hook_active": %s, ` + rest + `}`
	one := fire(fmt.Sprintf(stop, "false"))
	two := fire(fmt.Sprintf(stop, "true"))
	if post != "" || one != "" || two != "" || queued() != 1 {
		t.Fatalf("other events: out %q, %q, %q; queued %d", post, one, two, queued())
	}
	if out = fire(prompt); !strings.Contains(out, "later") || strings.Contains(out, "first") {
		t.Fatalf("the prompt after Stop answered %q", out)
	}
	if queued() != 0 {
		t.Fatalf("still queued: %d", queued())
	}
	if out = fire(prompt); out != "" {
		t.Fatalf("the next prompt printed %q", out)
	}
	// SessionEnd ends the session.
	if out = fire(`{"hook_event_name": "SessionEnd", "session_id": "m-1", "reason": "other", ` + rest + `}`); out != "" || byID("m-1") != nil {
		t.Fatalf("SessionEnd: out %q, session %+v", out, byID("m-1"))
	}
}
