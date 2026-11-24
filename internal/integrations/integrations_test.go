package integrations

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestInstallLifecycle(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("omp")
	if s := tg.Status(home, "/opt/agm"); s != "not installed" {
		t.Fatalf("status %q", s)
	}
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(home, tg.Path))
	body := string(data)
	if !strings.Contains(body, `"omp"`) || !strings.Contains(body, `"/opt/agm"`) || strings.Contains(body, "__MESH_") {
		t.Fatal("placeholders not rendered")
	}
	if s := tg.Status(home, "/opt/agm"); s != "current" {
		t.Fatalf("status %q, want current", s)
	}
	if s := tg.Status(home, "/moved/agm"); s != "outdated" {
		t.Fatalf("status %q, want outdated after binary moved", s)
	}
	if err := tg.Uninstall(home); err != nil || tg.Status(home, "/opt/agm") != "not installed" {
		t.Fatalf("uninstall: %v", err)
	}

	// A user file at our path is never overwritten or removed.
	os.WriteFile(filepath.Join(home, tg.Path), []byte("mine"), 0o644)
	if tg.Install(home, "/opt/agm") == nil || tg.Uninstall(home) == nil {
		t.Fatal("foreign file must be left alone")
	}
}

func TestOpencodeListEntry(t *testing.T) {
	home := t.TempDir()
	cfg := filepath.Join(home, ".config/opencode/cli.json")
	os.MkdirAll(filepath.Dir(cfg), 0o755)
	os.WriteFile(cfg, []byte(`{"plugins":["./herdr-opencode"],"theme":"x"}`), 0o644)
	tg, _ := Find("opencode")
	for range 2 { // idempotent
		if err := tg.Install(home, "/opt/agm"); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := os.ReadFile(cfg)
	if got := string(data); strings.Count(got, "./agent-mesh") != 1 || !strings.Contains(got, "./herdr-opencode") || !strings.Contains(got, `"theme": "x"`) {
		t.Fatalf("cli.json after install: %s", got)
	}
	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(cfg)
	if got := string(data); strings.Contains(got, "agent-mesh") || !strings.Contains(got, "./herdr-opencode") {
		t.Fatalf("cli.json after uninstall: %s", got)
	}
}

// TestOpencodeSymlinkedSharedConfigRefused: install and uninstall through a
// symlinked shared config refuse before any backup or write, leaving the
// link and its target untouched; an already-current link stays a no-op.
func TestOpencodeSymlinkedSharedConfigRefused(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("opencode")
	cfg := filepath.Join(home, ".config/opencode/cli.json")
	if err := os.MkdirAll(filepath.Dir(cfg), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "cli-target.json")
	orig := "{\"plugins\":[\"./herdr-opencode\"],\"theme\":\"x\"}"
	if err := os.WriteFile(target, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, cfg); err != nil {
		t.Fatal(err)
	}
	intact := func() {
		t.Helper()
		info, err := os.Lstat(cfg)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("shared config no longer a symlink: %v", err)
		}
		if data, err := os.ReadFile(target); string(data) != orig || err != nil {
			t.Fatalf("link target changed: %q %v", data, err)
		}
		if _, err := os.Lstat(cfg + ".bak"); !os.IsNotExist(err) {
			t.Fatal("backup written for a refused symlink")
		}
	}

	if err := tg.Install(home, "/opt/agm"); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("install through symlink: %v", err)
	}
	intact()
	if _, err := os.Lstat(filepath.Join(home, tg.Path)); !os.IsNotExist(err) {
		t.Fatal("adapter written before the symlink refusal")
	}

	// Uninstall, with mesh entries in the target, must refuse the same way.
	if err := os.Remove(cfg); err != nil {
		t.Fatal(err)
	}
	// State the uninstall path must refuse on: an installed config, now symlinked.
	plain := filepath.Join(t.TempDir(), "cli.json")
	if err := os.WriteFile(cfg, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	os.Remove(cfg + ".bak") // the refused uninstall below must not recreate it
	data, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	installed := string(data)
	adapter, err := os.ReadFile(filepath.Join(home, tg.Path))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(plain, cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plain, []byte(installed), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tg.Uninstall(home); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("uninstall through symlink: %v", err)
	}
	if now, err := os.ReadFile(filepath.Join(home, tg.Path)); err != nil || string(now) != string(adapter) {
		t.Fatalf("adapter removed before the symlink refusal: %q %v", now, err)
	}
	info, err := os.Lstat(cfg)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("shared config no longer a symlink: %v", err)
	}
	if data, err := os.ReadFile(plain); string(data) != installed || err != nil {
		t.Fatalf("link target changed by uninstall: %q %v", data, err)
	}
	if _, err := os.Lstat(cfg + ".bak"); !os.IsNotExist(err) {
		t.Fatal("backup written for a refused uninstall")
	}
}

// TestAgySymlinkedSecondEditPreflightsFirst: with only the second of two
// shared configs symlinked, install refuses before applying the first edit.
func TestAgySymlinkedSecondEditPreflightsFirst(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("agy")
	second := filepath.Join(home, ".gemini/antigravity-cli/settings.json")
	if err := os.MkdirAll(filepath.Dir(second), 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "agy-settings.json")
	if err := os.WriteFile(target, []byte("{\"permissions\":{\"allow\":[]}}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, second); err != nil {
		t.Fatal(err)
	}
	if err := tg.Install(home, "/opt/agm"); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("install through symlink: %v", err)
	}
	first := filepath.Join(home, ".gemini/config/hooks.json")
	if _, err := os.Lstat(first); !os.IsNotExist(err) {
		t.Fatal("first edit applied before the symlink refusal")
	}
	info, err := os.Lstat(second)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("second config no longer a symlink: %v", err)
	}
}

// TestFactoryLifecycle: install/status/uninstall on a disposable HOME keeps
// user keys (including hooksDisabled) and foreign hooks in hooks.json intact,
// and a foreign ManagedDir.. sees only our entries.
func TestFactoryLifecycle(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("droid")
	cfg := filepath.Join(home, ".factory/hooks.json")
	if err := os.MkdirAll(filepath.Dir(cfg), 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(cfg, []byte("{\"hooksDisabled\":false,\"PreToolUse\":[{\"matcher\":\"Read\",\"hooks\":[{\"type\":\"command\",\"command\":\"mine\"}]}]}"), 0o644)
	if s := tg.Status(home, "/opt/agm"); s != "not installed" {
		t.Fatalf("status %q", s)
	}
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	for _, want := range []string{"\"hooksDisabled\": false", "\"command\": \"mine\"", "hook droid"} {
		if !strings.Contains(body, want) {
			t.Fatalf("hooks.json after install missing %q: %s", want, body)
		}
	}
	for _, ev := range []string{"SessionStart", "UserPromptSubmit", "SessionEnd"} {
		if !strings.Contains(body, "\""+ev+"\"") {
			t.Fatalf("hooks.json missing event %s: %s", ev, body)
		}
	}
	if strings.Count(body, "hook droid") != 3 {
		t.Fatalf("want 3 droid hooks, got: %s", body)
	}
	if s := tg.Status(home, "/opt/agm"); s != "current" {
		t.Fatalf("status %q, want current", s)
	}
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(cfg)
	if strings.Count(string(data), "hook droid") != 3 {
		t.Fatalf("duplicate hooks after re-install: %s", data)
	}
	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(cfg)
	body = string(data)
	for _, gone := range []string{"SessionStart", "UserPromptSubmit", "SessionEnd"} {
		if strings.Contains(body, gone) || strings.Contains(body, "hook droid") {
			t.Fatalf("mesh entries left after uninstall: %s", body)
		}
	}
	for _, want := range []string{"\"hooksDisabled\": false", "\"command\": \"mine\"", "\"matcher\": \"Read\""} {
		if !strings.Contains(body, want) {
			t.Fatalf("user config damaged by uninstall, missing %q: %s", want, body)
		}
	}
	if s := tg.Status(home, "/opt/agm"); s != "not installed" {
		t.Fatalf("status after uninstall %q", s)
	}
}

// TestFactoryLeavesUnrelatedRootValuesAlone: unrelated root keys - including
// empty arrays, arrays of non-hook objects, null members and foreign events -
// are never rewritten by install or uninstall.
func TestFactoryLeavesUnrelatedRootValuesAlone(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("droid")
	cfg := filepath.Join(home, ".factory/hooks.json")
	if err := os.MkdirAll(filepath.Dir(cfg), 0o755); err != nil {
		t.Fatal(err)
	}
	orig := "{\"hooksDisabled\":true,\"EmptyStuff\":[],\"Other\":[{\"foo\":1},null],\"ForeignEvent\":[{\"matcher\":\"x\"}]}"
	if err := os.WriteFile(cfg, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	for _, keep := range []string{"\"hooksDisabled\": true", "\"EmptyStuff\": []", "\"foo\": 1", "null", "\"matcher\": \"x\""} {
		if !strings.Contains(body, keep) {
			t.Fatalf("unrelated root value rewritten, missing %q: %s", keep, body)
		}
	}
	if strings.Count(body, "hook droid") != 3 {
		t.Fatalf("want 3 droid hooks: %s", body)
	}
	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(cfg)
	body = string(data)
	for _, keep := range []string{"\"hooksDisabled\": true", "\"EmptyStuff\": []", "\"matcher\": \"x\""} {
		if !strings.Contains(body, keep) {
			t.Fatalf("unrelated root value damaged by uninstall, missing %q: %s", keep, body)
		}
	}
	// Original non-hook values survive verbatim in the parsed doc: the
	// unrelated entries are still empty arrays / objects / null after we are gone.
	if strings.Contains(body, "SessionStart") || strings.Contains(body, "UserPromptSubmit") || strings.Contains(body, "SessionEnd") {
		t.Fatalf("managed events left after uninstall: %s", body)
	}
}

// TestFactoryRejectsBadManagedShape: a managed event key holding an unsupported
// shape rejects install/uninstall with an error and leaves the file untouched
// (no partial write, no backup).
func TestFactoryRejectsBadManagedShape(t *testing.T) {
	for _, bad := range []string{
		`{"SessionStart":{}}`,
		`{"SessionStart":[null]}`,
		`{"SessionStart":[42]}`,
		`{"SessionStart":null}`,
		`{"SessionStart":[{"hooks":"foreign"}]}`,
	} {
		home := t.TempDir()
		tg, _ := Find("droid")
		cfg := filepath.Join(home, ".factory/hooks.json")
		if err := os.MkdirAll(filepath.Dir(cfg), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := tg.Install(home, "/opt/agm"); err == nil {
			t.Fatalf("install accepted bad config %s", bad)
		}
		if err := tg.Uninstall(home); err == nil {
			t.Fatalf("uninstall accepted bad config %s", bad)
		}
		data, err := os.ReadFile(cfg)
		if err != nil || string(data) != bad {
			t.Fatalf("rejected file changed: %q %v", data, err)
		}
		if _, err := os.Lstat(cfg + ".bak"); !os.IsNotExist(err) {
			t.Fatalf("backup written for rejected config %s", bad)
		}
	}
}

func TestClaudeHooksMerge(t *testing.T) {
	home := t.TempDir()
	cfg := filepath.Join(home, ".claude/settings.json")
	os.MkdirAll(filepath.Dir(cfg), 0o755)
	// Old installs added broad rules; the user owns Bash(agm spawn *).
	orig := `{"hooks":{"SessionStart":[{"matcher":"x","hooks":[{"type":"command","command":"bash herdr.sh"}]}]},"theme":"auto","permissions":{"allow":["Bash(ls:*)","Bash(agm *)","Bash(/old/agm:*)","Bash(agm spawn *)"]}}`
	os.WriteFile(cfg, []byte(orig), 0o644)
	tg, _ := Find("claude")

	for range 2 { // idempotent
		if err := tg.Install(home, "/old/agm"); err != nil {
			t.Fatal(err)
		}
	}
	if s := tg.Status(home, "/old/agm"); s != "current" {
		t.Fatalf("status %q", s)
	}
	if s := tg.Status(home, "/new/agm"); s != "outdated" {
		t.Fatalf("status after move %q", s)
	}
	tg.Install(home, "/new/agm") // binary moved: old entries replaced, not duplicated
	data, _ := os.ReadFile(cfg)
	got := string(data)
	for _, want := range []string{"bash herdr.sh", "Bash(ls:*)", `"/new/agm hook claude --wait"`, `"asyncRewake": true`, "Bash(agm send *)", "Bash(/new/agm list)", "Bash(agm show *)", "Bash(agm whoami)", "Bash(agm resolve *)", "Bash(agm inspect *)", "Bash(agm watch)", "Bash(agm task *)", "Bash(agm wait *)", "Bash(agm ack *)", "Bash(agm spawn *)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in\n%s", want, got)
		}
	}
	if strings.Contains(got, "/old/agm") || strings.Contains(got, `"Bash(agm *)"`) || strings.Contains(got, "file") || strings.Count(got, "/new/agm hook claude\"") != 5 {
		t.Fatalf("stale or duplicate entries:\n%s", got)
	}
	if strings.Index(got, `"hooks"`) > strings.Index(got, `"theme"`) || strings.Index(got, `"theme"`) > strings.Index(got, `"permissions"`) {
		t.Fatalf("top-level key order changed:\n%s", got)
	}

	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(cfg)
	got = string(data)
	if strings.Contains(got, "mesh") || strings.Contains(got, "agm send") || !strings.Contains(got, "bash herdr.sh") || !strings.Contains(got, "Bash(ls:*)") || !strings.Contains(got, "Bash(agm spawn *)") {
		t.Fatalf("uninstall must leave only foreign entries:\n%s", got)
	}
	if s := tg.Status(home, "/new/agm"); s != "not installed" {
		t.Fatalf("status %q", s)
	}
}

// TestGrokHooksFile: Grok loads every ~/.grok/hooks/*.json. Ours holds four events
// (PreToolUse included; no waiter, no allow rules), other files and config.toml stay
// as they are, a user hook in our file outlives install and uninstall, and uninstall
// removes the file once it holds nothing of ours.
func TestGrokHooksFile(t *testing.T) {
	home := t.TempDir()
	hooks := filepath.Join(home, ".grok", "hooks")
	file := filepath.Join(hooks, "agent-mesh.json")
	ok := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, content string) {
		ok(os.MkdirAll(filepath.Dir(p), 0o755))
		ok(os.WriteFile(p, []byte(content), 0o644))
	}
	read := func(p string) string {
		data, err := os.ReadFile(p)
		ok(err)
		return string(data)
	}
	const userHooks = `{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"fmt.sh"}]}]}}`
	const toml = "model = \"grok-4\"\n"
	write(filepath.Join(hooks, "fmt.json"), userHooks)
	write(filepath.Join(home, ".grok", "config.toml"), toml)
	write(file, userHooks) // a user may keep hooks in a file of this name too
	tg, _ := Find("grok")
	untouched := func() {
		t.Helper()
		if read(filepath.Join(hooks, "fmt.json")) != userHooks || read(filepath.Join(home, ".grok", "config.toml")) != toml {
			t.Error("a file that is not ours changed")
		}
	}

	ok(tg.Install(home, "/opt/agm"))
	got := read(file)
	if n := strings.Count(got, "/opt/agm hook grok"); n != 3 {
		t.Errorf("after install: %d mesh hook commands, want SessionStart, UserPromptSubmit and SessionEnd: %s", n, got)
	}
	for _, unwanted := range []string{"PreToolUse", "PostToolUse", "--wait", "permissions"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("after install: %s in %s", unwanted, got)
		}
	}
	if !strings.Contains(got, "fmt.sh") {
		t.Errorf("after install: the user's hook is gone: %s", got)
	}
	untouched()
	if s := tg.Status(home, "/opt/agm"); s != "current" {
		t.Errorf("after install: status %s", s)
	}
	ok(tg.Install(home, "/opt/agm"))
	if read(file) != got {
		t.Errorf("install is not idempotent: %s", read(file))
	}
	if s := tg.Status(home, "/opt/other/agm"); s != "outdated" {
		t.Errorf("after the binary moved: status %s, want outdated", s)
	}

	ok(tg.Uninstall(home))
	if after := read(file); !strings.Contains(after, "fmt.sh") || strings.Contains(after, "hook grok") {
		t.Errorf("after uninstall: %s", after)
	}
	untouched()
	if s := tg.Status(home, "/opt/agm"); s != "not installed" {
		t.Errorf("after uninstall: status %s", s)
	}
}

// A backup agm never wrote is not ours to delete or overwrite: a fresh install writes
// no .bak, and uninstall neither removes the one that is there nor replaces it with
// its own previous content.
func TestUninstallKeepsBackupItDidNotWrite(t *testing.T) {
	home := t.TempDir()
	file := filepath.Join(home, ".grok", "hooks", "agent-mesh.json")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file+".bak", []byte("user backup\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tg, _ := Find("grok")
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file + ".bak")
	if err != nil || string(data) != "user backup\n" {
		t.Errorf("agent-mesh.json.bak = %q, %v: want the user's file untouched", data, err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Errorf("after uninstall agent-mesh.json is still there (%v): it held nothing but mesh hooks", err)
	}
}
func TestCrushLine(t *testing.T) {
	home := t.TempDir()
	rc := filepath.Join(home, ".config/crush/crushrc")
	os.MkdirAll(filepath.Dir(rc), 0o755)
	os.WriteFile(rc, []byte("hook add PreToolUse --command \"x.py hook\" --name intercom\n"), 0o644)
	tg, _ := Find("crush")
	tg.Install(home, "/a/agm")
	tg.Install(home, "/b/agm")
	data, _ := os.ReadFile(rc)
	want := "hook add PreToolUse --command \"x.py hook\" --name intercom\nhook add PreToolUse --command \"/b/agm hook crush\" --name agm\n"
	if string(data) != want {
		t.Fatalf("got:\n%s", data)
	}
	tg.Uninstall(home)
	data, _ = os.ReadFile(rc)
	if string(data) != "hook add PreToolUse --command \"x.py hook\" --name intercom\n" {
		t.Fatalf("after uninstall:\n%s", data)
	}
}

func TestSkill(t *testing.T) {
	for _, name := range []string{"skill", "claude-skill"} {
		home := t.TempDir()
		tg, _ := Find(name)
		if err := tg.Install(home, "/opt/agm"); err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(filepath.Join(home, tg.Path))
		if !strings.HasPrefix(string(data), "---\nname: agent-mesh\n") || !strings.Contains(string(data), "/opt/agm ask") {
			t.Fatalf("%s: skill must start with frontmatter and have the bin path:\n%s", name, data)
		}
		if !ownedBy(data, name) || tg.Status(home, "/opt/agm") != "current" {
			t.Fatalf("%s: status %q", name, tg.Status(home, "/opt/agm"))
		}
		if err := tg.Uninstall(home); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(home, tg.Path)); !os.IsNotExist(err) {
			t.Fatalf("%s: not removed", name)
		}
	}
}

func TestCodexRules(t *testing.T) {
	home := t.TempDir()
	tg, _ := Find("codex")
	if err := tg.Install(home, "/opt/agm"); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(home, tg.Path))
	got := string(data)
	if !strings.HasPrefix(got, "# installed by agent-mesh") || !strings.Contains(got, `["/opt/agm", "agm"]`) || strings.Contains(got, "\n//") {
		t.Fatalf("rules file:\n%s", got)
	}
	if s := tg.Status(home, "/opt/agm"); s != "current" {
		t.Fatalf("status %q", s)
	}
	if err := tg.Uninstall(home); err != nil || tg.Status(home, "/opt/agm") != "not installed" {
		t.Fatalf("uninstall %v", err)
	}
}

func TestAgy(t *testing.T) {
	home := t.TempDir()
	hooks := filepath.Join(home, ".gemini/config/hooks.json")
	settings := filepath.Join(home, ".gemini/antigravity-cli/settings.json")
	os.MkdirAll(filepath.Dir(hooks), 0o755)
	os.MkdirAll(filepath.Dir(settings), 0o755)
	os.WriteFile(hooks, []byte(`{"lint":{"PostToolUse":[]}}`), 0o644)
	os.WriteFile(settings, []byte(`{"trustedWorkspaces":["/w"],"permissions":{"allow":["command(git)"]}}`), 0o644)
	tg, _ := Find("agy")
	for range 2 {
		if err := tg.Install(home, "/opt/agm"); err != nil {
			t.Fatal(err)
		}
	}
	if s := tg.Status(home, "/opt/agm"); s != "current" {
		t.Fatalf("status %q", s)
	}
	h, _ := os.ReadFile(hooks)
	st, _ := os.ReadFile(settings)
	if !strings.Contains(string(h), `"lint"`) || !strings.Contains(string(h), `"/opt/agm hook agy PreInvocation"`) {
		t.Fatalf("hooks.json:\n%s", h)
	}
	if !strings.Contains(string(st), `"command(git)"`) || !strings.Contains(string(st), `"/w"`) || strings.Count(string(st), "command(regex:") != 1 {
		t.Fatalf("settings.json:\n%s", st)
	}

	// The allow rule, as agy (Go regexp) would evaluate it.
	var cfg struct{ Permissions struct{ Allow []string } }
	json.Unmarshal(st, &cfg)
	rule := cfg.Permissions.Allow[1]
	re := regexp.MustCompile(strings.TrimSuffix(strings.TrimPrefix(rule, "command(regex:"), ")"))
	for cmd, want := range map[string]bool{
		"agm list":                            true,
		`agm send bob "done: 6*7 = 42"`:       true,
		"/opt/agm reply 1a2b ok":              true,
		"agm list; rm -rf ~":                  false,
		"agm send bob hi && curl evil":        false,
		`agm send bob "$(cat ~/.ssh/id_rsa)"`: false,
		"agm send bob hi | sh":                false,
		"rm -rf ~ # agm list":                 false,
		"agm install":                         false,
		"agm spawn task":                      false,
		"agm send bob hi\nrm -rf ~":           false,
		"/tmp/evil/agm list":                  false,
		"agm show 0123abcd":                   true,
		"agm history -n 5":                    true,
		"agm send-file bob x.md":              false,
		"agm whoami -json":                    true,
		"agm resolve bob":                     true,
		"agm wait -reply-to abc":              true,
		"agm ack 0123456789abcdef":            true,
		"agm ask-file bob -":                  false,
	} {
		if got := re.MatchString(cmd); got != want {
			t.Errorf("rule matches %q = %v, want %v", cmd, got, want)
		}
	}

	if err := tg.Uninstall(home); err != nil {
		t.Fatal(err)
	}
	h, _ = os.ReadFile(hooks)
	st, _ = os.ReadFile(settings)
	if strings.Contains(string(h), "mesh") || strings.Contains(string(st), "mesh") || !strings.Contains(string(st), `"command(git)"`) {
		t.Fatalf("after uninstall:\n%s\n%s", h, st)
	}
}

// TestFillNeverRescansValues: fill is single-pass; a value that looks like a
// placeholder is inserted verbatim (peer text must never be filled at all).
func TestFillNeverRescansValues(t *testing.T) {
	if got := fill("{cli} reply {msg}", "/opt/{msg}/agm", "", "ab12cd34ef56gh78"); got != "/opt/{msg}/agm reply ab12cd34ef56gh78" {
		t.Fatalf("fill rescans inserted values: %q", got)
	}
}

// TestAdapterTextInjected: install puts the canonical templates (text.go) into the
// pi/omp and opencode adapters, leaving no placeholder behind.
func TestAdapterTextInjected(t *testing.T) {
	js := textJSON()
	var txt map[string]string
	if err := json.Unmarshal([]byte(js), &txt); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pi", "opencode"} {
		home := t.TempDir()
		tg, _ := Find(name)
		if err := tg.Install(home, "/opt/agm"); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(home, tg.Path))
		if err != nil {
			t.Fatal(err)
		}
		body := string(data)
		if strings.Contains(body, "__MESH_TEXT__") {
			t.Errorf("%s: __MESH_TEXT__ placeholder left in the installed adapter", name)
		}
		if !strings.Contains(body, "const TEXT = "+js+";") {
			t.Errorf("%s: installed adapter does not carry the injected TEXT templates", name)
		}
		for key, tmpl := range txt {
			q, err := json.Marshal(tmpl)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(body, string(q)) {
				t.Errorf("%s: template %q missing from the installed adapter", name, key)
			}
		}
	}
}
