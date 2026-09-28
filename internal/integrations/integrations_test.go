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

func TestClaudeHooksMerge(t *testing.T) {
	home := t.TempDir()
	cfg := filepath.Join(home, ".claude/settings.json")
	os.MkdirAll(filepath.Dir(cfg), 0o755)
	orig := `{"hooks":{"SessionStart":[{"matcher":"x","hooks":[{"type":"command","command":"bash herdr.sh"}]}]},"theme":"auto","permissions":{"allow":["Bash(ls:*)"]}}`
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
	for _, want := range []string{"bash herdr.sh", "Bash(ls:*)", `"/new/agm hook claude --wait"`, `"asyncRewake": true`, "Bash(agm *)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in\n%s", want, got)
		}
	}
	if strings.Contains(got, "/old/agm") || strings.Count(got, "/new/agm hook claude\"") != 5 {
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
	if strings.Contains(got, "mesh") || !strings.Contains(got, "bash herdr.sh") || !strings.Contains(got, "Bash(ls:*)") {
		t.Fatalf("uninstall must leave only foreign entries:\n%s", got)
	}
	if s := tg.Status(home, "/new/agm"); s != "not installed" {
		t.Fatalf("status %q", s)
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
