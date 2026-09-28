// Package integrations installs mesh into harnesses (herdr-style). A target is an
// optional file fully owned by mesh plus edits to shared config files, where mesh only
// adds/removes its own entries (recognized by content, so a moved binary is still ours).
package integrations

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

//go:embed pi.ts
var piAdapter string

//go:embed opencode.js
var opencodeAdapter string

//go:embed skill.md
var skillDoc string

type Target struct {
	Name   string
	Detect string // dir (relative to $HOME) whose presence means the harness is set up
	Path   string // file fully owned by mesh, relative to $HOME (optional)
	render func(bin string) string
	Edits  []Edit // entries mesh owns inside shared config files
}

// Edit rewrites one shared config file: it drops the mesh-owned entries from prev
// (nil = missing file) and, if add, appends the current ones. had reports whether
// prev held any mesh entries.
type Edit interface {
	file() string
	edit(prev []byte, bin string, add bool) (next []byte, had bool, err error)
}

func piLike(harness string) func(string) string {
	return func(bin string) string {
		return header(harness) + strings.NewReplacer("__MESH_HARNESS__", harness, "__MESH_BIN__", bin).Replace(piAdapter)
	}
}

var Targets = []Target{
	{Name: "pi", Detect: ".pi/agent", Path: ".pi/agent/extensions/agent-mesh.ts", render: piLike("pi")},
	{Name: "omp", Detect: ".omp/agent", Path: ".omp/agent/extensions/agent-mesh.ts", render: piLike("omp")},
	// opencode v2 loads TUI plugins from directories listed in cli.json "plugins".
	{Name: "opencode", Detect: ".config/opencode", Path: ".config/opencode/agent-mesh/tui.js",
		render: func(bin string) string {
			return header("opencode") + strings.ReplaceAll(opencodeAdapter, "__MESH_BIN__", bin)
		},
		Edits: []Edit{&listEdit{File: ".config/opencode/cli.json", Path: []string{"plugins"},
			Value: func(string) string { return "./agent-mesh" },
			Ours:  func(v string) bool { return v == "./agent-mesh" }}}},
	{Name: "claude", Detect: ".claude", Edits: []Edit{&hookEdit{File: ".claude/settings.json", Harness: "claude", Wait: true, Allow: true}}},
	// Codex sandboxes shell commands (no access to the mesh socket); this execpolicy
	// rule runs the messaging subcommands outside the sandbox without prompting.
	{Name: "codex", Detect: ".codex", Path: ".codex/rules/agent-mesh.rules",
		render: func(bin string) string {
			return strings.ReplaceAll(header("codex"), "//", "#") + fmt.Sprintf(`prefix_rule(
    pattern = [[%q, "agm"], ["list", "send", "ask", "reply", "inbox"]],
    decision = "allow",
    justification = "agent-mesh: talk to the local agm daemon (unix socket outside the sandbox)",
    match = ["agm send peer hi", %q],
    not_match = ["agm install", "agm daemon"],
)
`, bin, bin+" reply 1 ok")
		},
		Edits: []Edit{&hookEdit{File: ".codex/hooks.json", Harness: "codex"}}},
	{Name: "crush", Detect: ".config/crush", Edits: []Edit{crushHook}},
	// Antigravity CLI: hooks under our own key in the global hooks.json, and an
	// allow rule for plain mesh messaging commands (anchored: agy matches unanchored
	// regexes against any part of a chained command).
	{Name: "agy", Detect: ".gemini/antigravity-cli", Edits: []Edit{
		&keyEdit{File: ".gemini/config/hooks.json", Key: "agent-mesh", Value: func(bin string) any {
			h := func(ev string) []any {
				return []any{map[string]any{"type": "command", "command": bin + " hook agy " + ev, "timeout": 10}}
			}
			return map[string]any{"PreInvocation": h("PreInvocation"), "Stop": h("Stop")}
		}},
		&listEdit{File: ".gemini/antigravity-cli/settings.json", Path: []string{"permissions", "allow"},
			Value: func(bin string) string {
				return "command(regex:^(agm|" + regexp.QuoteMeta(bin) + ") (list|send|ask|reply|inbox)( [^;&|<>$`\\\\\\n\\r]*)?$)"
			},
			Ours: func(v string) bool {
				return strings.HasPrefix(v, "command(regex:^") && strings.Contains(v, "agm") && strings.Contains(v, "(list|send|ask|reply|inbox)")
			}},
	}},
	// Shared skill (crush, pi, omp, codex read ~/.agents/skills).
	{Name: "skill", Detect: ".agents/skills", Path: ".agents/skills/agent-mesh/SKILL.md",
		render: func(bin string) string { return strings.ReplaceAll(skillDoc, "__MESH_BIN__", bin) }},
}

func header(id string) string {
	return fmt.Sprintf("// installed by agent-mesh; `agm install %s` overwrites this file, `agm uninstall %[1]s` removes it.\n// MESH_INTEGRATION_ID=%[1]s\n", id)
}

func ownedBy(data []byte, name string) bool {
	return regexp.MustCompile(`MESH_INTEGRATION_ID=` + regexp.QuoteMeta(name) + `\b`).Match(data)
}

func Find(name string) (*Target, error) {
	for i := range Targets {
		if Targets[i].Name == name {
			return &Targets[i], nil
		}
	}
	return nil, fmt.Errorf("unknown harness %q", name)
}

// Installed reports whether the harness is set up on this machine.
func (t *Target) Installed(home string) bool {
	_, err := os.Stat(filepath.Join(home, t.Detect))
	return err == nil
}

// Status: "not installed", "current", "outdated" (differs from what this binary at bin
// would install, e.g. adapter changed or binary moved), or "foreign" (a file at our
// path that mesh did not write; never touched).
func (t *Target) Status(home, bin string) string {
	var parts, present, current int
	if t.Path != "" {
		parts++
		data, err := os.ReadFile(filepath.Join(home, t.Path))
		switch {
		case err != nil:
		case !ownedBy(data, t.Name):
			return "foreign"
		default:
			present++
			if string(data) == t.render(bin) {
				current++
			}
		}
	}
	for _, e := range t.Edits {
		parts++
		p, c := editState(home, bin, e)
		if p {
			present++
		}
		if c {
			current++
		}
	}
	switch {
	case present == 0:
		return "not installed"
	case current == parts:
		return "current"
	}
	return "outdated"
}

func (t *Target) Install(home, bin string) error {
	if t.Path != "" {
		if t.Status(home, bin) == "foreign" {
			return fmt.Errorf("%s exists and is not managed by agent-mesh", t.Path)
		}
		dst := filepath.Join(home, t.Path)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := writeAtomic(dst, []byte(t.render(bin))); err != nil {
			return err
		}
	}
	for _, e := range t.Edits {
		if err := applyEdit(home, bin, e, true); err != nil {
			return err
		}
	}
	return nil
}

func (t *Target) Uninstall(home string) error {
	if t.Path != "" {
		p := filepath.Join(home, t.Path)
		data, err := os.ReadFile(p)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return err
		case !ownedBy(data, t.Name):
			return fmt.Errorf("%s is not managed by agent-mesh; leaving it", t.Path)
		default:
			if err := os.Remove(p); err != nil {
				return err
			}
			if filepath.Base(filepath.Dir(p)) == "agent-mesh" {
				os.Remove(filepath.Dir(p)) // our own dir; only succeeds if empty
			}
		}
	}
	for _, e := range t.Edits {
		if err := applyEdit(home, "", e, false); err != nil {
			return err
		}
	}
	return nil
}

func writeAtomic(dst string, data []byte) error {
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// writeConfig backs up the previous content (.bak) and writes data.
func writeConfig(p string, prev, data []byte) error {
	if bytes.Equal(prev, data) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if len(prev) > 0 {
		if err := os.WriteFile(p+".bak", prev, 0o644); err != nil {
			return err
		}
	}
	return writeAtomic(p, data)
}

func applyEdit(home, bin string, e Edit, add bool) error {
	p := filepath.Join(home, e.file())
	prev, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		prev, err = nil, nil
	}
	if err != nil {
		return err
	}
	next, had, err := e.edit(prev, bin, add)
	if err != nil {
		return fmt.Errorf("%s: %w", e.file(), err)
	}
	if !add && !had {
		return nil // nothing of ours: do not even reformat the file
	}
	return writeConfig(p, prev, next)
}

// editState: present = the file holds mesh entries; current = installing changes nothing.
func editState(home, bin string, e Edit) (present, current bool) {
	prev, err := os.ReadFile(filepath.Join(home, e.file()))
	if err != nil {
		return false, false
	}
	next, had, err := e.edit(prev, bin, true)
	return had && err == nil, err == nil && bytes.Equal(next, prev)
}

// --- JSON documents with top-level key order preserved ---

type jsonDoc struct {
	keys []string
	vals map[string]json.RawMessage
}

// parseDoc parses a JSON object; empty input is an empty doc. JSONC is refused.
func parseDoc(data []byte) (*jsonDoc, error) {
	d := &jsonDoc{vals: map[string]json.RawMessage{}}
	if len(bytes.TrimSpace(data)) == 0 {
		return d, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		k := tok.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, err
		}
		if _, dup := d.vals[k]; !dup {
			d.keys = append(d.keys, k)
		}
		d.vals[k] = raw
	}
	return d, nil
}

func (d *jsonDoc) get(k string, v any) error {
	if raw, ok := d.vals[k]; ok {
		return json.Unmarshal(raw, v)
	}
	return nil
}

// set stores v under k, or deletes k if empty.
func (d *jsonDoc) set(k string, v any, empty bool) {
	if empty {
		delete(d.vals, k)
		d.keys = slices.DeleteFunc(d.keys, func(x string) bool { return x == k })
		return
	}
	raw, _ := json.Marshal(v) // values came from JSON: always marshalable
	if _, ok := d.vals[k]; !ok {
		d.keys = append(d.keys, k)
	}
	d.vals[k] = raw
}

func (d *jsonDoc) bytes() []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range d.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		b.Write(d.vals[k])
	}
	b.WriteByte('}')
	var out bytes.Buffer
	json.Indent(&out, b.Bytes(), "", "  ")
	out.WriteByte('\n')
	return out.Bytes()
}

// --- listEdit: one string in a JSON array at Path (depth 1 or 2) ---

type listEdit struct {
	File  string
	Path  []string // e.g. {"plugins"} or {"permissions", "allow"}
	Value func(bin string) string
	Ours  func(v string) bool
}

func (l *listEdit) file() string { return l.File }

func (l *listEdit) edit(prev []byte, bin string, add bool) ([]byte, bool, error) {
	d, err := parseDoc(prev)
	if err != nil {
		return nil, false, fmt.Errorf("%w (add %q to %s by hand)", err, l.Value(bin), strings.Join(l.Path, "."))
	}
	var parent map[string]any // only for depth 2
	var list []any
	if len(l.Path) == 1 {
		err = d.get(l.Path[0], &list)
	} else if err = d.get(l.Path[0], &parent); err == nil {
		if parent == nil {
			parent = map[string]any{}
		}
		list, _ = parent[l.Path[1]].([]any)
	}
	if err != nil {
		return nil, false, err
	}
	n := len(list)
	list = slices.DeleteFunc(list, func(v any) bool { s, ok := v.(string); return ok && l.Ours(s) })
	had := len(list) < n
	if add {
		list = append(list, l.Value(bin))
	}
	if parent == nil {
		d.set(l.Path[0], list, len(list) == 0)
	} else {
		if len(list) == 0 {
			delete(parent, l.Path[1])
		} else {
			parent[l.Path[1]] = list
		}
		d.set(l.Path[0], parent, len(parent) == 0)
	}
	return d.bytes(), had, nil
}

// --- keyEdit: one top-level key of a JSON object, fully owned by mesh ---

type keyEdit struct {
	File, Key string
	Value     func(bin string) any
}

func (k *keyEdit) file() string { return k.File }

func (k *keyEdit) edit(prev []byte, bin string, add bool) ([]byte, bool, error) {
	d, err := parseDoc(prev)
	if err != nil {
		return nil, false, err
	}
	_, had := d.vals[k.Key]
	if add {
		d.set(k.Key, k.Value(bin), false) // in place if present
	} else {
		d.set(k.Key, nil, true)
	}
	return d.bytes(), had, nil
}

// --- hookEdit: Claude Code / Codex hooks (same JSON shape) ---

type hookEdit struct {
	File    string
	Harness string
	Wait    bool // Claude asyncRewake waiter on Stop
	Allow   bool // Claude permissions.allow for the agm CLI
}

var hookEvents = []string{"SessionStart", "UserPromptSubmit", "PostToolUse", "Stop", "SessionEnd"}

func (h *hookEdit) file() string { return h.File }

func (h *hookEdit) ours(x any) bool {
	m, _ := x.(map[string]any)
	cmd, _ := m["command"].(string)
	f := strings.Fields(cmd)
	return len(f) >= 3 && filepath.Base(f[0]) == "agm" && f[1] == "hook" && f[2] == h.Harness
}

func oursAllow(r any) bool {
	s, _ := r.(string)
	return strings.HasPrefix(s, "Bash(agm ") || strings.HasPrefix(s, "Bash(agm:") ||
		strings.Contains(s, "/agm ") || strings.Contains(s, "/agm:")
}

type hookGroup = map[string]any

func (h *hookEdit) edit(prev []byte, bin string, add bool) ([]byte, bool, error) {
	d, err := parseDoc(prev)
	if err != nil {
		return nil, false, err
	}
	hooks, perms := map[string][]hookGroup{}, map[string]any{}
	if err := d.get("hooks", &hooks); err != nil {
		return nil, false, err
	}
	if err := d.get("permissions", &perms); err != nil {
		return nil, false, err
	}
	had := false
	// Drop our entries (and groups that only held ours), keep everything else.
	for ev, groups := range hooks {
		var kept []hookGroup
		for _, g := range groups {
			list, _ := g["hooks"].([]any)
			n := len(list)
			list = slices.DeleteFunc(list, h.ours)
			if len(list) < n {
				had = true
				if len(list) == 0 {
					continue
				}
			}
			g["hooks"] = list
			kept = append(kept, g)
		}
		hooks[ev] = kept
		if len(kept) == 0 {
			delete(hooks, ev)
		}
	}
	allow, _ := perms["allow"].([]any)
	n := len(allow)
	allow = slices.DeleteFunc(allow, oursAllow)
	had = had || len(allow) < n
	if add {
		cmd := bin + " hook " + h.Harness
		for _, ev := range hookEvents {
			timeout := 30
			if ev == "SessionEnd" {
				timeout = 3 // only sends bye; Codex clamps SessionEnd hooks to 3s anyway
			}
			list := []any{map[string]any{"type": "command", "command": cmd, "timeout": timeout}}
			if h.Wait && ev == "Stop" {
				list = append(list, map[string]any{"type": "command", "command": cmd + " --wait", "asyncRewake": true, "timeout": 3600})
			}
			hooks[ev] = append(hooks[ev], hookGroup{"hooks": list})
		}
		if h.Allow {
			allow = append(allow, "Bash("+bin+" *)", "Bash("+bin+":*)", "Bash(agm *)", "Bash(agm:*)")
		}
	}
	d.set("hooks", hooks, len(hooks) == 0)
	if _, hasPerms := d.vals["permissions"]; hasPerms || len(allow) > 0 {
		if allow == nil {
			allow = []any{}
		}
		perms["allow"] = allow
		d.set("permissions", perms, false)
	}
	return d.bytes(), had, nil
}

// --- lineEdit: one line in a line-based config (crushrc) ---

type lineEdit struct {
	File string
	Line func(bin string) string
	Ours func(line string) bool
}

var crushHook = &lineEdit{
	File: ".config/crush/crushrc",
	Line: func(bin string) string {
		return fmt.Sprintf("hook add PreToolUse --command %q --name agm", bin+" hook crush")
	},
	Ours: func(l string) bool { return strings.Contains(l, "--name agm") && strings.Contains(l, "hook crush") },
}

func (l *lineEdit) file() string { return l.File }

func (l *lineEdit) edit(prev []byte, bin string, add bool) ([]byte, bool, error) {
	var lines []string
	if len(prev) > 0 {
		lines = strings.Split(strings.TrimRight(string(prev), "\n"), "\n")
	}
	n := len(lines)
	lines = slices.DeleteFunc(lines, l.Ours)
	had := len(lines) < n
	if add {
		lines = append(lines, l.Line(bin))
	}
	out := strings.Join(lines, "\n")
	if out != "" {
		out += "\n"
	}
	return []byte(out), had, nil
}
