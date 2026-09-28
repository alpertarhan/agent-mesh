package integrations

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// TestAdapterScripts runs the JS adapter checks in testdata with node (>= 22.18 or 23.6,
// which run TypeScript natively). The pi check talks to an isolated in-process broker.
func TestAdapterScripts(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	// Short path: unix socket paths are limited (~104 bytes on macOS).
	dir, err := os.MkdirTemp("/tmp", "agm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	probe := filepath.Join(dir, "probe.mts")
	os.WriteFile(probe, []byte("const x: number = 1;\n"), 0o600)
	if out, err := exec.Command(node, probe).CombinedOutput(); err != nil {
		t.Skipf("node cannot run TypeScript: %s", out)
	}

	sock := filepath.Join(dir, "s.sock")
	ln, err := broker.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := broker.New(broker.DefaultLimits(), "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- broker.Serve(ctx, ln, b) }()
	t.Cleanup(func() { cancel(); <-done })

	// A stand-in for the host TUI package (Box/Text), next to the pi adapter only; the
	// "pi-fallback" run loads the adapter from a directory without it.
	tuiDir := filepath.Join(dir, "pi", "node_modules", "@earendil-works", "pi-tui")
	os.MkdirAll(tuiDir, 0o700)
	os.MkdirAll(filepath.Join(dir, "bare"), 0o700)
	os.WriteFile(filepath.Join(tuiDir, "package.json"), []byte(`{"name":"@earendil-works/pi-tui","type":"module","main":"index.js"}`), 0o600)
	os.WriteFile(filepath.Join(tuiDir, "index.js"), []byte(`export class Text { constructor(t) { this.text = t; } render() { return this.text.split("\n"); } }
export class Box { constructor() { this.children = []; } addChild(c) { this.children.push(c); } render(w) { return this.children.flatMap((c) => c.render(w)); } }
`), 0o600)

	for name, tc := range map[string]struct{ harness, file, script, mode string }{
		"pi":          {"pi", "pi/agent-mesh.mts", "pi_check.mjs", ""},
		"pi-fallback": {"pi", "bare/agent-mesh.mts", "pi_check.mjs", "fallback"},
		"opencode":    {"opencode", "tui.js", "opencode_check.mjs", ""},
	} {
		t.Run(name, func(t *testing.T) {
			tg, _ := Find(tc.harness)
			adapter := filepath.Join(dir, tc.file)
			if err := os.WriteFile(adapter, []byte(tg.render("/nonexistent/agm")), 0o600); err != nil {
				t.Fatal(err)
			}
			script, _ := filepath.Abs(filepath.Join("testdata", tc.script))
			cctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			cmd := exec.CommandContext(cctx, node, script, adapter, tc.mode)
			cmd.Env = append(os.Environ(), "AGM_SOCKET="+sock, "AGM_BIN=/nonexistent/agm", "AGM_QUIET=", "AGM_NAME=")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			t.Logf("%s", out)
		})
	}
}
