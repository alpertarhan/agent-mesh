package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

// runOpenclawCheck runs one node check with the gate (serveLink, name srv) in front
// of the test daemon and the generated plugin in scratch/plugin.
func runOpenclawCheck(t *testing.T, script string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	testBroker(t) // AGM_SOCKET: the real daemon under test
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go serveLink(ctx, lln, "srv", nil)

	scratch := filepath.Join(dir, "scratch")
	if err := integrations.OpenclawPlugin(filepath.Join(scratch, "plugin")); err != nil {
		t.Fatal(err)
	}
	check, err := filepath.Abs(filepath.Join("testdata", script))
	if err != nil {
		t.Fatal(err)
	}
	cctx, stop := context.WithTimeout(context.Background(), 90*time.Second)
	defer stop()
	cmd := exec.CommandContext(cctx, node, check)
	cmd.Env = append(os.Environ(),
		"SCRATCH="+scratch,
		"LINK_SOCK="+linkSock,
		"DAEMON_SOCK="+os.Getenv("AGM_SOCKET"),
		"STATE_DIR="+filepath.Join(dir, "state"),
		"MESH_JS="+filepath.Join(scratch, "plugin", "mesh.js"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	t.Logf("%s", out)
}

// TestOpenclawMeshClient: the plain JS mesh client against a real daemon behind the
// real 3a gate: hello under the prefix, ack after acceptance, reply_to to a local
// wait, permanent errors, replay dedupe, and the outbox across a down link.
func TestOpenclawMeshClient(t *testing.T) {
	runOpenclawCheck(t, "openclaw_mesh_check.mjs")
}

// TestOpenclawAdapter: the generated plugin with a stub of only the SDK calls it
// uses, against the same gate: isolated per-peer routing, frame header, sender
// policy both ways, refusal handling, joined reply_to finals, proactive sends.
func TestOpenclawAdapter(t *testing.T) {
	runOpenclawCheck(t, "openclaw_adapter_check.mjs")
}

// TestPluginOpenclawCommand: the generator writes the four files with the wording
// injected and manifests that parse; a missing -o is a usage error.
func TestPluginOpenclawCommand(t *testing.T) {
	if _, stderr, code := agm(t, "", "plugin", "openclaw"); code == 0 || !strings.Contains(stderr, "-o DIR") {
		t.Fatalf("no -o: %q %d", stderr, code)
	}
	dir := t.TempDir()
	out, _, code := agm(t, "", "plugin", "openclaw", "-o", filepath.Join(dir, "p"))
	if code != 0 || strings.TrimSpace(out) != filepath.Join(dir, "p") {
		t.Fatalf("generate: %q %d", out, code)
	}
	for _, name := range []string{"index.js", "mesh.js", "package.json", "openclaw.plugin.json"} {
		if _, err := os.Stat(filepath.Join(dir, "p", name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "p", "index.js"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("__MESH_TEXT__")) {
		t.Fatal("text placeholder left in the generated entry")
	}
	var manifest map[string]any
	if err := json.Unmarshal(mustRead(t, filepath.Join(dir, "p", "openclaw.plugin.json")), &manifest); err != nil || manifest["id"] != "agent-mesh" {
		t.Fatalf("manifest: %v %v", err, manifest["id"])
	}
	var pkg map[string]any
	if err := json.Unmarshal(mustRead(t, filepath.Join(dir, "p", "package.json")), &pkg); err != nil || pkg["name"] != "agent-mesh" {
		t.Fatalf("package.json: %v", err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestOpenclawSmoke is the opt-in real-runtime check (S5): it never runs in CI and
// needs a real OpenClaw install plus a mock model. See the script header.
func TestOpenclawSmoke(t *testing.T) {
	if os.Getenv("OPENCLAW_SMOKE") != "1" || os.Getenv("OPENCLAW_DIR") == "" || os.Getenv("OPENCLAW_MODEL_URL") == "" {
		t.Skip("opt-in: set OPENCLAW_SMOKE=1, OPENCLAW_DIR and OPENCLAW_MODEL_URL")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go serveLink(ctx, lln, "srv", nil)
	scratch := filepath.Join(dir, "scratch")
	if err := integrations.OpenclawPlugin(filepath.Join(scratch, "plugin")); err != nil {
		t.Fatal(err)
	}
	check, err := filepath.Abs(filepath.Join("testdata", "openclaw_smoke.mjs"))
	if err != nil {
		t.Fatal(err)
	}
	cctx, stop := context.WithTimeout(context.Background(), 15*time.Minute)
	defer stop()
	cmd := exec.CommandContext(cctx, node, check)
	cmd.Env = append(os.Environ(),
		"SCRATCH="+scratch,
		"LINK_SOCK="+linkSock,
		"DAEMON_SOCK="+os.Getenv("AGM_SOCKET"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	t.Logf("%s", out)
}
