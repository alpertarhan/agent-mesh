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

// Installed SDKs are opt-in: normal CI needs neither global packages nor credentials.
func TestInstalledSDKAdapters(t *testing.T) {
	for _, host := range []struct{ name, env, runtime string }{
		{"pi", "AGM_PI_SDK", "node"},
		{"omp", "AGM_OMP_SDK", "bun"},
	} {
		t.Run(host.name, func(t *testing.T) {
			sdk := os.Getenv(host.env)
			if sdk == "" {
				t.Skipf("set %s to an installed SDK package directory", host.env)
			}
			runtime, err := exec.LookPath(host.runtime)
			if err != nil {
				t.Fatal(err)
			}
			dir, err := os.MkdirTemp("/tmp", "agm-sdk-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("preserved failed SDK fixture: %s", dir)
				} else {
					os.RemoveAll(dir)
				}
			})
			socket := filepath.Join(dir, "mesh.sock")
			ln, err := broker.Listen(socket)
			if err != nil {
				t.Fatal(err)
			}
			b, err := broker.New(broker.DefaultLimits(), "")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- broker.Serve(ctx, ln, b) }()
			t.Cleanup(func() { cancel(); <-done })
			target, _ := Find(host.name)
			adapter := filepath.Join(dir, "agent-mesh.mts")
			if err := os.WriteFile(adapter, []byte(target.render("/nonexistent/agm")), 0o600); err != nil {
				t.Fatal(err)
			}
			script, err := filepath.Abs("testdata/sdk_check.mjs")
			if err != nil {
				t.Fatal(err)
			}
			deadline, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			cmd := exec.CommandContext(deadline, runtime, script, adapter, host.name, sdk)
			cmd.Dir = dir
			cmd.Env = []string{
				"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TMPDIR=" + dir,
				"XDG_CONFIG_HOME=" + filepath.Join(dir, "config"),
				"XDG_CACHE_HOME=" + filepath.Join(dir, "cache"),
				"XDG_DATA_HOME=" + filepath.Join(dir, "data"),
				"PI_CODING_AGENT_DIR=" + filepath.Join(dir, "agent"),
				"AGM_SOCKET=" + socket, "AGM_BIN=/nonexistent/agm",
			}
			output, err := cmd.CombinedOutput()
			os.WriteFile(filepath.Join(dir, "output.log"), output, 0o600)
			if err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			t.Logf("%s", output)
		})
	}
}
