package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

func TestDaemonRefreshOnlyManagedTargets(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "agm-refresh-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(home)
	t.Setenv("HOME", home)
	pi, _ := integrations.Find("pi")
	omp, _ := integrations.Find("omp")
	skill, _ := integrations.Find("skill")
	if err := pi.Install(home, "/previous/agm"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, omp.Detect), 0700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(home, skill.Path)
	if err := os.MkdirAll(filepath.Dir(foreign), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreign, []byte("user instructions"), 0600); err != nil {
		t.Fatal(err)
	}

	bin, _ := binPath()
	for _, tc := range []struct{ sock, want string }{
		{filepath.Join(home, "isolated", "mesh.sock"), "outdated"},
		{filepath.Join(home, ".agent-mesh", "mesh.sock"), "current"},
	} {
		if err := os.MkdirAll(filepath.Dir(tc.sock), 0700); err != nil {
			t.Fatal(err)
		}
		done := daemonStart(t, tc.sock)
		daemonUp(t, tc.sock)
		daemonStop(t, tc.sock, done)
		if got := pi.Status(home, bin); got != tc.want {
			t.Fatalf("pi after %s: %s", tc.sock, got)
		}
		if got := omp.Status(home, bin); got != "not installed" {
			t.Fatalf("unselected omp: %s", got)
		}
		if b, err := os.ReadFile(foreign); err != nil || string(b) != "user instructions" {
			t.Fatalf("foreign skill: %q, %v", b, err)
		}
	}
}
