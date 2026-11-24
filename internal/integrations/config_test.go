package integrations

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteConfigPreservesPermissions(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o640, 0o644} {
		p := filepath.Join(t.TempDir(), "settings.json")
		prev := []byte(`{"private":"sensitive config"}`)
		if err := os.WriteFile(p, prev, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		// An older installer may already have left a permissive backup/temp file.
		for _, suffix := range []string{".bak", ".tmp"} {
			if err := os.WriteFile(p+suffix, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := writeConfig(p, prev, []byte(`{"private":"sensitive config","mesh":true}`)); err != nil {
			t.Fatal(err)
		}
		for _, file := range []string{p, p + ".bak"} {
			info, err := os.Stat(file)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != mode {
				t.Errorf("%s permissions = %o, want %o", filepath.Base(file), info.Mode().Perm(), mode)
			}
		}
	}
	p := filepath.Join(t.TempDir(), "new.json")
	if err := writeConfig(p, nil, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("new config permissions = %o, want 600", info.Mode().Perm())
	}
}

// A symlinked config path is never replaced with a regular file: live and
// dangling links are refused before any backup or write, and a write that
// changes nothing stays a no-op that leaves the link alone.
func TestWriteConfigRefusesSymlink(t *testing.T) {
	link := func(t *testing.T, dir, target string) string {
		t.Helper()
		p := filepath.Join(dir, "settings.json")
		if err := os.Symlink(target, p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	intact := func(t *testing.T, p, want string) {
		t.Helper()
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("path no longer a symlink: %v", err)
		}
		if data, err := os.ReadFile(p); err != nil || string(data) != want {
			t.Fatalf("target changed: %q %v", data, err)
		}
		if _, err := os.Lstat(p + ".bak"); !os.IsNotExist(err) {
			t.Fatal("backup written for a refused symlink")
		}
	}

	t.Run("live link", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "target.json")
		prev := []byte("{\"private\":\"sensitive config\"}")
		if err := os.WriteFile(target, prev, 0o644); err != nil {
			t.Fatal(err)
		}
		p := link(t, t.TempDir(), target)
		if err := writeConfig(p, prev, []byte("{\"private\":\"sensitive config\",\"mesh\":true}")); err == nil {
			t.Fatal("write through a symlink succeeded")
		}
		intact(t, p, string(prev))
	})

	t.Run("dangling link", func(t *testing.T) {
		p := link(t, t.TempDir(), filepath.Join(t.TempDir(), "missing.json"))
		if err := writeConfig(p, nil, []byte("{}")); err == nil {
			t.Fatal("write through a dangling symlink succeeded")
		}
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("dangling link replaced: %v", err)
		}
		if _, err := os.Lstat(p + ".bak"); !os.IsNotExist(err) {
			t.Fatal("backup written for a refused dangling link")
		}
	})

	t.Run("no-op leaves link alone", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "target.json")
		prev := []byte("{\"private\":\"sensitive config\"}")
		if err := os.WriteFile(target, prev, 0o644); err != nil {
			t.Fatal(err)
		}
		p := link(t, t.TempDir(), target)
		if err := writeConfig(p, prev, prev); err != nil {
			t.Fatalf("no-op on a symlink: %v", err)
		}
		intact(t, p, string(prev))
	})
}
func TestParseDocRejectsTrailingData(t *testing.T) {
	for _, s := range []string{`{"x":1`, `{"x":1} garbage`, `{"x":1} {"other":2}`} {
		if _, err := parseDoc([]byte(s)); err == nil {
			t.Errorf("accepted invalid JSON config: %q", s)
		}
	}
	if _, err := parseDoc([]byte("{\"x\":1} \n\t")); err != nil {
		t.Fatalf("valid trailing whitespace: %v", err)
	}
}
