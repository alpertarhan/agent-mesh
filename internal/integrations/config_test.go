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
