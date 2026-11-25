package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRoleSetClearPersistenceAndValidation(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	hello(t, b, "a", "alice")
	if info, _ := b.Resolve("a"); info.Role != nil {
		t.Fatalf("new id started with role: %+v", info.Role)
	}
	role := &RoleInfo{Name: "reviewer", Description: "reviews authorization changes"}
	if err := b.SetRole("a", role); err != nil {
		t.Fatal(err)
	}
	info, _ := b.Resolve("a")
	if info.Role == nil || info.Role.Name != "reviewer" || info.Role.Description != "reviews authorization changes" {
		t.Fatalf("role not stored: %+v", info.Role)
	}
	role.Description = "mutated"
	info, _ = b.Resolve("a")
	if info.Role.Description != "reviews authorization changes" {
		t.Fatal("role aliases caller memory")
	}
	restored, _ := New(DefaultLimits(), spool)
	info, _ = restored.Resolve("a")
	if info.Role == nil || info.Role.Name != "reviewer" {
		t.Fatal("role did not survive restart")
	}
	for _, bad := range []*RoleInfo{
		nil,
		{Name: "only-name"},
		{Name: " ", Description: "\n"},
		{Name: "scout", Description: "look\nelsewhere"},
		{Description: "only-description"},
		{Name: string(make([]byte, 65))},
		{Description: string(make([]byte, 257))},
	} {
		if err := b.SetRole("a", bad); code(err) != CodeBadRequest {
			t.Errorf("invalid role accepted: %+v (%v)", bad, err)
		}
	}
	if err := os.Mkdir(spool+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRole("a", &RoleInfo{Name: "x", Description: "y"}); code(err) != CodePersistence {
		t.Fatalf("want persistence failure, got %v", err)
	}
	info, _ = b.Resolve("a")
	if info.Role.Name != "reviewer" {
		t.Fatal("failed role update was not rolled back")
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRole("a", &RoleInfo{}); err != nil {
		t.Fatal(err)
	}
	info, _ = b.Resolve("a")
	if info.Role != nil {
		t.Fatal("role clear did not remove role")
	}
}
