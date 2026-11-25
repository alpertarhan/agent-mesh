package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHelloFreshnessCheckpoint(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool.json")
	b, err := New(DefaultLimits(), spool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	bind := func(name string) {
		t.Helper()
		if err := b.Hello(SessionInfo{ID: "session", Name: name}, nil, false, false); err != nil {
			t.Fatal(err)
		}
	}
	file := func() os.FileInfo {
		t.Helper()
		st, err := os.Stat(spool)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	restore := func() SessionInfo {
		t.Helper()
		restarted, err := New(DefaultLimits(), spool)
		if err != nil {
			t.Fatal(err)
		}
		rows := restarted.List()
		if len(rows) != 1 {
			t.Fatalf("restored %d sessions", len(rows))
		}
		return rows[0]
	}
	bind("first")
	original := file()
	for range 40 {
		now = now.Add(50 * time.Millisecond)
		bind("")
		if !os.SameFile(original, file()) {
			t.Fatal("unchanged hello replaced snapshot")
		}
	}
	if !b.List()[0].LastSeen.Equal(now) {
		t.Fatal("live last_seen is stale")
	}
	now = now.Add(time.Minute)
	bind("")
	if !restore().LastSeen.Equal(now) {
		t.Fatal("freshness was never checkpointed")
	}
	if err := os.Mkdir(spool+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	bind("renamed") // best-effort checkpoint fails
	if restore().Name != "first" {
		t.Fatal("failed write changed spool")
	}
	if err := os.Remove(spool + ".tmp"); err != nil {
		t.Fatal(err)
	}
	bind("")
	if restore().Name != "renamed" {
		t.Fatal("unchanged hello failed to retry identity write")
	}
	bind("latest")
	if restore().Name != "latest" {
		t.Fatal("rename was coalesced")
	}
}
