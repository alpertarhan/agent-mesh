package main

import (
	"strings"
	"testing"
)

func TestSpawnPromptReturnAddress(t *testing.T) {
	for _, id := range []string{"fixed-id", "-flag", "parent's session"} {
		prompt := spawnPromptText("worker", id, "check")
		if spawnName(prompt) != "worker" || !strings.Contains(prompt, " send -- "+shellQuote(id)+" ") {
			t.Errorf("wrong return address: %s", prompt)
		}
	}
	if prompt := spawnPromptText("worker", "", "check"); strings.Contains(prompt, " send ") {
		t.Errorf("anonymous parent: %s", prompt)
	}
}
