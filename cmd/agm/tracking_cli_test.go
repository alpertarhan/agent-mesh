package main

import (
	"strings"
	"testing"
)

func TestTrackingUsage(t *testing.T) {
	out, stderr, code := agm(t, "", "help")
	if code != 0 || stderr != "" || !strings.Contains(out, "agm track MESSAGE_ID") {
		t.Fatalf("help: %q %q (%d)", out, stderr, code)
	}
	for _, cmd := range []string{"track", "seen"} {
		_, stderr, code := agm(t, "", cmd, "-json", "prefix")
		if code != 2 || jsonError(t, stderr) != codeUsage {
			t.Fatalf("%s: %s (%d)", cmd, stderr, code)
		}
	}
}
