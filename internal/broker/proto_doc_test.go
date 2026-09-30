package broker

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestProtocolDocCoversOpsAndErrorCodes keeps docs/protocol.md in sync with the wire
// surface: every op handled in dispatch (server.go) needs its own "### `op`" heading,
// and every error code constant in proto.go its own "| `code` | ..." table row. Both
// checks are line-anchored: a mention inside another row or sentence does not count.
func TestProtocolDocCoversOpsAndErrorCodes(t *testing.T) {
	doc, err := os.ReadFile("../../docs/protocol.md")
	if err != nil {
		t.Fatal(err)
	}
	server, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	proto, err := os.ReadFile("proto.go")
	if err != nil {
		t.Fatal(err)
	}
	// Ops: every quoted value on a `case` line (multi-value cases included), plus the
	// if-based hello. Channel cases (`case <-c.done:`) match no quoted value.
	ops := map[string]bool{}
	quoted := regexp.MustCompile(`"([a-z_]+)"`)
	for _, line := range strings.Split(string(server), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "case ") {
			for _, m := range quoted.FindAllStringSubmatch(line, -1) {
				ops[m[1]] = true
			}
		}
	}
	for _, m := range regexp.MustCompile(`req\.Op == "([a-z_]+)"`).FindAllStringSubmatch(string(server), -1) {
		ops[m[1]] = true
	}
	for _, op := range []string{"hello", "send", "bye"} { // extraction invariants
		if !ops[op] {
			t.Fatalf("op extraction broke: %q not found in server.go dispatch", op)
		}
	}
	lines := strings.Split(string(doc), "\n")
	hasLinePrefix := func(p string) bool {
		for _, l := range lines {
			if strings.HasPrefix(l, p) {
				return true
			}
		}
		return false
	}
	var missing []string
	for op := range ops {
		if !hasLinePrefix("### `" + op + "`") {
			missing = append(missing, "op "+op+" (no \"### `"+op+"`\" heading)")
		}
	}
	for _, m := range regexp.MustCompile(`Code\w+\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(string(proto), -1) {
		if !hasLinePrefix("| `" + m[1] + "` |") {
			missing = append(missing, "error code "+m[1]+" (no \"| `"+m[1]+"` |\" row)")
		}
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Errorf("%s in server.go/proto.go but missing from docs/protocol.md", m)
	}
}
