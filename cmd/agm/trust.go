package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// checkTrust refuses to spawn Codex, Claude or Antigravity in a directory they have not been
// trusted in: their trust dialog would take the typed task (and its Enter) as the
// answer, i.e. mesh would trust the directory on the user's behalf.
// A directory counts as trusted if it, or its git root, is trusted.
func checkTrust(harness, dir string) error {
	var trusted func(string) bool
	switch harness {
	case "codex":
		trusted = codexTrusted
	case "claude":
		trusted = claudeTrusted
	case "agy":
		trusted = agyTrusted
	default:
		return nil
	}
	cands := []string{dir}
	if real, err := filepath.EvalSymlinks(dir); err == nil && real != dir {
		cands = append(cands, real)
	}
	if out, err := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel").Output(); err == nil {
		cands = append(cands, strings.TrimSpace(string(out)))
	}
	for _, c := range cands {
		if trusted(c) {
			return nil
		}
	}
	return fmt.Errorf("%s has not been trusted in %s: start %s there once and accept its trust prompt, then spawn again", harness, dir, harness)
}

// codexTrusted: ~/.codex/config.toml has [projects."<dir>"] with trust_level = "trusted".
func codexTrusted(dir string) bool {
	home, _ := os.UserHomeDir()
	codexHome := os.Getenv("CODEX_HOME")
	if codexHome == "" {
		codexHome = filepath.Join(home, ".codex")
	}
	f, err := os.Open(filepath.Join(codexHome, "config.toml"))
	if err != nil {
		return false
	}
	defer f.Close()
	headers := map[string]bool{`[projects."` + dir + `"]`: true, `[projects.'` + dir + `']`: true}
	in := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "[") {
			in = headers[line]
			continue
		}
		if in && strings.ReplaceAll(line, " ", "") == `trust_level="trusted"` {
			return true
		}
	}
	return false
}

// claudeTrusted: ~/.claude.json projects[dir].hasTrustDialogAccepted.
func claudeTrusted(dir string) bool {
	home, _ := os.UserHomeDir()
	data, err := os.ReadFile(filepath.Join(home, ".claude.json"))
	if err != nil {
		return false
	}
	var cfg struct {
		Projects map[string]struct {
			HasTrustDialogAccepted bool `json:"hasTrustDialogAccepted"`
		} `json:"projects"`
	}
	return json.Unmarshal(data, &cfg) == nil && cfg.Projects[dir].HasTrustDialogAccepted
}

// agyTrusted: ~/.gemini/antigravity-cli/settings.json trustedWorkspaces.
func agyTrusted(dir string) bool {
	home, _ := os.UserHomeDir()
	data, err := os.ReadFile(filepath.Join(home, ".gemini/antigravity-cli/settings.json"))
	if err != nil {
		return false
	}
	var cfg struct {
		TrustedWorkspaces []string `json:"trustedWorkspaces"`
	}
	return json.Unmarshal(data, &cfg) == nil && slices.Contains(cfg.TrustedWorkspaces, dir)
}
