package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// spawn starts an agent in a new, unfocused herdr tab and gives it a task as its
// first prompt. Limits (count, depth) and the name are decided by the daemon.
func spawn(as string, args []string) error {
	fs := flag.NewFlagSet("spawn", flag.ExitOnError)
	harness := fs.String("harness", "pi", "agent to start: pi, omp, claude, codex, opencode, crush (or another herdr agent kind)")
	name := fs.String("name", "", "mesh name (default: generated)")
	cwd := fs.String("cwd", "", "working directory (default: current)")
	focus := fs.Bool("focus", false, "focus the new tab")
	fs.Parse(args)
	task := strings.Join(fs.Args(), " ")
	if strings.TrimSpace(task) == "" {
		return errors.New(`spawn [-harness pi] [-name N] [-cwd DIR] "<task>"`)
	}
	if os.Getenv("HERDR_ENV") != "1" || os.Getenv("HERDR_WORKSPACE_ID") == "" {
		return errors.New("agm spawn needs herdr: run it from a herdr pane")
	}
	if *cwd == "" {
		*cwd, _ = os.Getwd()
	}
	dir, err := filepath.Abs(*cwd)
	if err != nil {
		return err
	}
	if err := checkTrust(*harness, dir); err != nil {
		return err
	}

	// Parent: the calling session if we can tell, else the user.
	c, err := session(as, broker.SessionInfo{})
	if err != nil {
		if c, err = dial(); err != nil {
			return err
		}
	}
	var res struct {
		Name  string `json:"name"`
		Depth int    `json:"depth"`
	}
	if err := c.call(broker.Request{Op: "spawn", Session: &broker.SessionInfo{Name: *name}}, &res); err != nil {
		return err
	}
	var self broker.SessionInfo // zero if the user spawns from a plain shell
	for _, s := range listSessions(c) {
		if c.id != "" && s.ID == c.id {
			self = s
		}
	}
	parent := cmp(self.Name, "the user")

	var tab struct {
		RootPane struct {
			PaneID string `json:"pane_id"`
		} `json:"root_pane"`
		Tab struct {
			TabID string `json:"tab_id"`
		} `json:"tab"`
	}
	tabArgs := []string{"tab", "create", "--workspace", os.Getenv("HERDR_WORKSPACE_ID"), "--cwd", dir,
		"--label", res.Name, "--env", "AGM_NAME=" + res.Name}
	if !*focus {
		tabArgs = append(tabArgs, "--no-focus")
	}
	if err := herdrJSON(&tab, tabArgs...); err != nil {
		return fmt.Errorf("herdr tab create: %w", err)
	}
	pane := tab.RootPane.PaneID

	// Instructions first: models skip a report step that trails the task.
	prompt := fmt.Sprintf("[agent-mesh] You are %q, an agent spawned by %s via agent-mesh.", res.Name, parent)
	if self.Name != "" {
		prompt += fmt.Sprintf(" Steps: 1) do the task below; 2) deliver the result by running this command with your shell tool (a chat answer does NOT reach %s): %s send %s \"<result>\"; do step 2 also if you are blocked.", self.Name, meshCmd(), self.Name)
	}
	prompt += " Task: " + oneLine(task)

	if err := startAgent(*harness, res.Name, pane); err != nil {
		return err
	}
	if err := typePrompt(*harness, pane, prompt); err != nil {
		return err
	}
	fmt.Printf("spawned %s (%s, depth %d) in herdr pane %s\n", res.Name, *harness, res.Depth, pane)
	return nil
}

func startAgent(harness, name, pane string) error {
	if harness == "crush" { // not a herdr agent kind: type the command
		time.Sleep(time.Second) // let the new shell reach its prompt
		if err := exec.Command("herdr", "pane", "send-text", pane, "crush").Run(); err != nil {
			return err
		}
		if err := exec.Command("herdr", "pane", "send-keys", pane, "Enter").Run(); err != nil {
			return err
		}
		time.Sleep(4 * time.Second)
		return nil
	}
	// The pane must be at a shell prompt; a brand-new tab may not be yet.
	var err error
	for range 10 {
		var out []byte
		out, err = exec.Command("herdr", "agent", "start", name, "--kind", harness, "--pane", pane, "--timeout", "60000").CombinedOutput()
		if err == nil {
			return nil
		}
		err = fmt.Errorf("herdr agent start: %s", strings.TrimSpace(string(out)))
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

func typePrompt(harness, pane, prompt string) error {
	if harness == "crush" {
		if err := exec.Command("herdr", "pane", "send-text", pane, prompt).Run(); err != nil {
			return err
		}
		return exec.Command("herdr", "pane", "send-keys", pane, "Enter").Run()
	}
	// --wait: the prompt must start a turn. A dialog (trust, hook review) shows as
	// "blocked" and would swallow it silently, so only "working" counts.
	out, err := exec.Command("herdr", "agent", "prompt", pane, prompt,
		"--wait", "--until", "working", "--timeout", "20000").CombinedOutput()
	if err != nil {
		return fmt.Errorf("the %s agent in herdr pane %s did not start the task (a dialog may be open there; answer it and type the task): %s",
			harness, pane, strings.TrimSpace(string(out)))
	}
	return nil
}

// herdrJSON runs herdr and decodes its "result" into v.
func herdrJSON(v any, args ...string) error {
	out, err := exec.Command("herdr", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	var r struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		return err
	}
	return json.Unmarshal(r.Result, v)
}

func listSessions(c *client) []broker.SessionInfo {
	var list []broker.SessionInfo
	c.call(broker.Request{Op: "list"}, &list)
	return list
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
