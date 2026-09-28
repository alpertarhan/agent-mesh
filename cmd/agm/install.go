package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

func manage(cmd string, args []string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	var asJSON *bool
	if cmd == "status" {
		fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
		asJSON = jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		args = fs.Args()
	}
	var targets []*integrations.Target
	for _, n := range args {
		t, err := integrations.Find(n)
		if err != nil {
			return coded(codeUsage, err)
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		if cmd == "uninstall" {
			return usageErr("uninstall <harness...>")
		}
		for i := range integrations.Targets {
			if t := &integrations.Targets[i]; cmd == "status" || t.Installed(home) {
				targets = append(targets, t)
			}
		}
	}
	bin, _ := binPath()

	if cmd == "status" && *asJSON {
		return encodeOut(statusOf(home, bin, targets))
	}
	if cmd == "status" {
		// Installed config ≠ connected: status never starts the daemon.
		d := "not running (starts on the first agm command or adapter)"
		if daemonRunning() {
			d = "running on " + socketPath()
		}
		fmt.Printf("daemon       %s\n", d)
		fmt.Printf("%-12s %-34s %s\n", "HARNESS", "ADAPTER", "DELIVERY WHEN INSTALLED")
	}
	var errs []error
	for _, t := range targets {
		switch cmd {
		case "status":
			state := t.Status(home, bin)
			if !t.Installed(home) {
				state += " (harness not found)"
			}
			fmt.Printf("%-12s %-34s %s\n", t.Name, state, delivery[t.Name])
			continue
		case "uninstall":
			err = t.Uninstall(home)
		case "install":
			err = t.Install(home, bin)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", t.Name, err))
			continue
		}
		fmt.Printf("%-10s %sed\n", t.Name, cmd)
	}
	if cmd == "install" && daemonRunning() {
		if err := restart(); err != nil {
			errs = append(errs, fmt.Errorf("daemon restart: %w", err))
		} else {
			fmt.Println("daemon     restarted")
		}
	}
	return errors.Join(errs...)
}

func daemonRunning() bool {
	c, err := net.Dial("unix", socketPath())
	if err == nil {
		c.Close()
	}
	return err == nil
}

// delivery explains how mail reaches each harness; see `agm list` for live sessions.
var delivery = map[string]string{
	"pi":           "push while running (card, /mesh, quiet mode)",
	"omp":          "push while running (card, /mesh, quiet mode)",
	"opencode":     "push to the selected session via its server",
	"claude":       "hooks: each turn; idle wake via Stop --wait hook",
	"codex":        "hooks: each turn; idle wake via app-server",
	"crush":        "hooks: each tool call; idle wake needs herdr",
	"agy":          "hooks: each model call; idle wake needs herdr",
	"skill":        "instructions only",
	"claude-skill": "instructions only",
}

type statusJSON struct {
	Daemon struct {
		Running bool   `json:"running"`
		Socket  string `json:"socket"`
	} `json:"daemon"`
	Targets []targetStatus `json:"targets"`
}

type targetStatus struct {
	Name         string `json:"name"`
	Adapter      string `json:"adapter"` // current | outdated | not installed | ...
	HarnessFound bool   `json:"harness_found"`
	Delivery     string `json:"delivery"`
}

// statusOf is `status -json`: install state per target and whether the daemon
// answers (it is never started). Installed does not mean connected: see `agm list`.
func statusOf(home, bin string, targets []*integrations.Target) statusJSON {
	var st statusJSON
	st.Daemon.Running, st.Daemon.Socket = daemonRunning(), socketPath()
	st.Targets = []targetStatus{}
	for _, t := range targets {
		st.Targets = append(st.Targets, targetStatus{t.Name, t.Status(home, bin), t.Installed(home), delivery[t.Name]})
	}
	return st
}
