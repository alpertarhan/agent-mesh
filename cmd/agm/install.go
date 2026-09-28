package main

import (
	"errors"
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
	var targets []*integrations.Target
	for _, n := range args {
		t, err := integrations.Find(n)
		if err != nil {
			return err
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		if cmd == "uninstall" {
			return errors.New("uninstall <harness...>")
		}
		for i := range integrations.Targets {
			if t := &integrations.Targets[i]; cmd == "status" || t.Installed(home) {
				targets = append(targets, t)
			}
		}
	}
	bin, _ := binPath()

	var errs []error
	for _, t := range targets {
		switch cmd {
		case "status":
			state := t.Status(home, bin)
			if !t.Installed(home) {
				state += " (harness not found)"
			}
			fmt.Printf("%-10s %s\n", t.Name, state)
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
