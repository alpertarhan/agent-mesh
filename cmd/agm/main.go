// Command agm is the agent-mesh daemon and CLI.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

const usage = `usage: agm [-as SESSION] <command> [args]

  daemon                          run the broker (auto-started by other commands)
  hello [-name N] [-harness H]    register/rename this session
  list [-json]                    list sessions
  send <to> <text...>             fire-and-forget message
  ask [-timeout 120s] <to> <text...>  send and wait for the reply
  reply <msg-id> <text...>        answer a message
  inbox [-ack] [-json]            show queued messages
  install [harness...]            install adapters (default: every detected harness)
  uninstall <harness...>          remove adapters
  status                          adapter status per harness
  restart                         restart the daemon
  version                         print the version
  spawn [-harness pi] [-name N] [-cwd D] [-focus] <task...>
                                  start an agent in a new herdr tab with a task
                                  (max 8 spawned at once, 2 levels deep)
  hook <harness> [--wait]         hook entry point: crush | claude | codex | agy
                                  (--wait: Claude asyncRewake waiter, exits 2 on mail)

Name: harness session name, $AGM_NAME, or a generated one (swift-otter).
Targets: id, id prefix, name, or name@harness (case-insensitive).
Session id: -as, $AGM_SESSION, or the registered harness process this command runs under.
Socket: $AGM_SOCKET (default ~/.agent-mesh/mesh.sock).
`

func main() {
	as := flag.String("as", os.Getenv("AGM_SESSION"), "session id")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*as, flag.Arg(0), flag.Args()[1:]); err != nil {
		var code exitCode
		if errors.As(err, &code) {
			os.Exit(int(code))
		}
		fmt.Fprintln(os.Stderr, "agm:", err)
		os.Exit(1)
	}
}

func run(as, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	switch cmd {
	case "daemon":
		return daemon()

	case "install", "uninstall", "status":
		return manage(cmd, args)

	case "restart":
		return restart()

	case "version":
		fmt.Println(buildVersion())
		return nil

	case "spawn":
		return spawn(as, args)

	case "hook":
		if len(args) < 1 {
			return errors.New("hook <harness> [--wait]")
		}
		return hook(args[0], args[1:], os.Stdin, os.Stdout, os.Stderr)

	case "list":
		asJSON := fs.Bool("json", false, "json output")
		fs.Parse(args)
		c, err := dial()
		if err != nil {
			return err
		}
		var list []broker.SessionInfo
		if err := c.call(broker.Request{Op: "list"}, &list); err != nil {
			return err
		}
		if *asJSON {
			return json.NewEncoder(os.Stdout).Encode(list)
		}
		for _, s := range list {
			state := "offline"
			if s.Live {
				state = "live"
			}
			fmt.Printf("%-38s %-18s %-9s %-7s %-8s queued=%d %s\n", s.ID, s.Name, s.Harness, state, cmp(s.Pane, "-"), s.Queued, s.Cwd)
		}
		return nil

	case "hello":
		name := fs.String("name", os.Getenv("AGM_NAME"), "display name (default $AGM_NAME)")
		harness := fs.String("harness", "", "harness (pi, omp, opencode, claude, ...)")
		fs.Parse(args)
		cwd, _ := os.Getwd()
		_, err := session(as, broker.SessionInfo{Name: *name, Harness: *harness, Cwd: cwd})
		return err

	case "send", "ask":
		timeout := fs.Duration("timeout", 120*time.Second, "ask timeout")
		fs.Parse(args)
		if fs.NArg() < 2 {
			return errors.New(cmd + " <to> <text...>")
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		req := broker.Request{Op: "send", SendReq: broker.SendReq{To: fs.Arg(0), Text: strings.Join(fs.Args()[1:], " "), ExpectsReply: cmd == "ask"}}
		var m broker.Message
		if err := c.call(req, &m); err != nil {
			return err
		}
		if cmd == "send" {
			fmt.Println(m.ID)
			return nil
		}
		reply, err := c.waitReply(m.ID, *timeout)
		if err != nil {
			return err
		}
		fmt.Println(reply.Text)
		return nil

	case "reply":
		if len(args) < 2 {
			return errors.New("reply <msg-id> <text...>")
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		var m broker.Message
		if err := c.call(broker.Request{Op: "send", SendReq: broker.SendReq{ReplyTo: args[0], Text: strings.Join(args[1:], " ")}}, &m); err != nil {
			return err
		}
		fmt.Println(m.ID)
		return nil

	case "inbox":
		ack := fs.Bool("ack", false, "acknowledge (remove) shown messages")
		asJSON := fs.Bool("json", false, "json output")
		fs.Parse(args)
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		var msgs []*broker.Message
		if err := c.call(broker.Request{Op: "inbox"}, &msgs); err != nil {
			return err
		}
		if *asJSON {
			json.NewEncoder(os.Stdout).Encode(msgs)
		} else {
			for _, m := range msgs {
				kind := "msg"
				if m.ExpectsReply {
					kind = "ASK"
				}
				fmt.Printf("[%s %s] %s: %s\n", kind, m.ID, cmp(m.FromName, m.From), m.Text)
			}
		}
		if !*ack || len(msgs) == 0 {
			return nil
		}
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		return c.call(broker.Request{Op: "ack", IDs: ids}, nil)
	}
	flag.Usage()
	return fmt.Errorf("unknown command %q", cmd)
}

// version is set by release builds (-ldflags "-X main.version=...").
var version = "dev"

func buildVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && version == "dev" && bi.Main.Version != "(devel)" && bi.Main.Version != "" {
		return bi.Main.Version // go install ...@vX.Y.Z
	}
	return version
}

func cmp(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func stateDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Join(home, ".agent-mesh")
}

func socketPath() string {
	if p := os.Getenv("AGM_SOCKET"); p != "" {
		return p
	}
	return filepath.Join(stateDir(), "mesh.sock")
}

func daemon() error {
	sock := socketPath()
	ln, err := broker.Listen(sock)
	if err != nil {
		return err
	}
	defer os.Remove(sock)
	b, err := broker.New(broker.DefaultLimits(), filepath.Join(filepath.Dir(sock), "spool.json"))
	if err != nil {
		return fmt.Errorf("load spool: %w", err)
	}
	w := &waker{b: b, inflight: map[string]bool{}, nudged: map[string]string{}}
	b.Waker = w.wake
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	exe, _ := os.Executable()
	self, _ := os.Stat(exe)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				// Upgraded (file replaced) or uninstalled: exit; the next client starts the
				// new binary. Queued mail is in the spool.
				if now, err := os.Stat(exe); self != nil && (err != nil || !os.SameFile(self, now) || !now.ModTime().Equal(self.ModTime())) {
					log.Printf("binary %s changed, exiting", exe)
					stop()
					return
				}
				if gone := b.Sweep(); len(gone) > 0 {
					log.Printf("gc: removed %v", gone)
				}
				for _, info := range b.Pending() {
					go w.wake(info)
				}
			}
		}
	}()
	log.Printf("agm daemon listening on %s", sock)
	go func() {
		select {
		case <-b.Quit():
			log.Printf("shutdown requested")
			stop()
		case <-ctx.Done():
		}
	}()
	return broker.Serve(ctx, ln, b)
}
