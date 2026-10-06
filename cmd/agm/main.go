// Command agm is the agent-mesh daemon and CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
	"github.com/alpertarhan/agent-mesh/internal/integrations"
)

const usage = `usage: agm [-as SESSION] <command> [args]

  daemon                          run the broker (auto-started by other commands)
  hello [-name N] [-harness H]    register/rename this session
  list [-json]                    list sessions
  whoami [-json]                  the session this command acts as, and why
  resolve [-json] <target>        the session a message to <target> would reach
  send [-ref PATH]... <to> <text...>   fire-and-forget message (prints its id)
  ask [-timeout 120s] [-no-wait] [-ref PATH]... <to> <text...>
                                  send and wait for the reply (prints the reply);
                                  -no-wait prints the question id instead
  wait [-timeout 120s] -reply-to <question-id>
                                  the reply to your question (now, or when it arrives)
  reply [-ref PATH]... <msg-id> <text...>  answer a message
  send-file|ask-file|reply-file [-timeout D] [-ref PATH]... <to|msg-id> <path|->
                                  same, text read from a file or stdin (-)
  ack <msg-id>...                 remove these messages from your queue (full ids)
  inbox [-ack] [-json]            show queued messages
  history [-n 20] [-with PEER] [-thread MSG-ID] [-json]
                                  recent messages you sent or received
  show [-json] <msg-id>           one full message from history
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
  link [-name NAME] [-remote-socket PATH] DEST
                                  serve DEST's remote socket from this daemon
                                  over ssh, restricted to NAME/ sessions (runs until interrupted)
  plugin openclaw -o DIR           write the OpenClaw channel plugin into DIR

Name: harness session name, $AGM_NAME, or a generated one (swift-otter).
Targets: id, id prefix, name, or name@harness (case-insensitive).
-ref sends a file's absolute path, not its content; the receiver reads the current file.
Session id: -as, $AGM_SESSION, or the registered harness process this command runs under.
Flags (-json, -ref, -timeout) go before <to>/<msg-id>: a word after it matching one of
the command's flags (e.g. agm send bob -json) is a usage error, not text. Put --
before <to> to send such a word literally. With -json, errors are {"error":{"code":"...","message":"..."}} on stderr.
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
	explicit := false
	flag.Visit(func(f *flag.Flag) { explicit = explicit || f.Name == "as" })
	if !explicit {
		asSource = "env AGM_SESSION"
	}
	if err := run(*as, flag.Arg(0), flag.Args()[1:]); err != nil {
		var code exitCode
		if errors.As(err, &code) {
			os.Exit(int(code))
		}
		os.Exit(reportError(err, os.Stderr))
	}
}

func run(as, cmd string, args []string) error {
	jsonOut = nil // set by the command's -json flag
	rpcDeadline = time.Time{}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	switch cmd {
	case "daemon":
		return daemon()

	case "install", "uninstall", "status":
		return manage(cmd, args)

	case "restart":
		return restart()

	case "version":
		return printlnOut(buildVersion())

	case "spawn":
		return spawn(as, args)

	case "hook":
		if len(args) < 1 {
			return usageErr("hook <harness> [--wait]")
		}
		return hook(args[0], args[1:], os.Stdin, os.Stdout, os.Stderr)

	case "plugin":
		if len(args) < 1 || args[0] != "openclaw" {
			return usageErr("plugin openclaw -o DIR")
		}
		out := fs.String("o", "", "output directory for the plugin")
		if err := parse(fs, args[1:]); err != nil {
			return err
		}
		if *out == "" || fs.NArg() != 0 {
			return usageErr("plugin openclaw -o DIR")
		}
		if err := integrations.OpenclawPlugin(*out); err != nil {
			return err
		}
		return printlnOut(*out)

	case "link":
		name := fs.String("name", "", "session prefix for the linked host (default DEST when it fits)")
		remoteSocket := fs.String("remote-socket", linkRemoteSock, "remote socket path (relative to the remote home)")
		bridge := fs.Bool("bridge", false, "also mirror sessions between the two daemons")
		localName := fs.String("local-name", "", "prefix for this host's sessions on DEST (default: the short hostname)")
		remoteAgm := fs.String("remote-agm", "agm", "the agm binary on DEST (with -bridge)")
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return usageErr("link [-name NAME] [-remote-socket PATH] [-bridge [-local-name LOCAL] [-remote-agm PATH]] DEST")
		}
		return link(fs.Arg(0), *name, *remoteSocket, linkOpts{bridge: *bridge, localName: *localName, remoteAgm: *remoteAgm})

	case "list":
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageErr("%s takes no arguments, got %q (flags go before arguments)", cmd, fs.Args())
		}
		c, err := dial()
		if err != nil {
			return err
		}
		defer c.nc.Close()
		var list []broker.SessionInfo
		if err := c.call(broker.Request{Op: "list"}, &list); err != nil {
			return err
		}
		if *asJSON {
			return encodeOut(list)
		}
		return writeOut(func(w io.Writer) error { printList(w, os.Stderr, list); return nil })

	case "whoami":
		// Read-only: no hello, so nothing is registered or refreshed.
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageErr("whoami [-json]")
		}
		c, err := dial()
		if err != nil {
			return err
		}
		defer c.nc.Close()
		id, source, err := c.identify(as)
		if err != nil {
			return err
		}
		var list []broker.SessionInfo
		if err := c.call(broker.Request{Op: "list"}, &list); err != nil {
			return err
		}
		who := whoami{ID: id, Source: source}
		for i := range list {
			if list[i].ID == id {
				who.Registered, who.Session = true, &list[i]
			}
		}
		if *asJSON {
			return encodeOut(who)
		}
		return writeOut(func(w io.Writer) error { printWhoami(w, who); return nil })

	case "resolve":
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return usageErr("resolve [-json] <target>")
		}
		c, err := dial()
		if err != nil {
			return err
		}
		defer c.nc.Close()
		if err := c.needProtocol("resolve"); err != nil {
			return err
		}
		var info broker.SessionInfo
		if err := c.call(broker.Request{Op: "resolve", SendReq: broker.SendReq{To: fs.Arg(0)}}, &info); err != nil {
			return err
		}
		if *asJSON {
			return encodeOut(info)
		}
		return writeOut(func(w io.Writer) error { printSession(w, info); return nil })

	case "hello":
		name := fs.String("name", os.Getenv("AGM_NAME"), "display name (default $AGM_NAME)")
		harness := fs.String("harness", "", "harness (pi, omp, opencode, claude, ...)")
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageErr("%s takes no arguments, got %q (flags go before arguments)", cmd, fs.Args())
		}
		cwd, _ := os.Getwd()
		c, err := session(as, broker.SessionInfo{Name: *name, Harness: *harness, Cwd: cwd})
		if err == nil {
			c.nc.Close()
		}
		return err

	case "send", "ask", "reply", "send-file", "ask-file", "reply-file":
		// *-file verbs read a file (or stdin) into the body: separate verbs so harness
		// allowlists can pre-approve plain messaging without granting file reads.
		verb, fromFile := strings.CutSuffix(cmd, "-file")
		timeout := 120 * time.Second
		noWait := new(bool)
		if verb == "ask" {
			fs.DurationVar(&timeout, "timeout", timeout, "ask timeout")
			noWait = fs.Bool("no-wait", false, "print the question id and return; get the answer later with `wait -reply-to ID`")
		}
		refs := refFlag(fs)
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if timeout <= 0 {
			return usageErr("-timeout must be positive, got %s", timeout)
		}
		target := "<to>"
		if verb == "reply" {
			target = "<msg-id>"
		}
		var text string
		var atts []broker.Attachment
		var err error
		if fromFile {
			if fs.NArg() != 2 {
				return usageErr("%s [-json] [-ref PATH]... %s <path|-> (flags go before %s)", cmd, target, target)
			}
			text, atts, err = messageBody(nil, fs.Arg(1), *refs, os.Stdin)
		} else {
			if fs.NArg() < 1 {
				return usageErr("%s [-json] [-ref PATH]... %s [text...]", cmd, target)
			}
			if err := trailingFlag(cmd, fs, args); err != nil {
				return err
			}
			text, atts, err = messageBody(fs.Args()[1:], "", *refs, os.Stdin)
		}
		if err != nil {
			return coded(codeInput, err)
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		req := broker.SendReq{Text: text, Attachments: atts, ExpectsReply: verb == "ask", NoWait: *noWait}
		if verb == "reply" {
			req.ReplyTo = fs.Arg(0)
		} else {
			req.To = fs.Arg(0)
		}
		if len(atts) > 0 || req.NoWait {
			if err := c.needProtocol("-ref / -no-wait"); err != nil {
				return err
			}
		}
		var m broker.Message
		if err := c.call(broker.Request{Op: "send", SendReq: req}, &m); err != nil {
			return err
		}
		if verb != "ask" || *noWait {
			var err error
			if *asJSON {
				err = encodeOut(m)
			} else {
				err = printlnOut(m.ID)
			}
			if err != nil { // the message is queued: do not resend
				return coded(codeOutput, fmt.Errorf("message %s was sent, but printing the result failed: %w", m.ID, err))
			}
			return nil
		}
		if isTTY(os.Stderr) && !*asJSON {
			fmt.Fprintf(os.Stderr, "asked %s (message %s); waiting up to %s for the reply...\n", fs.Arg(0), m.ID, timeout)
		}
		reply, err := c.waitReply(m.ID, timeout)
		if err != nil {
			return err
		}
		return printReply(reply, *asJSON)

	case "wait":
		qid := fs.String("reply-to", "", "id of a question you sent (ask -no-wait)")
		timeout := fs.Duration("timeout", 120*time.Second, "how long to wait")
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if *qid == "" || fs.NArg() != 0 {
			return usageErr("wait [-json] [-timeout 120s] -reply-to <question-id>")
		}
		if *timeout <= 0 {
			return usageErr("-timeout must be positive, got %s", *timeout)
		}
		deadline := time.Now().Add(*timeout) // one budget: identity, hello, lookup and the wait
		rpcDeadline = deadline
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		// The daemon returns an existing reply, or registers this connection and pushes
		// the reply when it arrives. Nothing is acked or removed from the mailbox.
		if err := c.needProtocol("wait"); err != nil {
			return err
		}
		var reply *broker.Message
		if err := c.call(broker.Request{Op: "wait", IDs: []string{*qid}}, &reply); err != nil {
			return err
		}
		if reply == nil {
			if isTTY(os.Stderr) && !*asJSON {
				fmt.Fprintf(os.Stderr, "waiting up to %s for a reply to %s...\n", *timeout, *qid)
			}
			if reply, err = c.waitReplyUntil(*qid, deadline, *timeout); err != nil {
				return err
			}
		}
		return printReply(reply, *asJSON)

	case "history":
		n := fs.Int("n", 20, "number of messages (max 200)")
		with := fs.String("with", "", "only messages between you and this peer")
		thread := fs.String("thread", "", "only the reply thread of this message")
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageErr("%s takes no arguments, got %q (flags go before arguments)", cmd, fs.Args())
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		if err := c.needProtocol("history"); err != nil {
			return err
		}
		var h []broker.Summary
		if err := c.call(broker.Request{Op: "history", Limit: *n, With: *with, Thread: *thread}, &h); err != nil {
			return err
		}
		if *asJSON {
			return encodeOut(h)
		}
		return writeOut(func(w io.Writer) error { printHistory(w, os.Stderr, h); return nil })

	case "show":
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return usageErr("show [-json] <msg-id>")
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		if err := c.needProtocol("show"); err != nil {
			return err
		}
		var m broker.Message
		if err := c.call(broker.Request{Op: "show", IDs: []string{fs.Arg(0)}}, &m); err != nil {
			return err
		}
		if *asJSON {
			return encodeOut(m)
		}
		return writeOut(func(w io.Writer) error { printMessage(w, &m); return nil })

	case "ack":
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			return usageErr("ack [-json] <msg-id>...")
		}
		var ids []string
		for _, id := range fs.Args() {
			if !isMsgID(id) {
				return usageErr("ack needs full 16-character message ids, got %q", id)
			}
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		if err := c.needProtocol("ack receipts"); err != nil {
			return err
		}
		acked := []string{}
		if err := c.call(broker.Request{Op: "ack", IDs: ids}, &acked); err != nil {
			return err
		}
		res := ackResult{Acked: acked, NotQueued: []string{}}
		for _, id := range ids {
			if !slices.Contains(acked, id) {
				res.NotQueued = append(res.NotQueued, id)
			}
		}
		if *asJSON {
			return encodeOut(res)
		}
		for _, id := range res.NotQueued {
			fmt.Fprintf(os.Stderr, "not queued: %s\n", id)
		}
		return writeOut(func(w io.Writer) error {
			for _, id := range res.Acked {
				if _, err := fmt.Fprintln(w, id); err != nil {
					return err
				}
			}
			return nil
		})

	case "inbox":
		ack := fs.Bool("ack", false, "acknowledge (remove) shown messages")
		asJSON := jsonFlag(fs)
		if err := parse(fs, args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return usageErr("%s takes no arguments, got %q (flags go before arguments)", cmd, fs.Args())
		}
		c, err := session(as, broker.SessionInfo{})
		if err != nil {
			return err
		}
		defer c.nc.Close()
		var msgs []*broker.Message
		if err := c.call(broker.Request{Op: "inbox"}, &msgs); err != nil {
			return err
		}
		if *asJSON {
			err = encodeOut(msgs)
		} else {
			err = writeOut(func(w io.Writer) error { printInbox(w, os.Stderr, msgs); return nil })
		}
		if err != nil || !*ack || len(msgs) == 0 {
			return err // not acked unless the messages were really written
		}
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		return c.call(broker.Request{Op: "ack", IDs: ids}, nil)
	}
	flag.Usage()
	return usageErr("unknown command %q", cmd)
}

// printReply prints a reply like `ask`: the text on stdout and attachments on stderr,
// or the complete message with -json.
func printReply(reply *broker.Message, asJSON bool) error {
	if asJSON {
		return encodeOut(reply) // complete message, refs included
	}
	if err := printlnOut(peer(reply.Text)); err != nil { // stdout: the reply text only (script contract)
		return err
	}
	replyExtras(os.Stderr, reply)
	return nil
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
