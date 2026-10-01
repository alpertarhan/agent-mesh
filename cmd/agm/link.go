package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

// wakesLocally reports the harnesses the daemon wakes on this machine: Codex
// delivery, crush/agy herdr nudges. waker.wake and the link gate share it, so the
// two lists cannot drift apart: a remote hello with one of these would make the
// daemon type into a local pane or deliver mail into a local thread.
func wakesLocally(h string) bool {
	switch h {
	case "codex", "crush", "agy":
		return true
	}
	return false
}

// linkDestRE validates the ssh destination: an alias or user@host. No leading dash
// (option injection), no whitespace, no shell metacharacters. linkNameRE validates
// the session prefix (NAME/): the old alias-only shape.
var (
	linkDestRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._@-]{0,254}$`)
	// linkNameRE caps the prefix at 62 characters: the gate defaults an empty
	// name to the id, and a 63+ character NAME/ prefix would be cut by the
	// daemon's 64-rune name cap before the slash.
	linkNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,61}$`)
)

// linkRemoteSock is the default remote socket path, relative to the remote home.
const linkRemoteSock = ".agent-mesh/link.sock"

// linkNames validates DEST and -name and returns the session prefix: DEST itself
// when it fits, otherwise the required -name flag.
func linkNames(dest, nameFlag string) (string, error) {
	if !linkDestRE.MatchString(dest) {
		return "", usageErr("link DEST (an ssh destination: letters, digits, _, ., @ and -; at most 255 characters)")
	}
	if nameFlag == "" {
		if !linkNameRE.MatchString(dest) {
			return "", usageErr("link -name NAME DEST (%q is not a valid session prefix, so -name is required)", dest)
		}
		return dest, nil
	}
	if !linkNameRE.MatchString(nameFlag) {
		return "", usageErr("link -name NAME (letters, digits, ., _ and -; at most 62 characters)")
	}
	return nameFlag, nil
}

// splitRemotePath splits a remote socket path into dir and base: "a/b.sock" gives
// ("a", "b.sock"), a bare name gives (".", name). An empty base means the path
// is a directory, which is rejected by the caller.
func splitRemotePath(p string) (dir, base string) {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		dir, base = p[:i], p[i+1:]
		if dir == "" {
			dir = "/"
		}
		return dir, base
	}
	return ".", p
}

// linkPreStep builds the pre-step remote command: create the dir under umask 077,
// remove a stale socket (sshd does not unlink it: StreamLocalBindUnlink defaults to
// no), and print the absolute dir. Dir and path are shell-quoted: the remote shell
// parses that string, so they are never embedded raw. It returns the command and
// the socket base name, which is appended to the printed dir for -R.
func linkPreStep(remoteSocket string) (script, base string) {
	dir, base := splitRemotePath(remoteSocket)
	return fmt.Sprintf("umask 077 && mkdir -p -- %s && rm -f -- %s && cd -- %s && pwd -P",
		shellQuote(dir), shellQuote(remoteSocket), shellQuote(dir)), base
}

// maxLinkConns caps concurrent remote connections per link.
const maxLinkConns = 32

// linkDirRE is the only shape accepted for the server's pre-step dir: OpenSSH
// expands ${VAR} from the laptop environment and % tokens (%u, %d, %l), and
// interprets \, [...] and :, in the -R forward spec, so anything else fails the
// attempt. linkPathRE applies the same character set to -remote-socket.
var (
	linkDirRE  = regexp.MustCompile(`^/[A-Za-z0-9._/-]*$`)
	linkPathRE = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
)

// link connects DEST's remote socket to the local daemon through a restricted
// gate: remote clients may only act as NAME/-prefixed sessions, and the gate
// refuses everything that would reach outside that namespace (shutdown, spawn,
// requeue, ref attachments, locally-woken harnesses). It runs until interrupted.
func link(dest, nameFlag, remoteSocket string) error {
	name, err := linkNames(dest, nameFlag)
	if err != nil {
		return err
	}
	script, base := linkPreStep(remoteSocket)
	if base == "" || !linkPathRE.MatchString(remoteSocket) {
		return usageErr("link -remote-socket PATH (letters, digits, ., _, - and /; relative to the remote home; no shell or ssh syntax)")
	}
	sock := filepath.Join(filepath.Dir(socketPath()), "link-"+name+".sock")
	ln, err := broker.Listen(sock)
	if err != nil {
		if strings.Contains(err.Error(), "already running") {
			return fmt.Errorf("a link is already running for %s", name)
		}
		return err
	}
	defer os.Remove(sock)
	fmt.Printf("link %s: serving %s; remote sessions need ids and names starting with %q\n", dest, remoteSocket, name+"/")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go serveLink(ctx, ln, name)
	runLinkSSH(ctx, dest, script, base, sock)
	ln.Close()
	return nil
}

// serveLink accepts remote connections until ln fails, forwarding each to the
// daemon through the gate. At most maxLinkConns run concurrently: over the cap
// the new connection is closed, since a runaway or compromised remote client
// could otherwise open daemon connections without limit. The gate answers nothing
// to clients it never checked.
func serveLink(ctx context.Context, ln net.Listener, name string) {
	sem := make(chan struct{}, maxLinkConns)
	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("link %s: accept: %v", name, err)
			}
			return
		}
		select {
		case sem <- struct{}{}:
			go func() { defer func() { <-sem }(); serveLinkConn(nc, name) }()
		default:
			log.Printf("link %s: too many remote connections", name)
			nc.Close()
		}
	}
}

// serveLinkConn forwards one remote connection to the real daemon through the gate:
// remote requests are checked and re-encoded, daemon frames pass through unchanged.
func serveLinkConn(remote net.Conn, name string) {
	dc, err := dialDaemon()
	if err != nil {
		log.Printf("link %s: daemon dial: %v", name, err)
		remote.Close()
		return
	}
	// Every write to the remote is a whole line under one mutex: the gate's own
	// refusals share the stream with the daemon pump below.
	var wmu sync.Mutex
	writeLine := func(line []byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		_, err := remote.Write(line)
		return err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(dc)
		sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
		for sc.Scan() {
			line := append(append([]byte(nil), sc.Bytes()...), '\n')
			if err := writeLine(line); err != nil {
				break
			}
		}
		remote.Close()
		dc.Close()
	}()
	sc := bufio.NewScanner(remote)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	for sc.Scan() {
		var req broker.Request
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			_ = writeLine(linkRefusal(0, "invalid json: "+err.Error()))
			continue
		}
		checked, rerr := checkLinkRequest(name, &req)
		if rerr != nil {
			_ = writeLine(linkRefusal(req.ID, rerr.Message))
			continue
		}
		// Re-encode without HTML escaping: json.Marshal would grow <, > and & to
		// 6 bytes each and push a valid request over MaxFrame, and the daemon
		// would then drop the connection without an answer. The daemon still
		// measures the message with its own encoding, so genuinely oversized
		// messages get its too_large exactly as for a direct client.
		var enc bytes.Buffer
		jo := json.NewEncoder(&enc)
		jo.SetEscapeHTML(false)
		if err := jo.Encode(checked); err != nil {
			_ = writeLine(linkRefusal(req.ID, "encode: "+err.Error()))
			continue
		}
		// U+2028, U+2029 and invalid UTF-8 still grow when encoded: refuse
		// instead of forwarding a line the daemon's scanner would drop.
		if enc.Len() > broker.MaxFrame {
			_ = writeLine(linkRefusalCode(req.ID, broker.CodeTooLarge, "request too large after re-encoding"))
			continue
		}
		if _, err := dc.Write(enc.Bytes()); err != nil {
			break
		}
	}
	remote.Close()
	dc.Close()
	<-done
}

// linkErr is a gate refusal: bad_request, so clients see it like any daemon error.
func linkErr(format string, a ...any) *broker.Error {
	return &broker.Error{Code: broker.CodeBadRequest, Message: "agm link: " + fmt.Sprintf(format, a...)}
}

func linkRefusal(id int64, msg string) []byte {
	return linkRefusalCode(id, broker.CodeBadRequest, msg)
}

func linkRefusalCode(id int64, code, msg string) []byte {
	if !strings.HasPrefix(msg, "agm link: ") {
		msg = "agm link: " + msg
	}
	data, _ := json.Marshal(broker.Response{ID: id, Error: &broker.Error{Code: code, Message: msg}})
	return append(data, '\n')
}

// checkLinkRequest enforces the gate policy. It returns the request to forward
// (re-encoded by the caller, never the original bytes: unmarshal already dropped
// unknown fields, and hello is rebuilt from its allowed fields only).
func checkLinkRequest(name string, req *broker.Request) (*broker.Request, *broker.Error) {
	prefix := name + "/"
	switch req.Op {
	case "protocol", "list", "resolve", "inbox", "ack", "take", "history", "show", "wait", "bye":
		out := *req // the daemon ignores fields these ops do not read
		return &out, nil
	case "hello":
		if req.Session == nil {
			return nil, linkErr("hello needs a session")
		}
		id := req.Session.ID
		if !strings.HasPrefix(id, prefix) || len(id) == len(prefix) {
			return nil, linkErr("hello id must start with %q", prefix)
		}
		// An empty name would become a generated local-looking name (scarlet-mako)
		// in `agm list`: the id is the safe default. The daemon stores the cleaned
		// name and harness, so the checks run on the cleaned values: "codex\n" must
		// not pass as a harness, and a name that only gains the prefix after cleaning
		// is still that stored name.
		name := req.Session.Name
		if name == "" {
			name = id
		}
		if !strings.HasPrefix(broker.CleanLine(name, 64), prefix) {
			return nil, linkErr("hello name must start with %q", prefix)
		}
		if h := broker.CleanLine(req.Session.Harness, 32); wakesLocally(h) {
			return nil, linkErr("hello harness %q is woken locally; remote sessions may not use it", req.Session.Harness)
		}
		return &broker.Request{
			ID: req.ID, Op: "hello",
			Session: &broker.SessionInfo{
				ID: id, Name: name, Harness: req.Session.Harness, Cwd: req.Session.Cwd,
			},
			Subscribe: req.Subscribe, Wait: req.Wait,
		}, nil
	case "send":
		for _, a := range req.Attachments {
			if a.Type == "ref" {
				return nil, linkErr("ref attachments do not cross hosts; send the content instead")
			}
		}
		out := *req
		return &out, nil
	}
	// shutdown (would stop the laptop daemon), spawn (would run a local agent),
	// requeue (takes client-supplied message objects), and unknown ops.
	return nil, linkErr("op %q is not allowed over agm link", req.Op)
}

// dialDaemon connects to the real daemon, starting it if the socket is not
// answering. Unlike dial, the connection lives long: no RPC deadline.
func dialDaemon() (net.Conn, error) {
	sock := socketPath()
	nc, err := net.Dial("unix", sock)
	if err != nil {
		if err := startDaemon(sock); err != nil {
			return nil, err
		}
		for i := 0; i < 40 && err != nil; i++ {
			time.Sleep(50 * time.Millisecond)
			nc, err = net.Dial("unix", sock)
		}
		if err != nil {
			return nil, fmt.Errorf("daemon did not come up on %s: %w", sock, err)
		}
	}
	return nc, nil
}

// linkSleep waits out a backoff, reporting false if ctx ended first; a var so tests
// shorten it.
var linkSleep = func(d time.Duration, ctx context.Context) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// runLinkSSH keeps the reverse forward up until ctx ends: pre-step, then ssh -R,
// then backoff (1 s, doubling, capped at 30 s; a connection that lasted at least a
// minute resets it to 1 s).
func runLinkSSH(ctx context.Context, dest, script, base, localSock string) {
	backoff := time.Second
	for ctx.Err() == nil {
		if dur := linkSession(ctx, dest, script, base, localSock); dur >= time.Minute {
			backoff = time.Second
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if !linkSleep(backoff, ctx) {
			return
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// linkSession runs the pre-step and one ssh -R session, and reports how long the
// forwarding connection lasted (0 if it never came up).
func linkSession(ctx context.Context, dest, script, base, localSock string) time.Duration {
	home, err := linkHome(ctx, dest, script)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("link %s: %v", dest, err)
		}
		return 0
	}
	if !linkDirRE.MatchString(home) {
		if ctx.Err() == nil {
			short := home
			if len(short) > 200 {
				short = short[:200]
			}
			log.Printf("link %s: refused remote dir %q", dest, short)
		}
		return 0
	}
	remote := home + "/" + base
	start := time.Now()
	err = linkRun(ctx, dest, remote, localSock)
	if ctx.Err() != nil {
		return 0
	}
	if err != nil {
		log.Printf("link %s: ssh exited: %v", dest, err)
	} else {
		log.Printf("link %s: ssh exited", dest)
	}
	return time.Since(start)
}

// linkPreStepTimeout bounds one pre-step; a var so tests shorten it.
var linkPreStepTimeout = 30 * time.Second

// linkHome runs the pre-step and returns the absolute remote dir for the -R path.
// It parses the last non-empty output line: sshd runs the command through bash,
// and an echo in ~/.bashrc would otherwise break the parse and retry forever.
func linkHome(ctx context.Context, dest, script string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, linkPreStepTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "--", dest, script)
	// The pipes are tailBuffers, so Wait would otherwise wait for every holder of
	// them to exit: a lingering ssh child (ProxyCommand, "sleep 20 & wait") would
	// stretch the timeout and SIGINT by its lifetime.
	cmd.WaitDelay = 3 * time.Second
	var stdout tailBuffer
	stdout.n = 4 << 10
	cmd.Stdout = &stdout
	var stderr tailBuffer
	stderr.n = 32 << 10
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		msg := err.Error()
		if trimmed := strings.TrimSpace(string(stderr.buf)); trimmed != "" {
			msg += ": " + broker.CleanLine(trimmed, 300)
		}
		return "", fmt.Errorf("pre-step: %s", msg)
	}
	home := strings.TrimSpace(string(stdout.buf))
	if i := strings.LastIndex(home, "\n"); i >= 0 {
		home = strings.TrimSpace(home[i+1:])
	}
	if !strings.HasPrefix(home, "/") || strings.ContainsAny(home, "\n\r") {
		return "", fmt.Errorf("unexpected pre-step output %q", home)
	}
	return home, nil
}

// tailBuffer keeps the last n bytes written: the pre-step answer is its last
// line, and a chatty server must not grow the process without bound.
type tailBuffer struct {
	buf []byte
	n   int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.n {
		t.buf = t.buf[len(t.buf)-t.n:]
	}
	return len(p), nil
}

// linkRun holds the reverse forward until ssh exits or ctx ends.
func linkRun(ctx context.Context, dest, remote, localSock string) error {
	cmd := exec.CommandContext(ctx, "ssh",
		"-N", "-T",
		"-o", "BatchMode=yes",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-R", remote+":"+localSock,
		"--", dest)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
