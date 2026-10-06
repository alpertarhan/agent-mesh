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
	"runtime"
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

// checkSockPath rejects a socket path that cannot fit sockaddr_un's sun_path
// (104 bytes with the terminator on macOS, 108 on Linux): ssh would fail the
// bind deep inside its retry loop, so fail up front with a clear error. One
// helper for link-NAME.sock and link-NAME-remote.sock.
func checkSockPath(p string) error {
	max := 103 // macOS: sun_path is 104 including the NUL
	if runtime.GOOS == "linux" {
		max = 107
	}
	if len(p) > max {
		return fmt.Errorf("socket path %s is %d bytes; a unix socket path is limited to %d on %s - use a shorter -name", p, len(p), max+1, runtime.GOOS)
	}
	return nil
}

// shortHost is the -local-name default: the short hostname, cut at the first
// dot. It must still match linkNameRE; if it does not, -local-name is
// required.
func shortHost() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return shortHostName(h)
}

// shortHostName cuts a hostname at its first dot: "foo.bar.local" is "foo".
func shortHostName(h string) string {
	if i := strings.Index(h, "."); i >= 0 {
		return h[:i]
	}
	return h
}

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
// no), and print the absolute dir. With -bridge it also brings DEST's daemon up
// (a plain dial auto-starts it) and asks the remote agm for its version and its
// daemon's socket, each on its own line after the dir, for the -L forward; the
// version is a log line at most - the gate on what the daemons can do is the
// bridge's capability check, not the binary. Dir and paths are shell-quoted:
// the remote shell parses that string, so they are never embedded raw. It
// returns the command and the socket base name, which is appended to the
// printed dir for -R.
func linkPreStep(remoteSocket string, opts linkOpts) (script, base string) {
	dir, base := splitRemotePath(remoteSocket)
	script = fmt.Sprintf("umask 077 && mkdir -p -- %s && rm -f -- %s && cd -- %s && pwd -P",
		shellQuote(dir), shellQuote(remoteSocket), shellQuote(dir))
	if opts.bridge {
		agm := shellQuote(opts.remoteAgm)
		script += fmt.Sprintf(" && %s list >/dev/null 2>&1; %s version 2>&1; %s status -json 2>&1", agm, agm, agm)
	}
	return script, base
}

// linkPreResult is what the extended pre-step answers: the absolute dir, the
// remote agm's version (informational) and DEST's daemon socket for -L.
type linkPreResult struct {
	home    string
	version string
	socket  string
}

// parsePreStep reads the pre-step output: from the end, the status json, the
// version line, then the dir. Output before them is tolerated (an echo in
// ~/.bashrc; the tailBuffer keeps the last 4 KiB), but the three answers
// themselves must be there and well-formed: the socket absolute and in the
// dir charset, the dir absolute and in the dir charset. The version line is
// whatever the remote agm printed (even an error line); it is never trusted.
//
// A dir that parses but extended answers that do not yields a PARTIAL result
// (home set, an error): the gate's -R can still come up while DEST's agm is
// missing or broken - the bridge alone waits. A dir that does not parse is a
// hard failure: nothing can be forwarded.
func parsePreStep(out []byte) (linkPreResult, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	keep := lines
	for len(keep) > 0 && strings.TrimSpace(keep[len(keep)-1]) == "" {
		keep = keep[:len(keep)-1]
	}
	dirOK := func(s string) bool {
		s = strings.TrimSpace(s)
		return strings.HasPrefix(s, "/") && linkDirRE.MatchString(s) && !strings.ContainsAny(s, "\n\r")
	}
	var home string
	if len(keep) >= 3 {
		home = strings.TrimSpace(keep[len(keep)-3])
	} else if len(keep) == 1 && dirOK(keep[0]) {
		home = strings.TrimSpace(keep[0]) // the dir printed; the agm answers did not
	}
	var st struct {
		Daemon struct {
			Socket string `json:"socket"`
		} `json:"daemon"`
	}
	if len(keep) >= 3 && json.Unmarshal([]byte(keep[len(keep)-1]), &st) == nil {
		socket := st.Daemon.Socket
		if !strings.HasPrefix(socket, "/") || !linkDirRE.MatchString(socket) {
			if dirOK(home) {
				return linkPreResult{home: home}, fmt.Errorf("pre-step: DEST's daemon socket %q is not absolute or not a plain path", socket)
			}
			return linkPreResult{}, fmt.Errorf("pre-step: DEST's daemon socket %q is not absolute or not a plain path", socket)
		}
		if !dirOK(home) {
			return linkPreResult{}, fmt.Errorf("unexpected pre-step output %q", home)
		}
		return linkPreResult{home: home, version: strings.TrimSpace(keep[len(keep)-2]), socket: socket}, nil
	}
	if dirOK(home) {
		if len(keep) >= 3 {
			return linkPreResult{home: home}, fmt.Errorf("pre-step: agm status -json did not answer with a socket")
		}
		return linkPreResult{home: home}, fmt.Errorf("pre-step output too short (%d line(s); DEST's agm did not answer)", len(keep))
	}
	return linkPreResult{}, fmt.Errorf("pre-step output too short (%d line(s); DEST's agm did not answer)", len(keep))
}

// maxLinkConns caps concurrent remote connections per link.
const maxLinkConns = 32

// linkDirRE is the only shape accepted for the server's pre-step dir: OpenSSH
// expands ${VAR} from the local environment and % tokens (%u, %d, %l), and
// interprets \, [...] and :, in the -R forward spec, so anything else fails the
// attempt. linkPathRE applies the same character set to -remote-socket.
var (
	linkDirRE  = regexp.MustCompile(`^/[A-Za-z0-9._/-]*$`)
	linkPathRE = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
)

// linkOpts carries the -bridge wiring: the flag itself, the prefix for this
// host's sessions on DEST, and the agm binary on DEST for the pre-step.
type linkOpts struct {
	bridge    bool
	localName string
	remoteAgm string
}

// link connects DEST's remote socket to the local daemon through a restricted
// gate: remote clients may only act as NAME/-prefixed sessions, and the gate
// refuses everything that would reach outside that namespace (shutdown, spawn,
// requeue, ref attachments, locally-woken harnesses). With -bridge, the same
// process also mirrors sessions between the two daemons: the bridge connects
// to the local daemon directly and to DEST's daemon through a -L forward on
// the same ssh. It runs until interrupted.
func link(dest, nameFlag, remoteSocket string, opts linkOpts) error {
	name, err := linkNames(dest, nameFlag)
	if err != nil {
		return err
	}
	script, base := linkPreStep(remoteSocket, opts)
	if base == "" || !linkPathRE.MatchString(remoteSocket) {
		return usageErr("link -remote-socket PATH (letters, digits, ., _, - and /; relative to the remote home; no shell or ssh syntax)")
	}
	if opts.bridge && opts.remoteAgm == "" {
		return usageErr("link -remote-agm PATH (the agm binary on DEST)")
	}
	dir := filepath.Dir(socketPath())
	sock := filepath.Join(dir, "link-"+name+".sock")
	if err := checkSockPath(sock); err != nil {
		return err
	}
	var br *Bridge
	if opts.bridge {
		local := opts.localName
		if local == "" {
			local = shortHost()
		}
		if !linkNameRE.MatchString(local) {
			return usageErr("link -local-name LOCAL (this host's short name %q is not a valid session prefix, so -local-name is required)", local)
		}
		localRemote := filepath.Join(dir, "link-"+name+"-remote.sock")
		if err := checkSockPath(localRemote); err != nil {
			return err
		}
		logf := func(format string, a ...any) {
			log.Printf("link %s: "+format, append([]any{name}, a...)...)
		}
		logf("bridge: mirroring %s's sessions as %q here and this host's as %q there, over %s", dest, name+"/", local+"/", localRemote)
		br = NewBridge(socketPath(), localRemote, local+"/", name+"/", filepath.Join(dir, "link-"+name+".map"), logf)
	}
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
	if br != nil {
		br.startCtx(ctx) // pre-wired here: Stop in the deferred call can never race Run wiring it
		go br.Run(ctx)
		defer br.Stop() // the bridge stops before the listener: its workers exit first
	}
	go serveLink(ctx, ln, name, br) // Accept unblocks when ln.Close runs below
	var localRemote string
	if br != nil {
		localRemote = br.sockets[1]
	}
	runLinkSSH(ctx, dest, script, base, sock, localRemote, br != nil)
	ln.Close()
	return nil
}

// serveLink accepts remote connections until ln fails, forwarding each to the
// daemon through the gate. At most maxLinkConns run concurrently: over the cap
// the new connection is closed, since a runaway or compromised remote client
// could otherwise open daemon connections without limit. The gate answers nothing
// to clients it never checked.
func serveLink(ctx context.Context, ln net.Listener, name string, br *Bridge) {
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
			go func() { defer func() { <-sem }(); serveLinkConn(nc, name, br) }()
		default:
			log.Printf("link %s: too many remote connections", name)
			nc.Close()
		}
	}
}

// serveLinkConn forwards one remote connection to the real daemon through the gate:
// remote requests are checked and re-encoded, daemon frames pass through unchanged.
// With a bridge in the same process, a connection's FIRST hello claims its id
// against the bridge's mirrored rows (the NAME/ overlap) and the connection
// keeps that id for its life; the claim is released exactly once, when the
// connection ends.
func serveLinkConn(remote net.Conn, name string, br *Bridge) {
	held := "" // the id this connection holds a claim on; set once
	defer func() {
		if br != nil && held != "" {
			br.gateRelease(1, held)
		}
	}()
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
		checked, claim, rerr := checkLinkRequest(name, &req, br, held)
		if rerr != nil {
			_ = writeLine(linkRefusal(req.ID, rerr.Message))
			continue
		}
		if claim != "" {
			held = claim // set on the first accepted hello; never moves after
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
func checkLinkRequest(name string, req *broker.Request, br *Bridge, held string) (*broker.Request, string, *broker.Error) {
	prefix := name + "/"
	switch req.Op {
	case "protocol", "list", "resolve", "inbox", "ack", "take", "history", "show", "wait", "bye":
		out := *req // the daemon ignores fields these ops do not read
		return &out, "", nil
	case "hello":
		if req.Session == nil {
			return nil, "", linkErr("hello needs a session")
		}
		id := req.Session.ID
		if !strings.HasPrefix(id, prefix) || len(id) == len(prefix) {
			return nil, "", linkErr("hello id must start with %q", prefix)
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
			return nil, "", linkErr("hello name must start with %q", prefix)
		}
		if h := broker.CleanLine(req.Session.Harness, 32); wakesLocally(h) {
			return nil, "", linkErr("hello harness %q is woken locally; remote sessions may not use it", req.Session.Harness)
		}
		claim := ""
		if br != nil {
			// NAME/ overlap: the bridge mirrors DEST's sessions under the same
			// prefix this gate serves. First claim wins, under the bridge's
			// mutex: a hello that races a proxy creation either sees the row
			// tracked and is refused, or claims the id and the creation skips.
			// Rows left by earlier processes are the bridge's list check.
			//
			// One id per connection: a same-id refresh keeps its holder count
			// (no second increment), and a different id is refused HERE,
			// before any claim moves - the daemon would refuse the rebind
			// anyway ("connection already bound"), and releasing the old
			// claim before that refusal would leave a live connection
			// holding an id nothing tracks.
			bare := strings.TrimPrefix(id, prefix)
			if held != "" && bare != held {
				return nil, "", linkErr("this connection is bound to %s; reconnect to change the id", prefix+held)
			}
			if bare != held && !br.gateClaim(1, bare) {
				return nil, "", linkErr("hello id %s is a mirrored session on this link; pick another id", id)
			}
			claim = bare
		}
		return &broker.Request{
			ID: req.ID, Op: "hello",
			Session: &broker.SessionInfo{
				ID: id, Name: name, Harness: req.Session.Harness, Cwd: req.Session.Cwd,
			},
			Subscribe: req.Subscribe, Wait: req.Wait,
		}, claim, nil
	case "send":
		for _, a := range req.Attachments {
			if a.Type == "ref" {
				return nil, "", linkErr("ref attachments do not cross hosts; send the content instead")
			}
		}
		out := *req
		return &out, "", nil
	}
	// shutdown (would stop the local daemon), spawn (would run a local agent),
	// requeue (takes client-supplied message objects), and unknown ops.
	return nil, "", linkErr("op %q is not allowed over agm link", req.Op)
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
func runLinkSSH(ctx context.Context, dest, script, base, localSock, localRemote string, bridge bool) {
	backoff := time.Second
	for ctx.Err() == nil {
		if dur := linkSession(ctx, dest, script, base, localSock, localRemote, bridge); dur >= time.Minute {
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

// linkSession runs the pre-step and one ssh session carrying the -R gate
// forward and, with -bridge, the -L daemon forward, and reports how long the
// forwarding connection lasted (0 if it never came up). The bridge itself is
// NOT restarted here: it runs for the link's whole life and rides out ssh
// restarts by treating the far side as down until -L answers again.
func linkSession(ctx context.Context, dest, script, base, localSock, localRemote string, bridge bool) time.Duration {
	home, sock, version, extErr, err := linkHome(ctx, dest, script, bridge)
	if err == nil && !linkDirRE.MatchString(home) {
		err = fmt.Errorf("refused remote dir %q", home)
	}
	if err != nil {
		if ctx.Err() == nil {
			logLinkErr(dest, err) // once per change, not once per retry (B6)
		}
		return 0
	}
	bridgeUp := bridge
	if extErr != nil {
		// The dir answered but DEST's agm did not: the gate's -R still comes
		// up (agm-less clients depend on it); only the bridge waits, and the
		// retry loop re-runs the full pre-step.
		bridgeUp = false
		if ctx.Err() == nil {
			logLinkErr(dest, extErr)
		}
	} else if ctx.Err() == nil {
		logLinkErr(dest, nil) // back to normal: silence, and remember it
	}
	if bridge && version != "" && version != lastLinkVersion(dest) {
		setLinkVersion(dest, version)
		// the version line is untrusted remote output: bounded and stripped
		// of control sequences before it reaches the log (B5)
		log.Printf("link %s: DEST runs agm %s", dest, broker.CleanLine(version, 64))
	}
	remote := home + "/" + base
	start := time.Now()
	if !bridgeUp {
		// Gate-only mode must not strand the bridge: the -R session stays up,
		// but the full pre-step keeps being retried beside it, and the moment
		// DEST's agm answers again the -R-only session is torn down (a brief
		// gate blip) so the outer loop can bring up BOTH forwards at once.
		gctx, gcancel := context.WithCancel(ctx)
		sshDone := make(chan error, 1)
		go func() { sshDone <- linkRun(gctx, dest, remote, localSock, "", "", false) }()
		poll := time.NewTicker(5 * time.Second)
		defer poll.Stop()
		for {
			select {
			case <-ctx.Done():
				gcancel()
				<-sshDone
				return 0
			case rerr := <-sshDone:
				// the -R session died: the gate is down, so let the outer loop
				// restart it (with the full pre-step, and -L if agm answers)
				gcancel()
				if rerr != nil {
					log.Printf("link %s: ssh exited: %v", dest, rerr)
				} else {
					log.Printf("link %s: ssh exited", dest)
				}
				return time.Since(start)
			case <-poll.C:
				if _, sock2, _, ext2, err2 := linkHome(ctx, dest, script, true); err2 == nil && ext2 == nil {
					_ = sock2
					logLinkErr(dest, nil)
					gcancel()
					<-sshDone
					return time.Since(start)
				} else if ctx.Err() == nil {
					if ext2 != nil {
						logLinkErr(dest, ext2)
					} else if err2 != nil {
						logLinkErr(dest, err2)
					}
				}
			}
		}
	}
	err = linkRun(ctx, dest, remote, localSock, localRemote, sock, bridgeUp)
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

// linkErrMu guards the per-DEST last-logged pre-step problem, so a retrying
// link logs a persisting failure once per change instead of once per attempt.
var (
	linkErrMu  sync.Mutex
	linkErrStr = map[string]string{}
)

func logLinkErr(dest string, err error) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	linkErrMu.Lock()
	prev, had := linkErrStr[dest]
	linkErrStr[dest] = msg
	linkErrMu.Unlock()
	if msg == prev {
		return
	}
	if !had || msg == "" {
		if msg == "" {
			return // recovery says nothing; the next line speaks
		}
	}
	if msg != "" {
		log.Printf("link %s: %s", dest, msg)
	}
}

// linkPreStepTimeout bounds one pre-step; a var so tests shorten it.
var linkPreStepTimeout = 30 * time.Second

// linkVersionMu guards the per-DEST last-seen remote version, so the log line
// fires once per change, not once per retry.
var (
	linkVersionMu sync.Mutex
	linkVersions  = map[string]string{}
)

func lastLinkVersion(dest string) string {
	linkVersionMu.Lock()
	defer linkVersionMu.Unlock()
	return linkVersions[dest]
}

func setLinkVersion(dest, v string) {
	linkVersionMu.Lock()
	defer linkVersionMu.Unlock()
	linkVersions[dest] = v
}

// linkHome runs the pre-step and returns the absolute remote dir for the -R
// path, DEST's daemon socket for the -L forward, and the remote agm's version
// line ("" without -bridge). The pre-step output is untrusted and parsed from
// its end (see parsePreStep): sshd runs the command through bash, and an echo
// in ~/.bashrc must not break the parse and retry forever.
func linkHome(ctx context.Context, dest, script string, bridge bool) (home, sock, version string, extErr, err error) {
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
	err = cmd.Run()
	if err != nil {
		msg := err.Error()
		if trimmed := strings.TrimSpace(string(stderr.buf)); trimmed != "" {
			msg += ": " + broker.CleanLine(trimmed, 300)
		}
		return "", "", "", nil, fmt.Errorf("pre-step: %s", msg)
	}
	if !bridge {
		h := strings.TrimSpace(string(stdout.buf))
		if i := strings.LastIndex(h, "\n"); i >= 0 {
			h = strings.TrimSpace(h[i+1:])
		}
		if !strings.HasPrefix(h, "/") || strings.ContainsAny(h, "\n\r") {
			return "", "", "", nil, fmt.Errorf("unexpected pre-step output %q", h)
		}
		return h, "", "", nil, nil
	}
	pre, perr := parsePreStep(stdout.buf)
	if perr != nil {
		if pre.home != "" {
			// partial: the gate's -R can run; only the bridge waits
			return pre.home, "", "", perr, nil
		}
		if trimmed := strings.TrimSpace(string(stderr.buf)); trimmed != "" {
			return "", "", "", nil, fmt.Errorf("%s: %s", perr, broker.CleanLine(trimmed, 300))
		}
		return "", "", "", nil, perr
	}
	return pre.home, pre.socket, pre.version, nil, nil
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

// linkSSHArgs builds the ssh argument vector: the -R gate forward always; with
// the bridge, the -L daemon forward on the same multiplexed connection.
// StreamLocalBindUnlink=yes replaces a stale local -L socket left by the
// previous run (the -R side is cleaned by the pre-step: sshd does not unlink).
func linkSSHArgs(dest, remote, localSock, localRemote, serverSock string, bridge bool) []string {
	args := []string{
		"-N", "-T",
		"-o", "BatchMode=yes",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-R", remote + ":" + localSock,
	}
	if bridge {
		args = append(args,
			"-o", "StreamLocalBindUnlink=yes",
			"-L", localRemote+":"+serverSock)
	}
	return append(args, "--", dest)
}

// linkRun holds the forwards until ssh exits or ctx ends.
func linkRun(ctx context.Context, dest, remote, localSock, localRemote, serverSock string, bridge bool) error {
	cmd := exec.CommandContext(ctx, "ssh", linkSSHArgs(dest, remote, localSock, localRemote, serverSock, bridge)...)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
