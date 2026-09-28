package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alpertarhan/agent-mesh/internal/broker"
)

type client struct {
	id     string // session id after session(); empty for anonymous connections
	nc     net.Conn
	sc     *bufio.Scanner
	nextID int64
	events []*broker.Message // pushed while waiting for a response
	proto  int               // daemon protocol, once checked
}

// rpcDeadline, if set (by `wait -timeout`), bounds all I/O on connections dialed
// afterwards. Connecting and starting the daemon are not covered.
var rpcDeadline time.Time

// dial connects to the daemon, starting it if the socket is not answering.
func dial() (*client, error) {
	sock := socketPath()
	nc, err := net.Dial("unix", sock)
	if err != nil {
		if err := startDaemon(sock); err != nil {
			return nil, coded(codeUnavailable, err)
		}
		for i := 0; i < 40 && err != nil; i++ {
			time.Sleep(50 * time.Millisecond)
			nc, err = net.Dial("unix", sock)
		}
		if err != nil {
			return nil, coded(codeUnavailable, fmt.Errorf("daemon did not come up on %s: %w", sock, err))
		}
	}
	if !rpcDeadline.IsZero() { // every RPC on this connection, identity lookup and hello included
		nc.SetDeadline(rpcDeadline)
	}
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 64<<10), broker.MaxFrame)
	return &client{nc: nc, sc: sc}, nil
}

func startDaemon(sock string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(filepath.Dir(sock), "daemon.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	cmd := exec.Command(exe, "daemon")
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}

// restart stops a running daemon and starts one from this binary. Subscribed
// adapters reconnect on their own; queued mail survives in the spool.
func restart() error {
	sock := socketPath()
	if nc, err := net.Dial("unix", sock); err == nil {
		nc.Write([]byte(`{"id":1,"op":"shutdown"}` + "\n"))
		nc.SetReadDeadline(time.Now().Add(3 * time.Second))
		io.Copy(io.Discard, nc) // returns when the daemon closes the connection
		nc.Close()
		stopped := false
		for i := 0; i < 40 && !stopped; i++ { // wait for the listener to go away
			c, err := net.Dial("unix", sock)
			if stopped = err != nil; !stopped {
				c.Close()
				time.Sleep(50 * time.Millisecond)
			}
		}
		if !stopped {
			return errors.New("daemon did not stop (older version?); stop it with: pkill -f 'agm daemon'")
		}
	}
	c, err := dial()
	if err != nil {
		return err
	}
	c.nc.Close()
	return nil
}

// asSource says where a non-empty -as value came from (main sets it).
var asSource = "flag -as"

// identify picks the session id this command acts as, and where it came from:
// -as/$AGM_SESSION, $CODEX_THREAD_ID, $ANTIGRAVITY_CONVERSATION_ID, or process ancestry.
// It does not register anything.
func (c *client) identify(as string) (string, string, error) {
	if as != "" {
		return as, asSource, nil
	}
	// Codex runs shell commands under a shared app-server daemon, so ancestor PIDs
	// cannot tell its sessions apart; it exports the thread id (= hook session_id).
	// Antigravity exports its conversation id the same way.
	for _, env := range []string{"CODEX_THREAD_ID", "ANTIGRAVITY_CONVERSATION_ID"} {
		if id := os.Getenv(env); id != "" {
			return id, "env " + env, nil
		}
	}
	id, pid, err := c.selfID()
	if err != nil {
		return "", "", err
	}
	return id, fmt.Sprintf("process ancestry (harness pid %d)", pid), nil
}

// session dials and says hello as the identified session.
func session(id string, info broker.SessionInfo) (*client, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	if id, _, err = c.identify(id); err != nil {
		c.nc.Close()
		return nil, err
	}
	info.ID = id
	if err := c.call(broker.Request{Op: "hello", Session: &info}, nil); err != nil {
		c.nc.Close()
		return nil, err
	}
	c.id = id
	return c, nil
}

// needProtocol fails with daemon_outdated if the running daemon predates broker.Protocol:
// an older daemon silently ignores newer request fields (filters, refs, no_wait) or ops.
// Call it before sending such a request. Nothing is restarted automatically.
func (c *client) needProtocol(feature string) error {
	if c.proto >= broker.Protocol {
		return nil
	}
	var r struct{ Protocol int }
	err := c.call(broker.Request{Op: "protocol"}, &r)
	var be *broker.Error
	if errors.As(err, &be) && (be.Code == broker.CodeBadRequest || be.Code == broker.CodeNotRegistered) {
		// A daemon from before the protocol op: "unknown op" after hello, or
		// not_registered before it (anonymous commands such as resolve). Timeouts and
		// transport errors are not answers and stay what they are.
		r.Protocol, err = 1, nil
	}
	if err != nil {
		return err
	}
	if c.proto = r.Protocol; c.proto < broker.Protocol {
		exe, _ := os.Executable()
		return coded(codeOutdated, fmt.Errorf("the running daemon (protocol %d) is older than this agm (protocol %d) and does not support %s; restart it with this binary: %s restart", r.Protocol, broker.Protocol, feature, shellQuote(cmp(exe, "agm"))))
	}
	return nil
}

// ponytail: if one harness process hosts several sessions (crush can switch sessions),
// the most recently seen one wins; pass -as when that is ambiguous.
func (c *client) selfID() (string, int, error) {
	var list []broker.SessionInfo
	if err := c.call(broker.Request{Op: "list"}, &list); err != nil {
		return "", 0, err
	}
	for _, p := range ancestors() {
		pid := p.pid
		var best *broker.SessionInfo
		for i := range list {
			if s := &list[i]; s.PID == pid && (best == nil || s.LastSeen.After(best.LastSeen)) {
				best = s
			}
		}
		if best != nil {
			return best.ID, pid, nil
		}
	}
	return "", 0, coded(codeNoIdentity, errors.New("no session id: pass -as, set AGM_SESSION, or run inside a registered harness"))
}

type proc struct {
	pid  int
	comm string
}

// ancestors returns parent, grandparent, ... (up to 8), via ps for macOS/Linux portability.
func ancestors() []proc {
	var out []proc
	pid := os.Getppid()
	for range 8 {
		if pid <= 1 {
			break
		}
		b, err := exec.Command("ps", "-o", "ppid=,comm=", "-p", strconv.Itoa(pid)).Output()
		f := strings.Fields(string(b))
		if err != nil || len(f) < 2 {
			break
		}
		out = append(out, proc{pid, filepath.Base(strings.Join(f[1:], " "))})
		if pid, err = strconv.Atoi(f[0]); err != nil {
			break
		}
	}
	return out
}

// harnessPID is the nearest non-shell ancestor: hooks run as `sh -c ...` under Claude
// Code and Codex, but directly under crush.
func harnessPID() int {
	for _, p := range ancestors() {
		switch strings.TrimPrefix(p.comm, "-") {
		case "sh", "bash", "zsh", "dash", "fish", "env":
			continue
		}
		return p.pid
	}
	return 0
}

func (c *client) call(req broker.Request, out any) error {
	c.nextID++
	req.ID = c.nextID
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if len(data) >= broker.MaxFrame { // the daemon would drop the connection
		return coded(codeTooLarge, fmt.Errorf("request is %d bytes encoded, limit %d; share big content with -ref PATH instead", len(data), broker.MaxMessage))
	}
	if _, err := c.nc.Write(append(data, '\n')); err != nil {
		return coded(codeTransport, err)
	}
	for {
		var frame struct {
			broker.Event
			ID     int64           `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *broker.Error   `json:"error"`
		}
		if err := c.next(&frame); err != nil {
			return err
		}
		if frame.Event.Event == "message" {
			c.events = append(c.events, frame.Message)
			continue
		}
		if frame.ID != req.ID {
			continue
		}
		if frame.Error != nil {
			return frame.Error
		}
		if out == nil || len(frame.Result) == 0 {
			return nil
		}
		return json.Unmarshal(frame.Result, out)
	}
}

// waitReply blocks until a message replying to msgID is pushed to this connection.
func (c *client) waitReply(msgID string, timeout time.Duration) (*broker.Message, error) {
	return c.waitReplyUntil(msgID, time.Now().Add(timeout), timeout)
}

// waitReplyUntil is waitReply with an absolute deadline (timeout is for the message).
func (c *client) waitReplyUntil(msgID string, deadline time.Time, timeout time.Duration) (*broker.Message, error) {
	c.nc.SetReadDeadline(deadline)
	for {
		for _, m := range c.events {
			if m.ReplyTo == msgID {
				return m, nil
			}
		}
		c.events = c.events[:0]
		var ev broker.Event
		if err := c.next(&ev); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, coded(codeTimeout, fmt.Errorf("no reply to %s within %s; the question stays answerable and a late reply is queued for you: check `agm inbox` or `agm history`", msgID, timeout))
			}
			return nil, err
		}
		if ev.Message != nil {
			c.events = append(c.events, ev.Message)
		}
	}
}

func (c *client) next(v any) error {
	if !c.sc.Scan() {
		if err := c.sc.Err(); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return coded(codeTimeout, err)
			}
			return coded(codeTransport, err)
		}
		return coded(codeTransport, errors.New("daemon closed the connection"))
	}
	return coded(codeTransport, json.Unmarshal(c.sc.Bytes(), v))
}
