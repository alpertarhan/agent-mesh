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
}

// dial connects to the daemon, starting it if the socket is not answering.
func dial() (*client, error) {
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

// session dials and says hello as id. Without id, the session is found by matching
// a registered harness PID against this process's ancestors.
func session(id string, info broker.SessionInfo) (*client, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	if id == "" {
		// Codex runs shell commands under a shared app-server daemon, so ancestor PIDs
		// cannot tell its sessions apart; it exports the thread id (= hook session_id).
		// Antigravity exports its conversation id the same way.
		id = cmp(os.Getenv("CODEX_THREAD_ID"), os.Getenv("ANTIGRAVITY_CONVERSATION_ID"))
	}
	if id == "" {
		if id, err = c.selfID(); err != nil {
			return nil, err
		}
	}
	info.ID = id
	if err := c.call(broker.Request{Op: "hello", Session: &info}, nil); err != nil {
		return c, err
	}
	c.id = id
	return c, nil
}

// ponytail: if one harness process hosts several sessions (crush can switch sessions),
// the most recently seen one wins; pass -as when that is ambiguous.
func (c *client) selfID() (string, error) {
	var list []broker.SessionInfo
	if err := c.call(broker.Request{Op: "list"}, &list); err != nil {
		return "", err
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
			return best.ID, nil
		}
	}
	return "", errors.New("no session id: pass -as, set AGM_SESSION, or run inside a registered harness")
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
	if _, err := c.nc.Write(append(data, '\n')); err != nil {
		return err
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
	c.nc.SetReadDeadline(time.Now().Add(timeout))
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
				return nil, fmt.Errorf("no reply within %s (a late reply lands in your inbox)", timeout)
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
			return err
		}
		return errors.New("daemon closed the connection")
	}
	return json.Unmarshal(c.sc.Bytes(), v)
}
