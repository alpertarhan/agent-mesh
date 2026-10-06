package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const outQueue = 256 // per-connection write buffer; overflow disconnects the slow client

// Listen opens the unix socket at path. The parent dir is 0700, the socket 0600.
// A stale socket file is removed; a live one means another daemon is running.
func Listen(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	// The lock is held for the life of the process (fd intentionally not closed), so
	// racing starters (e.g. every adapter after an upgrade) cannot replace each other's socket.
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("daemon already running on %s", path)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

// Serve accepts connections until ctx is done or ln fails.
func Serve(ctx context.Context, ln net.Listener, b *Broker) error {
	go func() { <-ctx.Done(); ln.Close() }()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() { defer wg.Done(); handle(ctx, nc, b) }()
	}
}

type conn struct {
	out  chan []byte
	done chan struct{}
	once sync.Once
}

func (c *conn) Push(m *Message) bool { return c.write(Event{Event: "message", Message: m}) }

// write never blocks: a full queue closes the connection.
func (c *conn) write(v any) bool {
	data, err := json.Marshal(v)
	if err != nil {
		return false
	}
	select {
	case <-c.done:
		return false
	case c.out <- append(data, '\n'):
		return true
	default:
		c.Close()
		return false
	}
}

func (c *conn) Close() { c.once.Do(func() { close(c.done) }) }

func handle(ctx context.Context, nc net.Conn, b *Broker) {
	c := &conn{out: make(chan []byte, outQueue), done: make(chan struct{})}
	defer nc.Close()
	go func() {
		defer nc.Close() // unblocks the reader
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.done:
				return
			case data := <-c.out:
				if _, err := nc.Write(data); err != nil {
					c.Close()
					return
				}
			}
		}
	}()

	var self string
	defer func() {
		c.Close()
		if self != "" {
			b.Detach(self, c)
		}
	}()

	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 64<<10), MaxFrame)
	for sc.Scan() {
		var req Request
		var result any
		var err error
		if err = json.Unmarshal(sc.Bytes(), &req); err != nil {
			err = errf(CodeBadRequest, "invalid json: %v", err)
		} else {
			result, err = dispatch(b, c, &self, &req)
		}
		resp := Response{ID: req.ID, Result: result}
		if err != nil {
			var me *Error
			if !errors.As(err, &me) {
				me = errf(CodeBadRequest, "%v", err)
			}
			resp.Error = me
		}
		if !c.write(resp) {
			return
		}
	}
}

func dispatch(b *Broker, c *conn, self *string, req *Request) (any, error) {
	switch req.Op {
	case "list":
		return b.List(), nil
	case "protocol": // read-only: lets new clients detect an older running daemon
		return map[string]int{"protocol": Protocol, "bridge": BridgeSupport}, nil
	case "resolve": // read-only, like list
		return b.Resolve(req.To)
	case "shutdown":
		b.Shutdown()
		return nil, nil
	case "spawn": // parent = this connection's session, if it said hello
		name := ""
		if req.Session != nil {
			name = req.Session.Name
		}
		name, depth, err := b.Spawn(*self, name)
		if err != nil {
			return nil, err
		}
		return map[string]any{"name": name, "depth": depth}, nil
	}
	if req.Op == "hello" {
		if req.Session == nil {
			return nil, errf(CodeBadRequest, "session required")
		}
		if *self != "" && *self != req.Session.ID {
			return nil, errf(CodeBadRequest, "connection already bound to %q", *self)
		}
		if err := b.Hello(*req.Session, c, req.Subscribe || req.Wait, req.Wait); err != nil {
			return nil, err
		}
		*self = req.Session.ID
		return map[string]string{"id": *self}, nil
	}
	if *self == "" {
		return nil, errf(CodeNotRegistered, "say hello first")
	}
	switch req.Op {
	case "send":
		return b.Send(*self, req.SendReq, c)
	case "inbox":
		return b.Inbox(*self)
	case "ack":
		return b.Ack(*self, req.IDs) // the ids actually removed
	case "take":
		return b.Take(*self)
	case "history": // read-only: own sent/received messages
		return b.History(*self, req.Limit, HistoryFilter{With: req.With, Thread: req.Thread})
	case "wait": // read-only: a reply to one of your questions, now or pushed later
		if len(req.IDs) != 1 {
			return nil, errf(CodeBadRequest, "wait needs exactly one question id")
		}
		return b.Wait(*self, req.IDs[0], c)
	case "show": // read-only
		if len(req.IDs) != 1 {
			return nil, errf(CodeBadRequest, "show needs exactly one id")
		}
		return b.Show(*self, req.IDs[0])
	case "requeue": // hook could not hand taken mail to its harness; own mailbox only
		b.Requeue(*self, req.Messages)
		return nil, nil
	case "bye":
		log.Printf("bye %s", *self)
		b.Bye(*self)
		return nil, nil
	}
	return nil, errf(CodeBadRequest, "unknown op %q", req.Op)
}
