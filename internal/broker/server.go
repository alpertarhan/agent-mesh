package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

const outQueue = 256 // per-connection write buffer; overflow disconnects the slow client

// lockedListener owns its lock files: an unreachable os.File is closed by Go's GC.
type lockedListener struct {
	net.Listener
	lock  atomic.Pointer[os.File] // the socket's, from Listen
	state atomic.Pointer[os.File] // the spool's, from ListenDaemon
	once  sync.Once
	err   error
}

func (l *lockedListener) Close() error {
	l.once.Do(func() {
		l.err = l.Listener.Close()
		if state := l.state.Swap(nil); state != nil {
			l.err = errors.Join(l.err, state.Close())
		}
		if lock := l.lock.Swap(nil); lock != nil {
			l.err = errors.Join(l.err, lock.Close())
		}
	})
	return l.err
}

// Listen opens the unix socket at path. A missing parent dir is created 0700; an
// existing one must be private already (no access for group or other) and is left
// as it is, like anything but a socket at path. The socket is 0600. A stale socket
// file is removed; a live one means another daemon is running.
func Listen(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
	if st, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if !st.IsDir() {
		return nil, fmt.Errorf("socket directory %s is not a directory", dir)
	} else if st.Mode()&0o077 != 0 {
		return nil, fmt.Errorf("socket directory %s has mode %o: group or other could reach the mesh socket; make it private (chmod 700) or use another socket path", dir, st.Mode()&0o777)
	}
	if st, err := os.Lstat(path); err == nil && st.Mode()&os.ModeSocket == 0 {
		return nil, fmt.Errorf("%s exists and is not a socket; move it away or use another socket path", path)
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	// Hold the lock for the listener's lifetime so racing starters cannot replace its socket.
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
		return nil, errors.Join(err, lock.Close())
	}
	owned := &lockedListener{Listener: ln}
	owned.lock.Store(lock)
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, errors.Join(err, owned.Close())
	}
	return owned, nil
}

// ListenDaemon is Listen for a daemon that keeps its state at spool: it also takes the
// exclusive lock on spool+".lock", so one daemon uses a directory, whichever socket it
// listens on. A daemon on another socket there fails with an error naming the
// directory. The listener owns both locks: Serve releases them once the in-flight
// handlers finished, the state lock first.
func ListenDaemon(path, spool string) (net.Listener, error) {
	ln, err := Listen(path)
	if err != nil {
		return nil, err
	}
	state, err := os.OpenFile(spool+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	if err := syscall.Flock(int(state.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		state.Close()
		err = fmt.Errorf("another daemon already uses the state in %s; one daemon per directory - give this socket a directory of its own", filepath.Dir(spool))
		return nil, errors.Join(err, ln.Close())
	}
	ln.(*lockedListener).state.Store(state)
	return ln, nil
}

// Serve accepts connections until ctx is done or ln fails. The locks the listener owns
// (its socket's, and for ListenDaemon the spool's) are released once the in-flight
// handlers finished.
func Serve(ctx context.Context, ln net.Listener, b *Broker) error {
	if owned, ok := ln.(*lockedListener); ok {
		// Stop accepting first, but retain the locks until in-flight mutations finish.
		if lock := owned.lock.Swap(nil); lock != nil {
			defer lock.Close()
		}
		if state := owned.state.Swap(nil); state != nil {
			defer state.Close() // deferred last, so it runs first: whoever gets the socket finds the state free
		}
	}
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
	nc   net.Conn
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

func (c *conn) Close() {
	c.once.Do(func() {
		close(c.done)
		if c.nc != nil {
			c.nc.Close() // also interrupts a writer blocked by a slow reader
		}
	})
}

func handle(ctx context.Context, nc net.Conn, b *Broker) {
	c := &conn{nc: nc, out: make(chan []byte, outQueue), done: make(chan struct{})}
	stopClose := context.AfterFunc(ctx, c.Close)
	defer stopClose()
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
		return map[string]int{
			"protocol": Protocol, "bridge": BridgeSupport,
			"coordination": CoordinationSupport, "idempotency": IdempotencySupport,
			"take_ids": TakeIDsSupport, "task_lifecycle": TaskLifecycleSupport, "role": RoleSupport, "delivery_tracking": 1,
		}, nil
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
	case "observe":
		return nil, b.Observe(*self, req.Presence, req.Progress, c)
	case "task":
		return nil, b.SetTask(*self, req.Task)
	case "role":
		return nil, b.SetRole(*self, req.Role)
	case "send":
		return b.Send(*self, req.SendReq, c)
	case "inbox":
		return b.Inbox(*self)
	case "ack":
		return b.Ack(*self, req.IDs) // the ids actually removed
	case "take":
		return b.Take(*self, req.IDs...)
	case "history": // read-only: own sent/received messages
		return b.History(*self, req.Limit, HistoryFilter{With: req.With, Thread: req.Thread})
	case "wait": // read-only: a reply to one of your questions, now or pushed later
		if len(req.IDs) != 1 {
			return nil, errf(CodeBadRequest, "wait needs exactly one question id")
		}
		return b.Wait(*self, req.IDs[0], c)
	case "track", "seen":
		if len(req.IDs) != 1 {
			return nil, errf(CodeBadRequest, "%s needs one full message id", req.Op)
		}
		return b.Track(*self, req.IDs[0], req.Op == "seen")
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
