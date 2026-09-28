package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestServerConcurrentMesh: N sessions each send K messages to every other session
// concurrently over the real socket; every message must arrive exactly once.
func TestServerConcurrentMesh(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "mesh") // short path: unix socket limit ~104 bytes on macOS
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "m.sock")

	lim := DefaultLimits()
	lim.SendBurst, lim.SendPerMin, lim.MailboxCap = 1e6, 1e6, 1e6
	b, _ := New(lim, "")
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, ln, b) }()

	const n, k = 20, 5
	type client struct {
		nc  net.Conn
		enc *json.Encoder
		got atomic.Int64
	}
	clients := make([]*client, n)
	var readers sync.WaitGroup
	for i := range n {
		nc, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		c := &client{nc: nc, enc: json.NewEncoder(nc)}
		clients[i] = c
		ready := make(chan struct{})
		readers.Add(1)
		go func() {
			defer readers.Done()
			sc := bufio.NewScanner(nc)
			for sc.Scan() {
				var frame struct {
					ID    int64  `json:"id"`
					Event string `json:"event"`
					Error *Error `json:"error"`
				}
				json.Unmarshal(sc.Bytes(), &frame)
				switch {
				case frame.Error != nil:
					t.Errorf("client %d: %v", i, frame.Error)
				case frame.Event == "message":
					c.got.Add(1)
				case frame.ID == 1:
					close(ready)
				}
			}
		}()
		c.enc.Encode(Request{ID: 1, Op: "hello", Session: &SessionInfo{ID: fmt.Sprintf("s%02d", i)}, Subscribe: true})
		<-ready
	}

	var senders sync.WaitGroup
	for i, c := range clients {
		senders.Add(1)
		go func() {
			defer senders.Done()
			id := int64(2)
			for range k {
				for j := range n {
					if j != i {
						c.enc.Encode(Request{ID: id, Op: "send", SendReq: SendReq{To: fmt.Sprintf("s%02d", j), Text: "x"}})
						id++
					}
				}
			}
		}()
	}
	senders.Wait()

	want := int64((n - 1) * k)
	deadline := time.Now().Add(5 * time.Second)
	for _, c := range clients {
		for c.got.Load() < want && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := c.got.Load(); got != want {
			t.Errorf("received %d, want %d", got, want)
		}
	}

	cancel()
	for _, c := range clients {
		c.nc.Close()
	}
	readers.Wait()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
