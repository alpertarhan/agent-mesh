// Package codex delivers mesh messages into running Codex sessions through the
// shared app-server daemon (JSON-RPC over a WebSocket on a unix socket). turn/start
// starts a turn on an idle thread and joins the running turn of a busy one, and the
// Codex TUI shows it like a typed prompt.
package codex

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const timeout = 20 * time.Second

func socketPath() string {
	home := os.Getenv("CODEX_HOME")
	if home == "" {
		h, _ := os.UserHomeDir()
		home = filepath.Join(h, ".codex")
	}
	return filepath.Join(home, "app-server-control", "app-server-control.sock")
}

// Deliver starts (or joins) a turn with text on thread id. It fails with
// ErrNotLoaded if no Codex client has the thread open: we never run a session headless.
func Deliver(id, text string) error {
	c, err := dial()
	if err != nil {
		return err
	}
	defer c.nc.Close()
	loaded, err := c.loaded()
	if err != nil {
		return err
	}
	if !slices.Contains(loaded, id) {
		return ErrNotLoaded
	}
	return c.call("turn/start", map[string]any{
		"threadId": id,
		"input":    []map[string]string{{"type": "text", "text": text}},
	}, nil)
}

var ErrNotLoaded = errors.New("codex thread not loaded")

func (c *client) loaded() ([]string, error) {
	var r struct {
		Data []string `json:"data"`
	}
	return r.Data, c.call("thread/loaded/list", map[string]any{}, &r)
}

type client struct {
	nc     net.Conn
	r      *bufio.Reader
	nextID int
}

func dial() (*client, error) {
	nc, err := net.DialTimeout("unix", socketPath(), 2*time.Second)
	if err != nil {
		return nil, err
	}
	nc.SetDeadline(time.Now().Add(timeout))
	c := &client{nc: nc, r: bufio.NewReader(nc)}
	if err := c.handshake(); err != nil {
		nc.Close()
		return nil, err
	}
	init := map[string]any{"clientInfo": map[string]string{"name": "agent-mesh", "version": "0.1"}}
	if err := c.call("initialize", init, nil); err != nil {
		nc.Close()
		return nil, err
	}
	if err := c.writeJSON(map[string]string{"method": "initialized"}); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

func (c *client) handshake() error {
	var key [16]byte
	rand.Read(key[:])
	req := "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key[:]) + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(c.nc, req); err != nil {
		return err
	}
	resp, err := http.ReadResponse(c.r, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return fmt.Errorf("codex app-server: websocket upgrade: %s", resp.Status)
	}
	return nil
}

// call sends a request and waits for its response, skipping notifications.
func (c *client) call(method string, params, out any) error {
	c.nextID++
	id := c.nextID
	if err := c.writeJSON(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return err
	}
	for {
		data, err := c.readMessage()
		if err != nil {
			return err
		}
		var m struct {
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &m) != nil || m.ID == nil || *m.ID != id {
			continue // notification or server request
		}
		if m.Error != nil {
			return fmt.Errorf("codex %s: %s", method, m.Error.Message)
		}
		if out != nil {
			return json.Unmarshal(m.Result, out)
		}
		return nil
	}
}

func (c *client) writeJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.writeFrame(0x1, data)
}

// writeFrame writes one final, masked (client→server) frame.
func (c *client) writeFrame(op byte, data []byte) error {
	hdr := []byte{0x80 | op}
	switch n := len(data); {
	case n < 126:
		hdr = append(hdr, 0x80|byte(n))
	case n < 1<<16:
		hdr = append(hdr, 0x80|126)
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(n))
	default:
		hdr = append(hdr, 0x80|127)
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}
	var mask [4]byte
	rand.Read(mask[:])
	hdr = append(hdr, mask[:]...)
	buf := make([]byte, len(data))
	for i, b := range data {
		buf[i] = b ^ mask[i%4]
	}
	_, err := c.nc.Write(append(hdr, buf...))
	return err
}

const maxMessage = 16 << 20

// readMessage returns the next text/binary message, answering pings and joining fragments.
func (c *client) readMessage() ([]byte, error) {
	var msg []byte
	for {
		var h [2]byte
		if _, err := io.ReadFull(c.r, h[:]); err != nil {
			return nil, err
		}
		fin, op := h[0]&0x80 != 0, h[0]&0x0f
		n := uint64(h[1] & 0x7f)
		switch n {
		case 126:
			var b [2]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return nil, err
			}
			n = uint64(binary.BigEndian.Uint16(b[:]))
		case 127:
			var b [8]byte
			if _, err := io.ReadFull(c.r, b[:]); err != nil {
				return nil, err
			}
			n = binary.BigEndian.Uint64(b[:])
		}
		if n > maxMessage || uint64(len(msg))+n > maxMessage {
			return nil, errors.New("codex app-server: message too large")
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, err
		}
		switch op {
		case 0x8:
			return nil, errors.New("codex app-server closed the connection")
		case 0x9:
			if err := c.writeFrame(0xA, payload); err != nil {
				return nil, err
			}
			continue
		case 0xA:
			continue
		}
		msg = append(msg, payload...)
		if fin {
			return msg, nil
		}
	}
}
