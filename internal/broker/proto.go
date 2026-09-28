// Package broker implements the agent-mesh router and its NDJSON socket protocol.
//
// Wire format: one JSON object per line.
//
//	client → daemon: Request  {"id":1,"op":"send","to":"bob","text":"hi"}
//	daemon → client: Response {"id":1,"result":{...}} | {"id":1,"error":{"code":"...","message":"..."}}
//	daemon → client: Event    {"event":"message","message":{...}}
package broker

import (
	"fmt"
	"time"
)

const MaxFrame = 1 << 20

type SessionInfo struct {
	ID       string    `json:"id"`
	Name     string    `json:"name,omitempty"`
	Harness  string    `json:"harness,omitempty"`
	Cwd      string    `json:"cwd,omitempty"`
	PID      int       `json:"pid,omitempty"`
	Pane     string    `json:"pane,omitempty"`   // herdr pane id, if the harness runs in one
	Parent   string    `json:"parent,omitempty"` // session that spawned this one (agm spawn)
	Depth    int       `json:"depth,omitempty"`  // spawn depth: 0 = started by the user
	LastSeen time.Time `json:"last_seen"`
	// Output only (list).
	Live   bool `json:"live"`
	Queued int  `json:"queued"`
}

type Attachment struct {
	Type     string `json:"type"` // file | snippet | context
	Name     string `json:"name"`
	Content  string `json:"content"`
	Language string `json:"language,omitempty"`
}

type Message struct {
	ID           string       `json:"id"`
	From         string       `json:"from"`
	FromName     string       `json:"from_name,omitempty"`
	To           string       `json:"to"`
	Text         string       `json:"text"`
	Attachments  []Attachment `json:"attachments,omitempty"`
	ReplyTo      string       `json:"reply_to,omitempty"`
	ExpectsReply bool         `json:"expects_reply,omitempty"`
	Hop          int          `json:"hop"`
	At           time.Time    `json:"at"`
}

type SendReq struct {
	To           string       `json:"to,omitempty"` // may be empty when ReplyTo is set
	Text         string       `json:"text,omitempty"`
	Attachments  []Attachment `json:"attachments,omitempty"`
	ReplyTo      string       `json:"reply_to,omitempty"`
	ExpectsReply bool         `json:"expects_reply,omitempty"`
}

type Request struct {
	ID        int64        `json:"id"`
	Op        string       `json:"op"` // hello | list | send | inbox | ack | take | bye | shutdown | spawn | requeue
	Session   *SessionInfo `json:"session,omitempty"`
	Subscribe bool         `json:"subscribe,omitempty"`
	Wait      bool         `json:"wait,omitempty"`     // exclusive subscriber: replaces the previous waiter
	Messages  []*Message   `json:"messages,omitempty"` // requeue
	IDs       []string     `json:"ids,omitempty"`
	SendReq
}

type Response struct {
	ID     int64  `json:"id"`
	Result any    `json:"result,omitempty"`
	Error  *Error `json:"error,omitempty"`
}

type Event struct {
	Event   string   `json:"event"`
	Message *Message `json:"message,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

const (
	CodeBadRequest    = "bad_request"
	CodeNotRegistered = "not_registered"
	CodeUnknownTarget = "unknown_target"
	CodeAmbiguous     = "ambiguous_target"
	CodeUnknownMsg    = "unknown_message"
	CodeMailboxFull   = "mailbox_full"
	CodeRateLimited   = "rate_limited"
	CodeHopLimit      = "hop_limit"
	CodeTooManyAsks   = "too_many_asks"
	CodeDeadlock      = "would_deadlock"
	CodeSpawnLimit    = "spawn_limit"
	CodeNameTaken     = "name_taken"
)

func errf(code, format string, a ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, a...)}
}
