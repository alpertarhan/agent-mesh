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
	Live      bool      `json:"live"`
	Connected bool      `json:"connected,omitempty"`
	Queued    int       `json:"queued"`
	Activity  string    `json:"activity,omitempty"` // unknown if unsupported, disconnected, or stale
	Presence  *Presence `json:"presence,omitempty"`
	Task      *TaskInfo `json:"task,omitempty"`
}

type Attachment struct {
	Type     string `json:"type"` // file | snippet | context | ref
	Name     string `json:"name"`
	Content  string `json:"content,omitempty"`
	Path     string `json:"path,omitempty"` // ref: absolute path, read by the receiver (no content sent)
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
	RequestKey   string       `json:"request_key,omitempty"` // sender-scoped retry key; requires idempotency capability
	To           string       `json:"to,omitempty"`          // may be empty when ReplyTo is set
	Text         string       `json:"text,omitempty"`
	Attachments  []Attachment `json:"attachments,omitempty"`
	ReplyTo      string       `json:"reply_to,omitempty"`
	ExpectsReply bool         `json:"expects_reply,omitempty"`
	NoWait       bool         `json:"no_wait,omitempty"` // ask without blocking: reply is queued (see Wait)
}

// BridgeSupport is the daemon's bridge capability, reported by op "protocol"
// next to Protocol. A daemon from before the bridge reports no value here:
// it sweeps "/" rows after IdleTTL and wakes "/" ids locally, so the bridge
// must not mirror through it. Bump it when the daemon gains bridge-relevant
// behavior. Protocol stays 2: the field is additive and older clients ignore
// it.
const BridgeSupport = 1

// Protocol is the daemon's protocol version (op "protocol"). New fields/ops that old
// daemons silently ignore require a version bump or a separately negotiated capability,
// so new clients can refuse unsupported behavior. 2: refs, history/show, resolve, no_wait/wait, history filters, ack result.
const Protocol = 2

type Request struct {
	ID        int64        `json:"id"`
	Op        string       `json:"op"` // protocol | hello | list | resolve | send | inbox | ack | take | history | show | wait | bye | shutdown | spawn | requeue
	Session   *SessionInfo `json:"session,omitempty"`
	Subscribe bool         `json:"subscribe,omitempty"`
	Wait      bool         `json:"wait,omitempty"`     // exclusive subscriber: replaces the previous waiter
	Messages  []*Message   `json:"messages,omitempty"` // requeue
	IDs       []string     `json:"ids,omitempty"`
	Limit     int          `json:"limit,omitempty"`    // history
	With      string       `json:"with,omitempty"`     // history: only messages with this peer
	Thread    string       `json:"thread,omitempty"`   // history: only the reply thread of this message
	Presence  *Presence    `json:"presence,omitempty"` // observe: full ephemeral observation
	Progress  bool         `json:"progress,omitempty"` // observe: actual work progress, not a heartbeat
	Task      *TaskInfo    `json:"task,omitempty"`     // task: self-declared work; empty object clears
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
	CodeTooLarge      = "too_large"
	CodePersistence   = "persistence_failed"
	CodeKeyConflict   = "key_conflict"
)

func errf(code, format string, a ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, a...)}
}
