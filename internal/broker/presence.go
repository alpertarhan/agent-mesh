package broker

import (
	"math"
	"slices"
	"strings"
	"time"
)

// CoordinationSupport is negotiated separately so protocol-2 clients keep working.
const CoordinationSupport = 1
const TaskLifecycleSupport = 1
const PresenceTTL = 30 * time.Second

// Presence is a bounded, ephemeral observation, not a transcript. The broker sets
// timestamps/staleness; heartbeats do not imply work progress.
type Presence struct {
	State       string          `json:"state"`
	Provider    string          `json:"provider,omitempty"`
	Model       string          `json:"model,omitempty"`
	ActiveTools []string        `json:"active_tools,omitempty"`
	Context     *ContextUsage   `json:"context"`
	Metrics     *RequestMetrics `json:"metrics"`
	UpdatedAt   time.Time       `json:"updated_at"`
	StateSince  time.Time       `json:"state_since"`
	ProgressAt  time.Time       `json:"progress_at"`
	Stale       bool            `json:"stale"`
}

type ContextUsage struct {
	Tokens int64 `json:"tokens"`
	Window int64 `json:"window"`
}

// RequestMetrics describes the last completed model request, not an entire turn.
// Nil timing values mean unknown. Tool/user waits are not generation time.
type RequestMetrics struct {
	Provider     string    `json:"provider,omitempty"`
	Model        string    `json:"model,omitempty"`
	ModelSource  string    `json:"model_source"` // response | request | unknown
	OutputTokens int64     `json:"output_tokens"`
	DurationMS   float64   `json:"duration_ms"`
	TTFTMS       *float64  `json:"ttft_ms"`
	TPS          *float64  `json:"tps"`         // output only: output tokens / post-first-token seconds
	TTFTSource   string    `json:"ttft_source"` // native | observed | unknown
	ObservedAt   time.Time `json:"observed_at"`
}

// TaskInfo is explicitly published by its owner; scopes are declarations, not locks.
type TaskInfo struct {
	Summary    string    `json:"summary"`
	Scope      []string  `json:"scope,omitempty"`
	WaitingFor string    `json:"waiting_for,omitempty"`
	State      string    `json:"state,omitempty"`
	RequestID  string    `json:"request_id,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func ValidTaskState(s string) bool {
	switch s {
	case "accepted", "working", "blocked", "done":
		return true
	}
	return false
}

func ValidActivity(s string) bool {
	switch s {
	case "idle", "busy", "waiting", "error", "unknown":
		return true
	}
	return false
}

func copyPresence(p *Presence) *Presence {
	if p == nil {
		return nil
	}
	out := *p
	out.ActiveTools = slices.Clone(p.ActiveTools)
	if p.Context != nil {
		c := *p.Context
		out.Context = &c
	}
	if p.Metrics != nil {
		m := *p.Metrics
		if m.TTFTMS != nil {
			x := *m.TTFTMS
			m.TTFTMS = &x
		}
		if m.TPS != nil {
			x := *m.TPS
			m.TPS = &x
		}
		out.Metrics = &m
	}
	return &out
}

func copyTask(t *TaskInfo) *TaskInfo {
	if t == nil {
		return nil
	}
	out := *t
	out.Scope = slices.Clone(t.Scope)
	return &out
}

// Observe replaces the bound session's ephemeral observation. No spool write.
func (b *Broker) Observe(id string, value *Presence, progress bool, owner Sink) error {
	if value == nil || owner == nil || !ValidActivity(value.State) {
		return errf(CodeBadRequest, "presence needs a state: idle, busy, waiting, error, or unknown")
	}
	p := copyPresence(value)
	p.Provider, p.Model = CleanLine(p.Provider, 128), CleanLine(p.Model, 256)
	if len(p.ActiveTools) > 8 {
		return errf(CodeBadRequest, "at most 8 active tool names")
	}
	for i, tool := range p.ActiveTools {
		p.ActiveTools[i] = CleanLine(tool, 64)
	}
	if c := p.Context; c != nil && (c.Tokens < 0 || c.Window <= 0) {
		return errf(CodeBadRequest, "context needs non-negative tokens and a positive window")
	}
	if m := p.Metrics; m != nil {
		finite := func(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
		if m.OutputTokens < 0 || !finite(m.DurationMS) || m.DurationMS <= 0 || m.ObservedAt.IsZero() {
			return errf(CodeBadRequest, "metrics need non-negative output tokens, a positive duration, and observed_at")
		}
		m.Provider, m.Model = CleanLine(m.Provider, 128), CleanLine(m.Model, 256)
		switch m.ModelSource {
		case "":
			m.ModelSource = "unknown"
		case "response", "request", "unknown":
		default:
			return errf(CodeBadRequest, "model_source must be response, request, or unknown")
		}
		m.TPS = nil // never trust a sender's derived throughput
		if m.TTFTMS == nil {
			m.TTFTSource = "unknown"
		} else {
			if !finite(*m.TTFTMS) || *m.TTFTMS < 0 || *m.TTFTMS > m.DurationMS {
				return errf(CodeBadRequest, "ttft_ms must be between zero and duration_ms")
			}
			if m.TTFTSource != "native" && m.TTFTSource != "observed" {
				return errf(CodeBadRequest, "ttft_source must be native or observed when ttft_ms is known")
			}
			if seconds := (m.DurationMS - *m.TTFTMS) / 1000; seconds > 0 && m.OutputTokens > 0 {
				tps := float64(m.OutputTokens) / seconds
				if finite(tps) {
					m.TPS = &tps
				}
			}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	now := b.now()
	p.UpdatedAt, p.StateSince, p.ProgressAt, p.Stale = now, now, now, false
	if prev := s.presence; prev != nil {
		if prev.State == p.State {
			p.StateSince = prev.StateSince
		}
		if !progress && prev.State == p.State {
			p.ProgressAt = prev.ProgressAt
		}
	}
	s.presence, s.presenceOwner = p, owner
	s.info.LastSeen = now
	return nil
}

// SetTask persists self-declared metadata without messages or file reads. Linked
// updates retain omitted request/state/scope fields; an empty task clears it.
func (b *Broker) SetTask(id string, value *TaskInfo) error {
	if value == nil {
		return errf(CodeBadRequest, "task object required; use an empty object to clear it")
	}
	task := copyTask(value)
	task.Summary, task.WaitingFor = CleanLine(task.Summary, 500), CleanLine(task.WaitingFor, 256)
	if len(task.Scope) > 8 {
		return errf(CodeBadRequest, "at most 8 task scope paths")
	}
	for _, p := range task.Scope {
		if !strings.HasPrefix(p, "/") || len(p) > 4096 || CleanLine(p, 0) != p {
			return errf(CodeBadRequest, "task scope needs absolute, single-line paths of at most 4096 bytes")
		}
	}
	if task.Summary == "" {
		if len(task.Scope) > 0 || task.WaitingFor != "" || task.State != "" || task.RequestID != "" {
			return errf(CodeBadRequest, "task summary required with scope, waiting_for, state or request_id")
		}
		task = nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	if !ok {
		return errf(CodeNotRegistered, "session %q has not said hello", id)
	}
	if task != nil {
		// A summary/blocker update must not silently sever a linked task's request.
		prev := s.info.Task
		if prev != nil && prev.RequestID != "" {
			if task.RequestID == "" {
				task.RequestID = prev.RequestID
			}
			if task.RequestID == prev.RequestID {
				if task.State == "" {
					task.State = prev.State
				}
				if task.Scope == nil {
					task.Scope = slices.Clone(prev.Scope)
				}
			}
		}
		if task.State != "" || task.RequestID != "" {
			if !ValidTaskState(task.State) || task.RequestID == "" || len(task.RequestID) > 256 || CleanLine(task.RequestID, 0) != task.RequestID {
				return errf(CodeBadRequest, "linked tasks need a request_id and state: accepted, working, blocked, or done")
			}
			// The current link was already validated, so long jobs survive history eviction.
			if prev == nil || prev.RequestID != task.RequestID {
				addressed := func(m *Message) bool { return m.ID == task.RequestID && m.To == id }
				if !slices.ContainsFunc(b.history, addressed) && !slices.ContainsFunc(s.mailbox, addressed) {
					return errf(CodeUnknownMsg, "task request must be a retained message addressed to this session")
				}
			}
		}
		task.UpdatedAt = b.now()
	}
	prev := s.info.Task
	s.info.Task = task
	if err := b.store(); err != nil {
		s.info.Task = prev
		return err
	}
	return nil
}

// sessionInfo is a detached read snapshot. Caller holds b.mu.
func (b *Broker) sessionInfo(s *session) SessionInfo {
	info := s.info
	info.Live, info.Queued = b.live(s), len(s.mailbox)
	info.Connected = len(s.subs) > 0 || s.presenceOwner != nil
	info.Task = copyTask(s.info.Task)
	info.Presence = copyPresence(s.presence)
	info.Activity = "unknown"
	if p := info.Presence; p != nil {
		p.Stale = s.presenceOwner == nil || b.now().Sub(p.UpdatedAt) >= PresenceTTL
		if !p.Stale {
			info.Activity = p.State
		}
	}
	return info
}
