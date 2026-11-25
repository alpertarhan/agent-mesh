package broker

import (
	"slices"
	"time"
)

// Evidence is separate from immutable wire messages and follows history retention.
type DeliveryEvidence struct {
	Delivery string     `json:"delivery,omitempty"`
	SeenAt   *time.Time `json:"seen_at,omitempty"`
}

type MessageStatus struct {
	ID      string `json:"id"`
	From    string `json:"from"`
	To      string `json:"to"`
	Queued  bool   `json:"queued"`
	ReplyID string `json:"reply_id,omitempty"`
	DeliveryEvidence
}

// Track reports local evidence. Marking seen is an explicit act by the recipient,
// independent of queue removal, replies and the declared state of a task.
func (b *Broker) Track(self, id string, markSeen bool) (MessageStatus, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var r MessageStatus
	for _, m := range b.history {
		if m.ID == id && (m.To == self || (!markSeen && m.From == self)) {
			r = MessageStatus{ID: id, From: m.From, To: m.To, DeliveryEvidence: b.evidence[id]}
		} else if r.ID != "" && m.ReplyTo == id && m.From == r.To && m.To == r.From {
			r.ReplyID = m.ID
		}
	}
	if r.ID == "" {
		return r, errf(CodeUnknownMsg, "message not found in your retained history or not eligible for this operation")
	}
	if s := b.sessions[r.To]; s != nil {
		r.Queued = slices.ContainsFunc(s.mailbox, func(m *Message) bool { return m != nil && m.ID == id })
	}
	if markSeen && r.SeenAt == nil {
		prev, existed := b.evidence[id]
		now := b.now()
		r.SeenAt = &now
		b.evidence[id] = r.DeliveryEvidence
		if err := b.store(); err != nil {
			if existed {
				b.evidence[id] = prev
			} else {
				delete(b.evidence, id)
			}
			return MessageStatus{}, err
		}
	}
	if r.Delivery == "" {
		r.Delivery = "unknown"
	}
	return r, nil
}
