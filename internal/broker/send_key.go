package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"unicode"
	"unicode/utf8"
)

// IdempotencySupport is negotiated separately so unkeyed protocol-2 clients keep working.
const IdempotencySupport = 1

const sendKeyCap = 4096

// Bodies are supplied by the identical retry, not duplicated in the durable ledger.
type keyedSend struct {
	Key         string  `json:"key"`
	Fingerprint string  `json:"fingerprint"`
	Message     Message `json:"message"`
}

// prepareSendKey runs under b.mu, before routing rewrites the original request.
func (b *Broker) prepareSendKey(from string, r SendReq) (*keyedSend, *Message, error) {
	if r.RequestKey == "" {
		return nil, nil, nil
	}
	invalidRune := strings.IndexFunc(r.RequestKey, func(r rune) bool {
		return unicode.IsSpace(r) || dirtyRune(r)
	}) >= 0
	invalidEncoding := !utf8.ValidString(r.RequestKey)
	if len(r.RequestKey) > 128 || invalidEncoding || invalidRune {
		return nil, nil, errf(CodeBadRequest, "request_key must be at most 128 UTF-8 bytes without whitespace or controls")
	}
	if r.ExpectsReply && !r.NoWait {
		return nil, nil, errf(CodeBadRequest, "request_key requires no_wait for asks; wait for the reply separately")
	}
	key := r.RequestKey
	r.RequestKey = ""
	data, err := json.Marshal(r)
	if err != nil {
		return nil, nil, errf(CodeBadRequest, "cannot fingerprint send request: %v", err)
	}
	hash := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(hash[:])
	if previous, ok := b.sendKeys[from+"\x00"+key]; ok {
		if previous.Fingerprint != fingerprint {
			return nil, nil, errf(
				CodeKeyConflict,
				"request_key %q already belongs to message %s with a different request",
				key,
				previous.Message.ID,
			)
		}
		m := previous.Message
		m.Text, m.Attachments = r.Text, r.Attachments
		return nil, &m, nil
	}
	return &keyedSend{Key: key, Fingerprint: fingerprint}, nil, nil
}

func (b *Broker) rememberSendKey(entry keyedSend) {
	entry.Message.Text = ""
	entry.Message.Attachments = nil
	key := entry.Message.From + "\x00" + entry.Key
	if _, exists := b.sendKeys[key]; exists {
		return
	}
	b.sendKeys[key] = entry
	b.sendKeyRing = append(b.sendKeyRing, key)
	if len(b.sendKeyRing) > sendKeyCap {
		delete(b.sendKeys, b.sendKeyRing[0])
		b.sendKeyRing[0] = ""
		b.sendKeyRing = b.sendKeyRing[1:]
	}
}
