// Package message defines the channel-neutral request and reply contracts used
// between adapters and the Agent execution runtime.
package message

import "time"

type InboundMessage struct {
	Channel        string
	BindingID      string
	MessageID      string
	ExternalUserID string
	ConversationID string
	Text           string
	RequestID      string
	TraceID        string
	ReceivedAt     time.Time
}

type OutboundMessage struct {
	Channel   string `json:"-"`
	BindingID string `json:"-"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}
