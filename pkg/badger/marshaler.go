package badger

import (
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type (
	// PersistedMessage represents a persisted message
	PersistedMessage struct {
		UUID     string
		Metadata message.Metadata
		Payload  message.Payload
		Created  time.Time
	}

	// persistedMessage represents an internal persisted message for marshaling
	persistedMessage struct {
		UUID     string            `json:"uuid,omitempty"`
		Metadata map[string]string `json:"metadata,omitempty"`
		Payload  []byte            `json:"payload,omitempty"`
		Created  time.Time         `json:"created,omitempty"`
	}

	// Marshaler represents a marshaler for watermill messages
	Marshaler interface {
		Marshal(msg PersistedMessage) ([]byte, error)
		Unmarshal(b []byte) (PersistedMessage, error)
	}

	// JSONMarshaler is a JSON implementation of the Marshaler interface
	JSONMarshaler struct{}
)

// Marshal marshals the message to JSON
func (m JSONMarshaler) Marshal(msg PersistedMessage) ([]byte, error) {
	v := persistedMessage{
		UUID:     msg.UUID,
		Metadata: msg.Metadata,
		Payload:  msg.Payload,
		Created:  msg.Created,
	}

	return json.Marshal(v)
}

// Unmarshal unmarshals the message from JSON
func (m JSONMarshaler) Unmarshal(b []byte) (PersistedMessage, error) {
	var v persistedMessage

	if err := json.Unmarshal(b, &v); err != nil {
		return PersistedMessage{}, err
	}

	msg := message.NewMessage(v.UUID, v.Payload)
	msg.Metadata = v.Metadata

	return PersistedMessage{
		UUID:     v.UUID,
		Metadata: v.Metadata,
		Payload:  v.Payload,
		Created:  v.Created,
	}, nil

}
