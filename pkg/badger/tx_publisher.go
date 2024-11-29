package badger

import (
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type (
	// TxPublisher represents a BadgerDB Watermill publisher
	// TxPublisher would be typically be used in scenarios where messages must
	// be published within a pre-existing transaction as part of an outbox pattern.
	TxPublisher struct {
		tx       Tx
		registry Registry
		config   PublisherConfig
	}

	Tx interface {
		Set(key []byte, val []byte) error
	}
)

// NewTxPublisher returns a new publisher using the specified transaction
func NewTxPublisher(tx Tx, r Registry, c PublisherConfig) TxPublisher {
	c.setDefaults()

	return TxPublisher{
		tx:       tx,
		registry: r,
		config:   c,
	}
}

// Publish publishes the specified messages
func (p TxPublisher) Publish(topic string, messages ...*message.Message) error {
	if topic == "" {
		return errEmptyTopic
	}

	if len(messages) < 1 {
		return nil
	}

	prefixes, err := p.registry.GetPrefixes(topic)
	if err != nil {
		return fmt.Errorf("failed to retrieve subscription prefixes: %w", err)
	}

	if len(prefixes) < 1 {
		return nil
	}

	sequence, err := p.registry.GetSequence(topic, uint64(len(messages)*len(prefixes)))
	if err != nil {
		return err
	}
	defer sequence.Release()

	now := time.Now().UTC()

	for _, message := range messages {
		value, err := p.marshalMessage(message, now)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		dueAt, err := p.getDueAt(message, now)
		if err != nil {
			return fmt.Errorf("failed to parse delay: %w", err)
		}

		for _, prefix := range prefixes {
			seq, err := sequence.Next()
			if err != nil {
				return fmt.Errorf("failed to get next sequence: %w", err)
			}

			key := encodeKey(prefix, dueAt, seq)
			if err = p.tx.Set(key, value); err != nil {
				return fmt.Errorf("failed to write message: %w", err)
			}
		}
	}

	return nil
}

func (p TxPublisher) Close() error {
	return nil
}

func (p TxPublisher) marshalMessage(m *message.Message, now time.Time) ([]byte, error) {
	persistedMessage := PersistedMessage{
		UUID:     m.UUID,
		Metadata: m.Metadata,
		Payload:  m.Payload,
		Created:  now,
	}

	value, err := p.config.Marshaler.Marshal(persistedMessage)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (p TxPublisher) getDueAt(m *message.Message, now time.Time) (time.Time, error) {
	if m.Metadata == nil {
		return now, nil
	}

	until, exists := m.Metadata[delay.DelayedUntilKey]
	if !exists {
		return now, nil
	}

	return time.Parse(time.RFC3339, until)
}
