package badger

import (
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dgraph-io/badger/v4"
)

// TxPublisher represents a BadgerDB Watermill publisher
// TxPublisher would be typically be used in scenarios where messages must
// be published within a pre-existing transaction as part of an outbox pattern.
type TxPublisher struct {
	tx       *badger.Txn
	registry Registry
	config   PublisherConfig
}

// NewTxPublisher returns a new publisher using the specified transaction
func NewTxPublisher(tx *badger.Txn, r Registry, c PublisherConfig) TxPublisher {
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

	subscriptions, err := p.registry.Subscriptions(topic)
	if err != nil {
		return fmt.Errorf("failed to retrieve subscriptions: %w", err)
	}

	if len(subscriptions) < 1 {
		return nil
	}

	now := time.Now().UTC()

	for _, subscription := range subscriptions {
		for _, message := range messages {
			sequence, err := subscription.Sequence.Next()
			if err != nil {
				return err
			}

			value, err := p.marshalMessage(message, now)
			if err != nil {
				return fmt.Errorf("failed to marshal message: %w", err)
			}

			dueAt, err := p.getDueAt(message, now)
			if err != nil {
				return fmt.Errorf("failed to parse delay: %w", err)
			}

			key := EncodeMessageKey(subscription.MessageKeyPrefix, dueAt, sequence)
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
