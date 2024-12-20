package badger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dgraph-io/badger/v4"
)

type (
	// SubscriberConfig represents subscriber configuration
	// An empty value is valid, using JSON marshaling by default
	SubscriberConfig struct {
		Name              string
		Marshaler         Marshaler
		ReceiveInterval   time.Duration
		ReceiveBatchSize  int
		VisibilityTimeout time.Duration
		Logger            watermill.LoggerAdapter
	}

	// Subscriber represents a BadgerDB Watermill publisher
	Subscriber struct {
		db       *badger.DB
		registry Registry
		config   SubscriberConfig
		quit     chan struct{}
		wg       sync.WaitGroup
	}

	rawMessage struct {
		key   []byte
		value []byte
	}
)

// NewSubscriber returns a new subscriber
func NewSubscriber(db *badger.DB, r Registry, c SubscriberConfig) *Subscriber {
	c.setDefaults()

	return &Subscriber{
		db:       db,
		registry: r,
		config:   c,
		quit:     make(chan struct{}),
	}
}

// Subscriber creates a subscription to the specified topic
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	subscription, err := s.registry.Register(topic, s.config.Name)
	if err != nil {
		return nil, err
	}

	ch := make(chan *message.Message)

	s.wg.Add(1)
	go s.run(ctx, topic, subscription.MessageKeyPrefix, ch)

	return ch, nil
}

func (s *Subscriber) Close() error {
	select {
	case <-s.quit:
	default:
		close(s.quit)
		s.wg.Wait()
	}
	return nil
}

func (s *Subscriber) run(ctx context.Context, topic string, prefix []byte, ch chan<- *message.Message) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer s.wg.Done()

	for {
		if err := s.receiveMessages(ctx, topic, prefix, ch); err != nil {
			s.config.Logger.Error("failed to receive messages", err, watermill.LogFields{
				"topic":        topic,
				"subscription": s.config.Name,
			})
		}

		select {
		case <-time.After(s.config.ReceiveInterval):
			continue
		case <-s.quit:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Subscriber) receiveMessages(ctx context.Context, topic string, prefix []byte, ch chan<- *message.Message) error {
	messages, err := s.getMessages(prefix)
	if err != nil {
		return fmt.Errorf("failed to get messages: %w", err)
	}
	if len(messages) < 1 {
		return nil
	}

	s.config.Logger.Debug("got messages", watermill.LogFields{
		"topic":        topic,
		"subscription": s.config.Name,
		"count":        len(messages),
	})

	for _, message := range messages {
		if err = s.sendMessage(ctx, ch, message); err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
	}

	return nil
}

func (s *Subscriber) getMessages(prefix []byte) ([]rawMessage, error) {
	var messages []rawMessage
	now := time.Now().UTC()

	err := s.db.Update(func(tx *badger.Txn) error {
		iter := tx.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		var count int
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			if !iter.Valid() {
				break
			}

			item := iter.Item()
			key := MessageKey(item.KeyCopy(nil))

			dueAt, err := key.DueAt()
			if err != nil {
				return err
			}
			if dueAt.After(now) {
				break
			}

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			newKey, err := key.Update(dueAt.Add(s.config.VisibilityTimeout))
			if err != nil {
				return err
			}

			if err := tx.Set(newKey, value); err != nil {
				return err
			}

			if err := tx.Delete(key); err != nil {
				return err
			}

			messages = append(messages, rawMessage{key: newKey, value: value})

			count++
			if count >= s.config.ReceiveBatchSize {
				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (s *Subscriber) sendMessage(ctx context.Context, ch chan<- *message.Message, rawMessage rawMessage) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	persistedMessage, err := s.config.Marshaler.Unmarshal(rawMessage.value)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	message := message.NewMessage(persistedMessage.UUID, persistedMessage.Payload)
	message.Metadata = persistedMessage.Metadata
	message.SetContext(ctx)

	select {
	case ch <- message:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.quit:
		return errors.New("subscriber was closed")
	}

	select {
	case <-message.Acked():
		if err = s.ack(rawMessage.key); err != nil {
			return fmt.Errorf("failed to ack: %w", err)
		}
		return nil
	case <-message.Nacked():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.quit:
		return errors.New("subscriber was closed")
	}
}

func (s *Subscriber) ack(rawMessageKey []byte) error {
	return s.db.Update(func(tx *badger.Txn) error {
		return tx.Delete(rawMessageKey)
	})
}

func (c *SubscriberConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = JSONMarshaler{}
	}

	if c.ReceiveInterval < 1 {
		c.ReceiveInterval = time.Second
	}

	if c.ReceiveBatchSize < 1 {
		c.ReceiveBatchSize = 100
	}

	if c.VisibilityTimeout < 1 {
		c.VisibilityTimeout = 5 * time.Second
	}

	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}
