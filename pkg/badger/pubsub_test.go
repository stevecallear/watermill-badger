package badger_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestPubSub(t *testing.T) {
	registry := newRegistry()
	defer registry.Close()

	publisher := badger.NewPublisher(testDB, registry, badger.PublisherConfig{})
	defer publisher.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	subscriber := badger.NewSubscriber(testDB, registry, badger.SubscriberConfig{
		ReceiveInterval:   10 * time.Millisecond,
		VisibilityTimeout: 20 * time.Millisecond,
		Logger:            watermill.NewSlogLogger(logger),
	})
	defer subscriber.Close()

	testDelayedPublish(t, publisher, subscriber)
	testVisibilityTimeout(t, publisher, subscriber)
	testFanOut(t, publisher, registry, watermill.NewSlogLogger(logger))
	testLargeBatch(t, publisher, subscriber)
}

func testDelayedPublish(t *testing.T, p message.Publisher, s message.Subscriber) {
	t.Run("should apply publish delay", func(t *testing.T) {
		const topic = "delay"

		ch, err := s.Subscribe(context.Background(), topic)
		if !assertNilError(t, err) {
			return
		}

		exp1 := newDelayedMessage("delayed", time.Second)
		exp2 := newMessage("immediate")

		err = p.Publish(topic, exp1, exp2)
		if !assertNilError(t, err) {
			return
		}

		assertMessageReceived(t, ch, time.Second, exp2, true)
		assertMessageReceived(t, ch, time.Second, exp1, true)
	})
}

func testVisibilityTimeout(t *testing.T, p message.Publisher, s message.Subscriber) {
	t.Run("should apply the visibility timeout", func(t *testing.T) {
		const topic = "visibility"

		ch, err := s.Subscribe(context.Background(), topic)
		if !assertNilError(t, err) {
			return
		}

		exp := newMessage("payload")

		err = p.Publish(topic, exp)
		if !assertNilError(t, err) {
			return
		}

		assertMessageReceived(t, ch, time.Second, exp, false)
		assertMessageReceived(t, ch, time.Second, exp, true)
	})
}

func testFanOut(t *testing.T, p message.Publisher, r badger.Registry, l watermill.LoggerAdapter) {
	const topic = "fanout"

	config := badger.SubscriberConfig{
		ReceiveInterval:   10 * time.Millisecond,
		ReceiveBatchSize:  100,
		VisibilityTimeout: time.Second,
		Logger:            l,
	}

	config1 := config
	config1.Name = "s1"

	config2 := config
	config2.Name = "s2"

	s1 := badger.NewSubscriber(testDB, r, config1)
	defer s1.Close()

	ch1, err := s1.Subscribe(context.Background(), topic)
	if !assertNilError(t, err) {
		return
	}

	s2 := badger.NewSubscriber(testDB, r, config2)
	defer s1.Close()

	ch2, err := s2.Subscribe(context.Background(), topic)
	if !assertNilError(t, err) {
		return
	}

	exp := newMessage("payload")

	err = p.Publish(topic, exp)
	if !assertNilError(t, err) {
		return
	}

	assertMessageReceived(t, ch1, time.Second, exp, true)
	assertMessageReceived(t, ch2, time.Second, exp, true)
}

func testLargeBatch(t *testing.T, p message.Publisher, s message.Subscriber) {
	t.Run("should handle large message batches", func(t *testing.T) {
		const topic = "batch"
		const batchCount = 100
		const batchSize = 100

		ch, err := s.Subscribe(context.Background(), topic)
		if !assertNilError(t, err) {
			return
		}

		batches := make([][]*message.Message, batchCount)
		for i := range batches {
			batches[i] = make([]*message.Message, batchSize)
			for j := range batches[i] {
				batches[i][j] = newMessage(fmt.Sprintf("payload_%d_%d", i, j))
			}

			err = p.Publish(topic, batches[i]...)
			if !assertNilError(t, err) {
				return
			}
		}

		for i := range batches {
			for j := range batches[i] {
				assertMessageReceived(t, ch, time.Second, batches[i][j], true)
			}
		}
	})
}
