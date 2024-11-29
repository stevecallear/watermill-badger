package badger_test

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestPubSub_InMemory(t *testing.T) {
	testPubSub(t, newInMemoryRegistry())
}

func TestPubSub_Persistent(t *testing.T) {
	testPubSub(t, newPersistentRegistry())
}

func testPubSub(t *testing.T, r badger.Registry) {
	p := badger.NewPublisher(testDB, r, badger.PublisherConfig{})
	defer p.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	s := badger.NewSubscriber(testDB, r, badger.SubscriberConfig{
		ReceiveInterval:   100 * time.Millisecond,
		VisibilityTimeout: 2 * time.Second,
		Logger:            watermill.NewSlogLogger(logger),
	})
	defer s.Close()

	testDelayedPublish(t, p, s)
	testVisibilityTimeout(t, p, s)
	testFanOut(t, p, r, watermill.NewSlogLogger(logger))
	testLargeBatch(t, p, s)
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
		assertMessageReceived(t, ch, 5*time.Second, exp1, true)
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
		assertMessageReceived(t, ch, 5*time.Second, exp, true)
	})
}

func testFanOut(t *testing.T, p message.Publisher, r badger.Registry, l watermill.LoggerAdapter) {
	const topic = "fanout"

	config := badger.SubscriberConfig{
		ReceiveInterval:   100 * time.Millisecond,
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
		const batchSize = 1000

		ch, err := s.Subscribe(context.Background(), topic)
		if !assertNilError(t, err) {
			return
		}

		messages := make([]*message.Message, batchSize)
		for i := range messages {
			messages[i] = newMessage("payload_" + strconv.Itoa(i))
		}

		err = p.Publish(topic, messages...)
		if !assertNilError(t, err) {
			return
		}

		for i := 0; i < batchSize; i++ {
			assertMessageReceived(t, ch, 5*time.Second, messages[i], true)
		}
	})
}
