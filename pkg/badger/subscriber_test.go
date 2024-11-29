package badger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestSubscriber_Subscriber(t *testing.T) {
	t.Run("should return an error if the prefix cannot be created", func(t *testing.T) {
		registry := &testRegistry{
			createPrefix: func(topic, subscription string) ([]byte, error) {
				return nil, errTest
			},
		}

		sut := badger.NewSubscriber(testDB, registry, badger.SubscriberConfig{})
		_, err := sut.Subscribe(context.Background(), "topic")
		assertErrorExists(t, err, true)
	})
}

func TestSubscriber_ErrorCases(t *testing.T) {
	registry := newInMemoryRegistry()

	tests := []struct {
		name       string
		registryFn func(err error) badger.Registry
	}{
		{
			name: "should log an error if the sequence cannot be obtained",
			registryFn: func(err error) badger.Registry {
				return &testRegistry{
					getSequence: func(topic string, bandwidth uint64) (badger.Sequence, error) {
						return nil, err
					},
					inner: registry,
				}
			},
		},
		{
			name: "should log an error if the next sequence cannot be obtained",
			registryFn: func(err error) badger.Registry {
				return &testRegistry{
					getSequence: func(topic string, bandwidth uint64) (badger.Sequence, error) {
						return &testSequence{
							next: func() (uint64, error) {
								return 0, err
							},
						}, nil
					},
					inner: registry,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp := errors.New(uuid.NewString())

			logger := watermill.NewCaptureLogger()

			publisher := badger.NewPublisher(testDB, registry, badger.PublisherConfig{})
			defer publisher.Close()

			subscriber := badger.NewSubscriber(testDB, tt.registryFn(exp), badger.SubscriberConfig{
				ReceiveInterval: 10 * time.Millisecond,
				Logger:          logger,
			})
			defer subscriber.Close()

			_, err := subscriber.Subscribe(context.Background(), "topic")
			if !assertNilError(t, err) {
				return
			}

			m := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))
			err = publisher.Publish("topic", m)
			if !assertNilError(t, err) {
				return
			}

			if !waitForLoggerError(logger, exp) {
				t.Errorf("got nil, expected error")
			}
		})
	}
}

func waitForLoggerError(l *watermill.CaptureLoggerAdapter, err error) bool {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)

	for {
		select {
		case <-timeout:
			return false
		case <-ticker.C:
			for _, c := range l.Captured()[watermill.ErrorLogLevel] {
				if errors.Is(c.Err, err) {
					return true
				}
			}
		}
	}
}
