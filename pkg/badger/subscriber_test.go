package badger_test

import (
	"context"
	"testing"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestSubscriber_Subscribe(t *testing.T) {
	t.Run("should return an error if the registration cannot be created", func(t *testing.T) {
		registry := &testRegistry{
			registerFn: func(topic, subscription string) (*badger.Subscription, error) {
				return nil, errTest
			},
		}

		sut := badger.NewSubscriber(testDB, registry, badger.SubscriberConfig{})
		_, err := sut.Subscribe(context.Background(), "topic")
		assertErrorExists(t, err, true)
	})
}
