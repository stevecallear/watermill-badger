package badger_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestTxPublisher_Publish(t *testing.T) {
	testPublisher_Publish(
		t,
		func(r badger.Registry) (message.Publisher, func(), func() error) {
			tx := testDB.NewTransaction(true)
			return badger.NewTxPublisher(tx, r, badger.PublisherConfig{}), tx.Discard, tx.Commit
		},
	)

	execErrorTest := func(r badger.Registry, m *message.Message) {
		tx := testDB.NewTransaction(true)
		defer tx.Discard()

		p := badger.NewTxPublisher(tx, r, badger.PublisherConfig{})

		err := p.Publish("topic", m)
		assertErrorExists(t, err, true)
	}

	t.Run("should return an error if the prefixes cannot be obtained", func(t *testing.T) {
		m := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))
		execErrorTest(&testRegistry{}, m)
	})

	t.Run("should return an error if the sequence cannot be obtained", func(t *testing.T) {
		m := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))
		execErrorTest(&testRegistry{
			getPrefixes: func(s string) ([][]byte, error) {
				return [][]byte{
					[]byte("prefix"),
				}, nil
			},
		}, m)
	})

	t.Run("should return an error if delay metadata is invalid", func(t *testing.T) {
		m := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))
		m.Metadata.Set(delay.DelayedUntilKey, "invalid")

		execErrorTest(&testRegistry{
			getPrefixes: func(s string) ([][]byte, error) {
				return [][]byte{
					[]byte("prefix"),
				}, nil
			},
			getSequence: func(s string, u uint64) (badger.Sequence, error) {
				return &testSequence{}, nil
			},
		}, m)
	})

	t.Run("should return an error if the next sequence cannot be obtained", func(t *testing.T) {
		m := message.NewMessage(watermill.NewUUID(), message.Payload("payload"))

		execErrorTest(&testRegistry{
			getPrefixes: func(s string) ([][]byte, error) {
				return [][]byte{
					[]byte("prefix"),
				}, nil
			},
			getSequence: func(s string, u uint64) (badger.Sequence, error) {
				return &testSequence{}, nil
			},
		}, m)
	})
}
