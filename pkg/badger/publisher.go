package badger

import (
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dgraph-io/badger/v4"
)

type (
	// PublisherConfig represents publisher configuration
	// An empty value is valid, using JSON marshaling by default
	PublisherConfig struct {
		Marshaler Marshaler
	}

	// Publisher represents a BadgerDB Watermill publisher
	Publisher struct {
		db       *badger.DB
		registry Registry
		config   PublisherConfig
	}
)

var errEmptyTopic = errors.New("topic is an empty string")

// NewPublisher returns a new publisher using the specified Badger DB
func NewPublisher(db *badger.DB, r Registry, c PublisherConfig) Publisher {
	c.setDefaults()

	return Publisher{
		db:       db,
		registry: r,
		config:   c,
	}
}

// Publish publishes the specified messages
func (p Publisher) Publish(topic string, messages ...*message.Message) error {
	return p.db.Update(func(tx *badger.Txn) error {
		publisher := NewTxPublisher(tx, p.registry, p.config)
		return publisher.Publish(topic, messages...)
	})
}

func (p Publisher) Close() error {
	return nil
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = JSONMarshaler{}
	}
}
