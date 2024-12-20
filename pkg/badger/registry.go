package badger

import (
	"errors"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type (
	// Registry represents a key prefix registry
	Registry interface {
		Register(topic, subscription string) (*Subscription, error)
		Subscriptions(topic string) ([]*Subscription, error)
		Close() error
	}

	Subscription struct {
		Sequence         *badger.Sequence
		MessageKeyPrefix []byte
	}

	// RegistryConfig represents registry configuration
	// An empty value is valid.
	RegistryConfig struct {
		Prefix            string
		SequenceBandwidth uint64
	}

	registry struct {
		db            *badger.DB
		registrations map[string]map[string]struct{}
		subscriptions map[string][]*Subscription
		config        RegistryConfig
		mu            sync.RWMutex
	}
)

// NewRegistry returns a new registry
func NewRegistry(db *badger.DB, c RegistryConfig) Registry {
	c.setDefaults()

	return &registry{
		db:            db,
		registrations: make(map[string]map[string]struct{}),
		subscriptions: make(map[string][]*Subscription),
		config:        c,
	}
}

// Register registers the specified topic/subscription combination
// An error will be returned if the registration already exists.
func (r *registry) Register(topic string, subscription string) (*Subscription, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	_, topicExists := r.registrations[topic]
	if topicExists {
		if _, prefixExists := r.registrations[topic][subscription]; prefixExists {
			return nil, errors.New("registration already exists")
		}
	}

	s, err := r.newSubscription(topic, subscription)
	if err != nil {
		return nil, err
	}

	if topicExists {
		r.registrations[topic][subscription] = struct{}{}
	} else {
		r.registrations[topic] = map[string]struct{}{subscription: {}}
	}

	r.subscriptions[topic] = append(r.subscriptions[topic], s)
	return s, nil
}

// Subscriptions returns all registered subscriptions for the specified topic
func (r *registry) Subscriptions(topic string) ([]*Subscription, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	subscriptions := r.subscriptions[topic]
	return subscriptions, nil
}

// Close releases all sequences and clears the registrations
func (r *registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for topic, subscriptions := range r.subscriptions {
		for _, subscription := range subscriptions {
			if rerr := subscription.Sequence.Release(); rerr != nil {
				err = errors.Join(err, rerr)
			}
		}

		delete(r.subscriptions, topic)
		delete(r.registrations, topic)
	}

	return err
}

func (r *registry) newSubscription(topic, subscription string) (*Subscription, error) {
	s := new(Subscription)

	sequenceKey, err := GenerateSequenceKey(r.config.Prefix, topic, subscription)
	if err != nil {
		return nil, err
	}

	s.Sequence, err = r.db.GetSequence(sequenceKey, r.config.SequenceBandwidth)
	if err != nil {
		return nil, err
	}

	s.MessageKeyPrefix, err = GenerateMessageKeyPrefix(r.config.Prefix, topic, subscription)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (c *RegistryConfig) setDefaults() {
	if c.SequenceBandwidth < 1 {
		c.SequenceBandwidth = 100
	}
}
