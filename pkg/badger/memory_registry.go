package badger

import (
	"sync"
	"sync/atomic"
)

type (
	// InMemoryRegistryConfig represents registry configuration
	// An empty value is valid. Prefix allows a global prefix to be applied
	// to all message keys.
	InMemoryRegistryConfig struct {
		Prefix string
	}

	// InMemoryRegistry represents an in-memory registry
	InMemoryRegistry struct {
		subscriptions map[string]map[string][]byte
		prefixes      map[string][][]byte
		sequence      uint64
		config        InMemoryRegistryConfig
		mu            sync.RWMutex
	}

	sequenceFunc func() (uint64, error)
)

// NewInMemoryRegistry returns a new in-memory registry
func NewInMemoryRegistry(c InMemoryRegistryConfig) *InMemoryRegistry {
	return &InMemoryRegistry{
		subscriptions: make(map[string]map[string][]byte),
		prefixes:      make(map[string][][]byte),
		config:        c,
	}
}

// GetSequence returns a new sequence for the specified topic and bandwidth
// The provided topic and bandwidth are unused for this implementation.
func (r *InMemoryRegistry) GetSequence(topic string, bandwidth uint64) (Sequence, error) {
	return sequenceFunc(func() (uint64, error) {
		return atomic.AddUint64(&r.sequence, 1) - 1, nil
	}), nil
}

// GetPrefixes returns all registered key prefixes for the specified topic
func (r *InMemoryRegistry) GetPrefixes(topic string) ([][]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	prefixes := r.prefixes[topic]
	return prefixes, nil
}

// CreatePrefix creates registers a key prefix for the specified topic and subscription
// If the specified topic/subscription combination has already been registered, the
// existing key prefix will be returned and a duplicate will not be registered.
func (r *InMemoryRegistry) CreatePrefix(topic string, subscription string) ([]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	_, topicExists := r.subscriptions[topic]
	if topicExists {
		if prefix, prefixExists := r.subscriptions[topic][subscription]; prefixExists {
			return prefix, nil
		}
	}

	prefix, err := EncodeKeyPrefix(r.config.Prefix, topic, subscription)
	if err != nil {
		return nil, err
	}

	if topicExists {
		r.subscriptions[topic][subscription] = prefix
	} else {
		r.subscriptions[topic] = map[string][]byte{subscription: prefix}
	}

	r.prefixes[topic] = append(r.prefixes[topic], prefix)
	return prefix, nil
}

func (fn sequenceFunc) Next() (uint64, error) {
	return fn()
}

func (fn sequenceFunc) Release() error {
	return nil
}
