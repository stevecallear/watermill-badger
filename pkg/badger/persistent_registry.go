package badger

import (
	"bytes"
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

type (
	// InMemoryRegistryConfig represents registry configuration
	// An empty value is valid. Prefix allows a global prefix to be applied
	// to all message keys.
	PersistentRegistryConfig struct {
		Prefix              string
		GenerateSequenceKey GenerateKeyFunc
		GeneratePrefixesKey GenerateKeyFunc
	}

	// GenerateKeyFunc represents a key generation func
	GenerateKeyFunc func(prefix, topic string) []byte

	// InMemoryRegistry represents a persistent registry
	PersistentRegistry struct {
		db     *badger.DB
		config PersistentRegistryConfig
	}
)

// NewInMemoryRegistry returns a new persistent registry
func NewPersistentRegistry(db *badger.DB, c PersistentRegistryConfig) *PersistentRegistry {
	c.setDefaults()

	return &PersistentRegistry{
		db:     db,
		config: c,
	}
}

// GetSequence returns a new persistent sequence for the specified topic and bandwidth
func (r *PersistentRegistry) GetSequence(topic string, bandwidth uint64) (Sequence, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	if err := r.validateKeyComponents(topic); err != nil {
		return nil, err
	}

	key := r.config.GenerateSequenceKey(r.config.Prefix, topic)
	return r.db.GetSequence(key, bandwidth)
}

// GetPrefixes returns all registered key prefixes for the specified topic
func (r *PersistentRegistry) GetPrefixes(topic string) ([][]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	var prefixes [][]byte
	err := r.db.View(func(tx *badger.Txn) error {
		key := r.config.GeneratePrefixesKey(r.config.Prefix, topic)

		var err error
		prefixes, err = r.getPrefixes(tx, key)
		return err
	})
	if err != nil {
		return nil, err
	}

	return prefixes, nil
}

// CreatePrefix creates registers a key prefix for the specified topic and subscription
// If the specified topic/subscription combination has already been registered, the
// existing key prefix will be returned and a duplicate will not be registered.
func (r *PersistentRegistry) CreatePrefix(topic string, subscription string) ([]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	if err := r.validateKeyComponents(topic, subscription); err != nil {
		return nil, err
	}

	prefix, err := EncodeKeyPrefix(r.config.Prefix, topic, subscription)
	if err != nil {
		return nil, err
	}

	err = r.db.Update(func(tx *badger.Txn) error {
		key := r.config.GeneratePrefixesKey(r.config.Prefix, topic)
		prefixes, err := r.getPrefixes(tx, key)
		if err != nil {
			return err
		}

		for _, p := range prefixes {
			if bytes.Equal(prefix, p) {
				return nil
			}
		}

		prefixes = append(prefixes, prefix)

		return tx.Set(key, bytes.Join(prefixes, []byte(" ")))
	})
	if err != nil {
		return nil, err
	}

	return prefix, nil
}

func (r *PersistentRegistry) validateKeyComponents(components ...string) error {
	for _, c := range components {
		if strings.Contains(c, " ") {
			return errors.New("invalid prefix component")
		}
	}
	return nil
}

func (r *PersistentRegistry) getPrefixes(tx *badger.Txn, key []byte) ([][]byte, error) {
	item, err := tx.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return bytes.Split(value, []byte(" ")), nil
}

func (c *PersistentRegistryConfig) setDefaults() {
	if c.GenerateSequenceKey == nil {
		c.GenerateSequenceKey = generateKeyFn("sequence")
	}

	if c.GeneratePrefixesKey == nil {
		c.GeneratePrefixesKey = generateKeyFn("prefixes")
	}
}

func generateKeyFn(name string) GenerateKeyFunc {
	return func(prefix, topic string) []byte {
		s := topic + "_" + name
		if prefix != "" {
			s = prefix + "_" + s
		}
		return []byte(s)
	}
}
