package badger

import (
	"errors"
)

type (
	// Registry represents a key prefix registry
	Registry interface {
		GetSequence(topic string, bandwidth uint64) (Sequence, error)
		GetPrefixes(topic string) ([][]byte, error)
		CreatePrefix(topic, subscription string) ([]byte, error)
	}

	// Sequence represents an incrementing uint64 sequence
	Sequence interface {
		Next() (uint64, error)
		Release() error
	}
)

// EncodeKeyPrefix returns a new key prefix for the specified parameters
func EncodeKeyPrefix(prefix, topic, subscription string) ([]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	key := topic
	if subscription != "" {
		key = key + "_" + subscription
	}

	keyLen := len(key)
	if keyLen < 1 || keyLen > 255 {
		return nil, errors.New("invalid key prefix")
	}

	prefixLen := len(prefix)

	encoded := make([]byte, prefixLen+1+keyLen)
	copy(encoded[:prefixLen], prefix)
	encoded[prefixLen] = uint8(keyLen)
	copy(encoded[prefixLen+1:], key)

	return encoded, nil
}
