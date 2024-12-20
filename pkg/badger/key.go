package badger

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	sequenceIdentifier = "sequence"
	messageIdentifier  = "message"
)

func GenerateSequenceKey(prefix, topic, subscription string) ([]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	key := topic + "." + sequenceIdentifier
	key = applyKeyPrefix(key, prefix)
	key = applyKeySuffix(key, subscription)

	return []byte(key), nil
}

func GenerateMessageKeyPrefix(prefix, topic, subscription string) ([]byte, error) {
	if topic == "" {
		return nil, errEmptyTopic
	}

	key := topic + "." + messageIdentifier
	key = applyKeyPrefix(key, prefix)
	key = applyKeySuffix(key, subscription)

	return []byte(key), nil
}

func applyKeyPrefix(key, prefix string) string {
	if prefix != "" {
		key = prefix + "." + key
	}
	return key
}

func applyKeySuffix(key, suffix string) string {
	if suffix != "" {
		key = key + "." + suffix
	}
	return key
}

type MessageKey []byte

func EncodeMessageKey(prefix []byte, dueAt time.Time, seq uint64) MessageKey {
	prefixLen := len(prefix)

	encoded := make([]byte, prefixLen+8+8+16)
	encoded[0] = uint8(prefixLen)

	copy(encoded[:prefixLen], []byte(prefix))
	binary.BigEndian.PutUint64(encoded[prefixLen:prefixLen+8], uint64(dueAt.UnixNano()))
	binary.BigEndian.PutUint64(encoded[prefixLen+8:prefixLen+16], uint64(seq))

	random := uuid.New()
	copy(encoded[prefixLen+16:], random[:])

	return encoded
}

func (k MessageKey) DueAt() (time.Time, error) {
	if err := k.validate(); err != nil {
		return time.Time{}, err
	}
	keyLen := len(k)
	nanos := binary.BigEndian.Uint64(k[keyLen-32 : keyLen-24])

	return time.Unix(0, int64(nanos)).UTC(), nil
}

func (k MessageKey) Update(dueAt time.Time) (MessageKey, error) {
	if err := k.validate(); err != nil {
		return k, err
	}

	keyCopy := make(MessageKey, len(k))
	copy(keyCopy, k)

	keyLen := len(keyCopy)
	binary.BigEndian.PutUint64(keyCopy[keyLen-32:keyLen-24], uint64(dueAt.UnixNano()))

	return keyCopy, nil
}

func (k MessageKey) validate() error {
	if len(k) < 34 { // 0 + 1 + 1 + 8 + 8 + 16
		return errors.New("invalid key")
	}
	return nil
}
