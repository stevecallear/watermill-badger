package badger

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

type key []byte

func encodeKey(prefix []byte, dueAt time.Time, seq uint64) key {
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

func (k key) dueAt() time.Time {
	keyLen := len(k)
	nanos := binary.BigEndian.Uint64(k[keyLen-32 : keyLen-24])

	return time.Unix(0, int64(nanos))
}

func (k key) copy(dueAt time.Time, seq uint64) key {
	keyCopy := make(key, len(k))
	copy(keyCopy, k)

	keyLen := len(keyCopy)

	binary.BigEndian.PutUint64(keyCopy[keyLen-32:keyLen-24], uint64(dueAt.UnixNano()))
	binary.BigEndian.PutUint64(keyCopy[keyLen-24:keyLen-16], seq)

	return keyCopy
}

func (k key) validate() error {
	if len(k) < 34 { // 0 + 1 + 1 + 8 + 8 + 16
		return errors.New("invalid key")
	}
	return nil
}
