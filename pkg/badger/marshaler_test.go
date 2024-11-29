package badger_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestJSONMarshaler(t *testing.T) {
	exp := badger.PersistedMessage{
		UUID:     watermill.NewUUID(),
		Metadata: map[string]string{"key": "value"},
		Payload:  []byte("payload"),
		Created:  time.Now().UTC(),
	}

	sut := badger.JSONMarshaler{}

	var b []byte
	var err error

	t.Run("should marshal the message", func(t *testing.T) {
		b, err = sut.Marshal(exp)
		assertNilError(t, err)
	})

	t.Run("should unmarshal the message", func(t *testing.T) {
		act, err := sut.Unmarshal(b)
		if !assertNilError(t, err) {
			return
		}
		assertDeepEqual(t, act, exp)
	})

	t.Run("should return an error if the bytes are invalid", func(t *testing.T) {
		b := []byte("{")
		_, err = sut.Unmarshal(b)
		assertErrorExists(t, err, true)
	})
}
