package badger_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestGenerateSequenceKey(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		topic        string
		subscription string
		exp          []byte
		err          bool
	}{
		{
			name:         "should return an error if the topic is empty",
			prefix:       "pre",
			topic:        "",
			subscription: "sub",
			err:          true,
		},
		{
			name:         "should permit empty prefix",
			prefix:       "",
			topic:        "top",
			subscription: "sub",
			exp:          []byte("top.sequence.sub"),
		},
		{
			name:         "should permit empty subscription",
			prefix:       "",
			topic:        "top",
			subscription: "",
			exp:          []byte("top.sequence"),
		},
		{
			name:         "should return the key",
			prefix:       "pre",
			topic:        "top",
			subscription: "sub",
			exp:          []byte("pre.top.sequence.sub"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			act, err := badger.GenerateSequenceKey(tt.prefix, tt.topic, tt.subscription)
			assertErrorExists(t, err, tt.err)
			if !bytes.Equal(act, tt.exp) {
				t.Errorf("got %s, expected %s", act, tt.exp)
			}
		})
	}
}

func TestGenerateMessageKeyPrefix(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		topic        string
		subscription string
		exp          []byte
		err          bool
	}{
		{
			name:         "should return an error if the topic is empty",
			prefix:       "pre",
			topic:        "",
			subscription: "sub",
			err:          true,
		},
		{
			name:         "should permit empty prefix",
			prefix:       "",
			topic:        "top",
			subscription: "sub",
			exp:          []byte("top.message.sub"),
		},
		{
			name:         "should permit empty subscription",
			prefix:       "",
			topic:        "top",
			subscription: "",
			exp:          []byte("top.message"),
		},
		{
			name:         "should return the key",
			prefix:       "pre",
			topic:        "top",
			subscription: "sub",
			exp:          []byte("pre.top.message.sub"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			act, err := badger.GenerateMessageKeyPrefix(tt.prefix, tt.topic, tt.subscription)
			assertErrorExists(t, err, tt.err)
			if !bytes.Equal(act, tt.exp) {
				t.Errorf("got %s, expected %s", act, tt.exp)
			}
		})
	}
}

func TestMessageKey_DueAt(t *testing.T) {
	dueAt := time.Unix(0, time.Now().UnixNano()).UTC()

	tests := []struct {
		name string
		sut  badger.MessageKey
		exp  time.Time
		err  bool
	}{
		{
			name: "should return an error if the key is invalid",
			sut:  badger.MessageKey{},
			err:  true,
		},
		{
			name: "should return the due at time",
			sut:  badger.EncodeMessageKey([]byte("prefix"), dueAt, 1),
			exp:  dueAt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			act, err := tt.sut.DueAt()
			assertErrorExists(t, err, tt.err)
			assertEqual(t, act, tt.exp)
		})
	}
}

func TestMessageKey_Update(t *testing.T) {
	dueAt := time.Unix(0, time.Now().UnixNano()).UTC()
	newDueAt := time.Unix(0, time.Now().Add(time.Hour).UnixNano()).UTC()

	tests := []struct {
		name  string
		sut   badger.MessageKey
		dueAt time.Time
		exp   badger.MessageKey
		err   bool
	}{
		{
			name: "should return an error if the key is invalid",
			sut:  badger.MessageKey{},
			err:  true,
		},
		{
			name:  "should update the due at time",
			sut:   badger.EncodeMessageKey([]byte("prefix"), dueAt, 1),
			dueAt: newDueAt,
			exp:   badger.EncodeMessageKey([]byte("prefix"), newDueAt, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated, err := tt.sut.Update(tt.dueAt)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			act := updated[:len(updated)-16]
			exp := tt.exp[:len(tt.exp)-16]

			if !bytes.Equal(act, exp) {
				t.Errorf("got %v, expected %v", act, exp)
			}
		})
	}
}
