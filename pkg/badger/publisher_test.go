package badger_test

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestPublisher_Publish(t *testing.T) {
	testPublisher_Publish(
		t,
		func(r badger.Registry) (message.Publisher, func(), func() error) {
			return badger.NewPublisher(testDB, r, badger.PublisherConfig{}),
				func() {},
				func() error { return nil }
		},
	)
}

func testPublisher_Publish(
	t *testing.T,
	init func(badger.Registry) (p message.Publisher, discard func(), commit func() error),
) {
	tests := []struct {
		name     string
		pubTopic string
		subTopic string
		messages []*message.Message
		expCount int
		err      bool
	}{
		{
			name:     "should return an error if the topic is empty",
			pubTopic: "",
			subTopic: "topic",
			err:      true,
		},
		{
			name:     "should not error on zero messages",
			pubTopic: "topic",
			subTopic: "topic",
			messages: []*message.Message{},
		},
		{
			name:     "should not error on zero subscriptions",
			pubTopic: "topic",
			subTopic: "another_topic",
			messages: []*message.Message{
				newMessage("payload", "key", "value"),
			},
		},
		{
			name:     "should publish one message",
			pubTopic: "topic",
			subTopic: "topic",
			messages: []*message.Message{
				newMessage("payload", "key", "value"),
			},
			expCount: 1,
		},
		{
			name:     "should publish multiple messages",
			pubTopic: "topic",
			subTopic: "topic",
			messages: []*message.Message{
				newMessage("payload1", "key", "value1"),
				newMessage("payload2", "key", "value2"),
			},
			expCount: 2,
		},
		{
			name:     "should publish delayed messages",
			pubTopic: "topic",
			subTopic: "topic",
			messages: []*message.Message{
				newDelayedMessage("payload", time.Second),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newInMemoryRegistry()
			sut, discard, commit := init(r)
			defer discard()
			defer sut.Close()

			subscriber := badger.NewSubscriber(testDB, r, badger.SubscriberConfig{
				ReceiveInterval: 100 * time.Millisecond,
			})
			defer subscriber.Close()

			ch, err := subscriber.Subscribe(context.Background(), tt.subTopic)
			if !assertNilError(t, err) {
				return
			}

			err = sut.Publish(tt.pubTopic, tt.messages...)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			err = commit()
			if !assertNilError(t, err) {
				return
			}

			var received []*message.Message
			for i := 0; i < tt.expCount; i++ {
				select {
				case m := <-ch:
					received = append(received, m)
					m.Ack()
				case <-time.After(2 * time.Second):
					t.Errorf("timeout waiting for message (%d received)", len(received))
					return
				}
			}

			if tt.expCount > 0 {
				assertMessagesEqual(t, received, tt.messages)
			}
		})
	}
}
