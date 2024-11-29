package badger_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"

	badgerdb "github.com/dgraph-io/badger/v4"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

var testDB *badgerdb.DB

func ExamplePublisher() {
	registry := badger.NewInMemoryRegistry(badger.InMemoryRegistryConfig{})

	subscriber := badger.NewSubscriber(testDB, registry, badger.SubscriberConfig{})
	defer subscriber.Close()

	ch, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		log.Fatal(err)
	}

	publisher := badger.NewPublisher(testDB, registry, badger.PublisherConfig{})
	defer publisher.Close()

	publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), message.Payload("payload")))

	msg := <-ch
	fmt.Println(string(msg.Payload))
	//output: payload
}

func BenchmarkPublisher_InMemory(b *testing.B) {
	benchmarkPublisher(b, badger.NewInMemoryRegistry(badger.InMemoryRegistryConfig{
		Prefix: uuid.NewString(),
	}))
}

func BenchmarkPublisher_Persistent(b *testing.B) {
	benchmarkPublisher(b, badger.NewPersistentRegistry(testDB, badger.PersistentRegistryConfig{
		Prefix: uuid.NewString(),
	}))
}

func benchmarkPublisher(b *testing.B, r badger.Registry) {
	subscriber := badger.NewSubscriber(testDB, r, badger.SubscriberConfig{})
	defer subscriber.Close()

	_, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		log.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		publisher := badger.NewPublisher(testDB, r, badger.PublisherConfig{})
		defer publisher.Close()

		publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), message.Payload("payload")))
	}
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	path := "./db_" + uuid.NewString()
	defer os.RemoveAll(path)

	db, err := badgerdb.Open(badgerdb.DefaultOptions(path))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	testDB = db
	return m.Run()
}

func newMessage(payload string, metadata ...string) *message.Message {
	m := message.NewMessage(watermill.NewUUID(), message.Payload(payload))

	for i := 1; i < len(metadata); i += 2 {
		m.Metadata.Set(metadata[i-1], metadata[i])
	}

	return m
}

func newDelayedMessage(payload string, delayFor time.Duration, metadata ...string) *message.Message {
	m := newMessage(payload, metadata...)
	delay.Message(m, delay.For(delayFor))
	return m
}

func assertErrorExists(t *testing.T, err error, exists bool) bool {
	t.Helper()

	if err != nil && !exists {
		t.Fatalf("got %v, expected nil", err)
		return false
	}
	if err == nil && exists {
		t.Fatalf("got nil, expected error")
		return false
	}

	return true
}

func assertNilError(t *testing.T, err error) bool {
	t.Helper()

	return assertErrorExists(t, err, false)
}

func assertEqual[T comparable](t *testing.T, act, exp T) {
	t.Helper()

	if act != exp {
		t.Errorf("got %v, expected %v", act, exp)
	}
}

func assertDeepEqual(t *testing.T, act, exp any) {
	t.Helper()

	if !reflect.DeepEqual(act, exp) {
		t.Errorf("got %v, expected %v", act, exp)
	}
}

func assertMessageEqual(t *testing.T, act, exp *message.Message) {
	t.Helper()

	actMsg := convertMessage(act)
	expMsg := convertMessage(exp)

	if !reflect.DeepEqual(actMsg, expMsg) {
		t.Errorf("got %#v, expected %#v", actMsg, expMsg)
	}
}

func assertMessagesEqual(t *testing.T, act, exp []*message.Message) {
	t.Helper()

	actMsgs := convertMessages(act)
	expMsgs := convertMessages(exp)

	if !reflect.DeepEqual(actMsgs, expMsgs) {
		t.Errorf("got %#v, expected %#v", actMsgs, expMsgs)
	}
}

func assertMessageReceived(t *testing.T, ch <-chan *message.Message, timeout time.Duration, exp *message.Message, ack bool) {
	t.Helper()

	select {
	case act := <-ch:
		assertMessageEqual(t, act, exp)
		if ack {
			act.Ack()
		} else {
			act.Nack()
		}
	case <-time.After(timeout):
		t.Errorf("timeout waiting for message")
	}
}

type testMessage struct {
	uuid     string
	payload  message.Payload
	metadata message.Metadata
}

func convertMessage(m *message.Message) testMessage {
	md := m.Metadata
	if md == nil {
		md = message.Metadata{}
	}

	return testMessage{
		uuid:     m.UUID,
		payload:  m.Payload,
		metadata: md,
	}
}

func convertMessages(m []*message.Message) []testMessage {
	converted := make([]testMessage, len(m))
	for i, mm := range m {
		converted[i] = convertMessage(mm)
	}

	slices.SortFunc(converted, func(a, b testMessage) int {
		return strings.Compare(a.uuid, b.uuid)
	})

	return converted
}
