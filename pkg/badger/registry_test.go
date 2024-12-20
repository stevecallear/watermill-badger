package badger_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/stevecallear/watermill-badger/pkg/badger"
)

func TestInMemoryRegistry_Register(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(*testing.T, badger.Registry)
		topic        string
		subscription string
		err          bool
	}{
		{
			name:         "should return an error if the topic is invalid",
			topic:        "",
			subscription: "sub",
			err:          true,
		},
		{
			name: "should return an error if the registration exists",
			setup: func(t *testing.T, r badger.Registry) {
				_, err := r.Register("top", "sub")
				assertNilError(t, err)
			},
			topic:        "top",
			subscription: "sub",
			err:          true,
		},
		{
			name:         "should create the prefix",
			topic:        "top",
			subscription: "sub",
		},
		{
			name:         "should permit empty subscription",
			topic:        "top",
			subscription: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sut := newRegistry()
			defer sut.Close()

			if tt.setup != nil {
				tt.setup(t, sut)
			}

			subscription, err := sut.Register(tt.topic, tt.subscription)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			subscriptions, err := sut.Subscriptions(tt.topic)
			if !assertNilError(t, err) {
				return
			}

			assertEqual(t, len(subscriptions), 1)
			assertDeepEqual(t, subscriptions[0], subscription)
		})
	}
}

func TestInMemoryRegistry_Subscriptions(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, badger.Registry) []*badger.Subscription
		topic string
		err   bool
	}{
		{
			name:  "should return an error if the topic is empty",
			topic: "",
			err:   true,
		},
		{
			name:  "should return nil if there are no registrations",
			topic: "top",
		},
		{
			name: "should return the channels",
			setup: func(t *testing.T, r badger.Registry) []*badger.Subscription {
				s1, err := r.Register("top", "sub1")
				assertNilError(t, err)

				s2, err := r.Register("top", "sub2")
				assertNilError(t, err)

				return []*badger.Subscription{s1, s2}
			},
			topic: "top",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sut := newRegistry()
			defer sut.Close()

			var exp []*badger.Subscription
			if tt.setup != nil {
				exp = tt.setup(t, sut)
			}

			act, err := sut.Subscriptions(tt.topic)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			assertDeepEqual(t, act, exp)
		})
	}
}

func newRegistry() badger.Registry {
	return badger.NewRegistry(testDB, badger.RegistryConfig{
		Prefix: uuid.NewString(),
	})
}

type testRegistry struct {
	registerFn      func(topic, subscription string) (*badger.Subscription, error)
	subscriptionsFn func(topic string) ([]*badger.Subscription, error)
	inner           badger.Registry
}

var errTest = errors.New("error")

func (r *testRegistry) Register(topic string, subscription string) (*badger.Subscription, error) {
	if r.registerFn == nil {
		if r.inner != nil {
			return r.inner.Register(topic, subscription)
		}
		return nil, errTest
	}
	return r.registerFn(topic, subscription)
}

func (r *testRegistry) Subscriptions(topic string) ([]*badger.Subscription, error) {
	if r.subscriptionsFn == nil {
		if r.inner != nil {
			return r.inner.Subscriptions(topic)
		}
		return nil, errTest
	}
	return r.subscriptionsFn(topic)
}

func (r *testRegistry) Close() error {
	return nil
}
