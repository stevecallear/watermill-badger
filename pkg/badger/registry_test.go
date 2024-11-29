package badger_test

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stevecallear/watermill-badger/pkg/badger"
)

type (
	testRegistry struct {
		getSequence  func(topic string, bandwidth uint64) (badger.Sequence, error)
		getPrefixes  func(topic string) ([][]byte, error)
		createPrefix func(topic string, subscription string) ([]byte, error)
		inner        badger.Registry
	}

	testSequence struct {
		next func() (uint64, error)
	}
)

var errTest = errors.New("error")

func (r *testRegistry) GetSequence(topic string, bandwidth uint64) (badger.Sequence, error) {
	if r.getSequence == nil {
		if r.inner != nil {
			return r.inner.GetSequence(topic, bandwidth)
		}
		return nil, errTest
	}
	return r.getSequence(topic, bandwidth)
}

func (r *testRegistry) GetPrefixes(topic string) ([][]byte, error) {
	if r.getPrefixes == nil {
		if r.inner != nil {
			return r.inner.GetPrefixes(topic)
		}
		return nil, errTest
	}
	return r.getPrefixes(topic)
}

func (r *testRegistry) CreatePrefix(topic string, subscription string) ([]byte, error) {
	if r.getPrefixes == nil {
		if r.inner != nil {
			return r.inner.CreatePrefix(topic, subscription)
		}
		return nil, errTest
	}
	return r.createPrefix(topic, subscription)
}

func (s *testSequence) Next() (uint64, error) {
	if s.next == nil {
		return 0, errTest
	}
	return s.next()
}

func (s *testSequence) Release() error {
	return nil
}

func newInMemoryRegistry() badger.Registry {
	return badger.NewInMemoryRegistry(badger.InMemoryRegistryConfig{
		Prefix: uuid.NewString(),
	})
}

func newPersistentRegistry() badger.Registry {
	return badger.NewPersistentRegistry(testDB, badger.PersistentRegistryConfig{
		Prefix: uuid.NewString(),
	})
}

func testRegistry_GetSequence(t *testing.T, newFn func() badger.Registry) {
	r := newFn()

	t.Run("should return the sequence", func(t *testing.T) {
		const bandwidth = 100

		sut, err := r.GetSequence("topic", bandwidth)
		if !assertNilError(t, err) {
			return
		}
		defer sut.Release()

		for i := 0; i < bandwidth; i++ {
			seq, err := sut.Next()
			assertErrorExists(t, err, false)
			assertEqual(t, seq, uint64(i))
		}
	})

	t.Run("should be atomic", func(t *testing.T) {
		const bandwidth = 10000

		sut, err := r.GetSequence("topic", bandwidth)
		if !assertNilError(t, err) {
			return
		}
		defer sut.Release()

		results := make(map[uint64]struct{}, bandwidth)
		mu := new(sync.Mutex)

		wg := new(sync.WaitGroup)
		for i := 0; i < bandwidth; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				seq, _ := sut.Next()

				mu.Lock()
				defer mu.Unlock()
				results[seq] = struct{}{}
			}()
		}

		wg.Wait()
		assertEqual(t, len(results), bandwidth)
	})
}

func testRegistry_GetPrefixes(t *testing.T, newFn func() badger.Registry) {
	tests := []struct {
		name  string
		setup func(*testing.T, badger.Registry) [][]byte
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
			setup: func(t *testing.T, r badger.Registry) [][]byte {
				p1, err := r.CreatePrefix("top", "sub1")
				assertNilError(t, err)

				p2, err := r.CreatePrefix("top", "sub2")
				assertNilError(t, err)

				return [][]byte{p1, p2}
			},
			topic: "top",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sut := newFn()

			var exp [][]byte
			if tt.setup != nil {
				exp = tt.setup(t, sut)
			}

			act, err := sut.GetPrefixes(tt.topic)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			assertDeepEqual(t, act, exp)
		})
	}
}

func testRegistry_CreatePrefix(t *testing.T, newFn func() badger.Registry) {
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
			name:         "should return an error if the prefix is too large",
			topic:        strings.Repeat("t", 128),
			subscription: strings.Repeat("s", 128),
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
		{
			name: "should not create duplicate prefixes",
			setup: func(t *testing.T, r badger.Registry) {
				_, err := r.CreatePrefix("top", "sub")
				assertNilError(t, err)
			},
			topic:        "top",
			subscription: "sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sut := newFn()

			if tt.setup != nil {
				tt.setup(t, sut)
			}

			prefix, err := sut.CreatePrefix(tt.topic, tt.subscription)
			assertErrorExists(t, err, tt.err)
			if err != nil {
				return
			}

			prefixes, err := sut.GetPrefixes(tt.topic)
			if !assertNilError(t, err) {
				return
			}

			assertEqual(t, len(prefixes), 1)

			if act, exp := prefixes[0], prefix; !bytes.Equal(act, exp) {
				t.Errorf("got %v, expected %v", act, exp)
			}
		})
	}
}
