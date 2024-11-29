package badger_test

import (
	"testing"
)

func TestPersistentRegistry_GetSequence(t *testing.T) {
	testRegistry_GetSequence(t, newPersistentRegistry)

	t.Run("should return an error if the topic is empty", func(t *testing.T) {
		sut := newPersistentRegistry()
		_, err := sut.GetSequence("", 100)
		assertErrorExists(t, err, true)
	})

	t.Run("should return an error if the topic is invalid", func(t *testing.T) {
		sut := newPersistentRegistry()
		_, err := sut.GetSequence("topic with spaces", 100)
		assertErrorExists(t, err, true)
	})
}

func TestPersistentRegistry_GetPrefixes(t *testing.T) {
	testRegistry_GetPrefixes(t, newPersistentRegistry)
}

func TestPersistentRegistry_CreatePrefix(t *testing.T) {
	testRegistry_CreatePrefix(t, newPersistentRegistry)

	t.Run("should return an error if the topic is invalid", func(t *testing.T) {
		sut := newPersistentRegistry()
		_, err := sut.CreatePrefix("topic with spaces", "sub")
		assertErrorExists(t, err, true)
	})

	t.Run("should return an error if the subscription is invalid", func(t *testing.T) {
		sut := newPersistentRegistry()
		_, err := sut.CreatePrefix("top", "subscription with spaces")
		assertErrorExists(t, err, true)
	})
}
