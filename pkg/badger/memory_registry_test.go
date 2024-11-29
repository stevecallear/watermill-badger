package badger_test

import (
	"testing"
)

func TestInMemoryRegistry_GetSequence(t *testing.T) {
	testRegistry_GetSequence(t, newInMemoryRegistry)
}

func TestInMemoryRegistry_GetPrefixes(t *testing.T) {
	testRegistry_GetPrefixes(t, newInMemoryRegistry)
}

func TestInMemoryRegistry_CreatePrefix(t *testing.T) {
	testRegistry_CreatePrefix(t, newInMemoryRegistry)
}
