package inmem

import (
	"bytes"
	"context"

	"github.com/Workiva/go-datastructures/trie/ctrie"
	"github.com/aperturerobotics/objstore/db"
)

// InmemDb is a in-memory database.
type InmemDb struct {
	ct *ctrie.Ctrie
}

// NewInmemDb returns a in-memory database.
func NewInmemDb() db.Db {
	return &InmemDb{
		ct: ctrie.New(nil),
	}
}

// Get retrieves an object from the database.
// Not found should return nil, nil
func (m *InmemDb) Get(ctx context.Context, key []byte) ([]byte, error) {
	obj, ok := m.ct.Lookup(key)
	if !ok {
		return nil, nil
	}

	return obj.([]byte), nil
}

// Set sets an object in the database.
func (m *InmemDb) Set(ctx context.Context, key []byte, val []byte) error {
	m.ct.Insert(key, val)
	return nil
}

// List returns a list of keys with the specified prefix.
func (m *InmemDb) List(ctx context.Context, prefix []byte) ([][]byte, error) {
	entryCh := m.ct.Iterator(ctx.Done())
	var ks [][]byte
	for entry := range entryCh {
		key := entry.Key
		if len(prefix) == 0 || bytes.HasPrefix(key, prefix) {
			ks = append(ks, key)
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return ks, nil
	}
}

// Delete deletes a set of keys from the database.
func (m *InmemDb) Delete(ctx context.Context, keys ...[]byte) error {
	for _, key := range keys {
		m.ct.Remove(key)
	}

	return nil
}
