package inmem

import (
	"bytes"
	"context"
	"crypto/sha1"
	"sync"

	"github.com/aperturerobotics/objstore/db"
)

// InmemDb is a in-memory database.
type InmemDb struct {
	m  sync.Map // map[[sha1.Size]byte][]byte // map of values
	mk sync.Map // map[[sha1.Size]byte][]byte // map of keys
}

// NewInmemDb returns a in-memory database.
func NewInmemDb() db.Db {
	return &InmemDb{}
}

// hashKey hashes a key.
func (m *InmemDb) hashKey(key []byte) [sha1.Size]byte {
	return sha1.Sum(key)
}

// Get retrieves an object from the database.
// Not found should return nil, nil
func (m *InmemDb) Get(ctx context.Context, key []byte) ([]byte, error) {
	k := m.hashKey(key)
	obj, ok := m.m.Load(k)
	if !ok {
		return nil, nil
	}

	return obj.([]byte), nil
}

// Set sets an object in the database.
func (m *InmemDb) Set(ctx context.Context, key []byte, val []byte) error {
	k := m.hashKey(key)
	m.m.Store(k, val)
	m.mk.Store(k, key)
	return nil
}

// List returns a list of keys with the specified prefix.
func (m *InmemDb) List(ctx context.Context, prefix []byte) ([][]byte, error) {
	var ks [][]byte
	m.m.Range(func(key interface{}, value interface{}) bool {
		keyHash := key.([sha1.Size]byte)
		key, ok := m.mk.Load(keyHash)
		if !ok {
			return true
		}

		kb := key.([]byte)
		if len(prefix) == 0 || bytes.HasPrefix(kb, prefix) {
			ks = append(ks, kb)
		}
		return true
	})

	return ks, nil
}
