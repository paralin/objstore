package db

import (
	"context"
)

// Db is an implementation of a key-value database.
type Db interface {
	// Get retrieves an object from the database.
	// Not found should return nil, nil
	Get(ctx context.Context, key []byte) ([]byte, error)
	// Set sets an object in the database.
	Set(ctx context.Context, key []byte, val []byte) error
	// List returns a list of keys with the specified prefix.
	List(ctx context.Context, prefix []byte) ([][]byte, error)
	// Delete clears a set of keys from the db.
	// Not found should not return an error.
	Delete(ctx context.Context, keys ...[]byte) error
}
