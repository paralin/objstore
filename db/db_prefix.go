package db

import (
	"context"
)

// Prefixer prefixes everything going in and out of a db.
type Prefixer struct {
	db     Db
	prefix []byte
}

// applyPrefix applies the prefix to a key.
func (d *Prefixer) applyPrefix(key []byte) []byte {
	r := make([]byte, len(key)+len(d.prefix))
	copy(r, d.prefix)
	copy(r[len(d.prefix):], key)
	return r
}

// Get retrieves an object from the database.
// Not found should return nil, nil
func (d *Prefixer) Get(ctx context.Context, key []byte) ([]byte, error) {
	return d.db.Get(ctx, d.applyPrefix(key))
}

// Set sets an object in the database.
func (d *Prefixer) Set(ctx context.Context, key []byte, val []byte) error {
	return d.db.Set(ctx, d.applyPrefix(key), val)
}

func (d *Prefixer) List(ctx context.Context, prefix []byte) ([][]byte, error) {
	return d.db.List(ctx, d.applyPrefix(prefix))
}

// WithPrefix adds a prefix to a database.
// Note: calling WithPrefix repeatedly means that they will be applied in reverse order.
// Example:
//    dbm = db.WithPrefix(dbm, []byte("/prefix1"))
//    dbm = db.WithPrefix(dbm, []byte("/prefix2"))
// Key: /prefix1/prefix2/key
func WithPrefix(d Db, prefix []byte) Db {
	return &Prefixer{db: d, prefix: prefix}
}
