package objstore

import (
	"context"
)

var objStoreCtxKey = &(struct{ objStoreCtxKey string }{})

// WithObjStore attaches an object store to the context.
func WithObjStore(ctx context.Context, objStore *ObjectStore) context.Context {
	return context.WithValue(ctx, objStoreCtxKey, objStore)
}

// GetObjStore attempts to return an object store from the context.
func GetObjStore(ctx context.Context) *ObjectStore {
	n := ctx.Value(objStoreCtxKey)
	if n != nil {
		return n.(*ObjectStore)
	}
	return nil
}
