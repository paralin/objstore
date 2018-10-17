package btree

import (
	"context"
	"testing"

	"github.com/aperturerobotics/objstore"
	"github.com/aperturerobotics/objstore/db/inmem"
	"github.com/aperturerobotics/objstore/localdb"
	"github.com/aperturerobotics/pbobject"
	"github.com/aperturerobotics/storageref"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	ctx := context.Background()
	localStore := localdb.NewLocalDb(inmem.NewInmemDb())
	objStore := objstore.NewObjectStore(ctx, localStore, nil)

	bt, err := NewBTree(ctx, objStore, pbobject.EncryptionConfig{})
	assert.NoError(t, err)

	key := "test"
	val := ((*storageref.StorageRef)(nil))
	iv, err := bt.ReplaceOrInsert(ctx, key, val)
	assert.NoError(t, err)
	assert.Nil(t, iv)

	iv, err = bt.ReplaceOrInsert(ctx, key, val)
	assert.NoError(t, err)
	assert.NotNil(t, iv)
}
