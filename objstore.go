package objstore

import (
	"context"
	"errors"
	"time"

	"github.com/aperturerobotics/pbobject"
	"github.com/aperturerobotics/storageref"
	"github.com/golang/protobuf/proto"
)

// ErrNotFound is a not found error.
var ErrNotFound = errors.New("object not found")

// StoreParams are optional parameters to the Store action.
type StoreParams struct {
	// TTL is the time to live. If zero, lives forever.
	TTL time.Duration
}

// LocalStore is the local cached unencrypted hash blob store.
type LocalStore interface {
	// GetLocal returns an object by digest, assuming it has already been
	// fetched into the decrypted cache. Implemented by the database layer.
	// The digest is of the innermost data of the object, unencrypted,
	// without the multihash header.
	GetLocal(ctx context.Context, digest []byte, obj pbobject.Object) error
	// StoreLocal encodes an object to an unencrypted blob, hashing it with the
	// database hashing scheme. hashPtr is a pointer to the expected unencrypted
	// hash of the data. If the target array is nil, the target will be written
	// with the computed hash and not verified before storing. If the target
	// array is not nil, the hash will be checked before storage.
	StoreLocal(
		ctx context.Context,
		object pbobject.Object,
		hashPtr *[]byte,
		params StoreParams,
	) error
	// DigestData digests the unencrypted data.
	DigestData(data []byte) ([]byte, error)
}

// RemoteStore stores blobs in remote storage.
type RemoteStore interface {
	// FetchRemote returns a blob from blob storage given the storage reference.
	FetchRemote(ctx context.Context, storageRef string, isBlock bool) ([]byte, error)

	// StoreRemote stores a blob in blob storage and returns the storage ref.
	// Additionally, the boolean indicates the object was a single block.
	StoreRemote(ctx context.Context, blob []byte) (string, bool, error)
}

// ObjectStore overlays a remote encrypted-at-rest blob store over the local
// unencrypted hash-based storage.
type ObjectStore struct {
	LocalStore
	RemoteStore

	ctx context.Context
}

// NewObjectStore builds a new object store.
func NewObjectStore(ctx context.Context, localStore LocalStore, remoteStore RemoteStore) *ObjectStore {
	return &ObjectStore{ctx: ctx, LocalStore: localStore, RemoteStore: remoteStore}
}

// GetOrFetch returns an object by hash if it has been fetched into the
// decrypted cache, or attempts to fetch the requested data from the backing
// store (IPFS) given the reference string. This will start OR join a process to
// attempt to fetch this storage ref with this hash.
// TODO: If the function is called multiple times simultaneously, only one
// actual fetch routine will be spawned.
// The multihash code and length must match the database multihash code and
// length or an error is returned.
// The digest is of the innermost data of the object, unencrypted.
func (o *ObjectStore) GetOrFetch(
	ctx context.Context,
	digest []byte,
	storageRef string,
	isBlock bool,
	obj pbobject.Object,
	objWrapper *pbobject.ObjectWrapper,
	encConf pbobject.EncryptionConfig,
) error {
	// Attempt to cache hit the local database.
	getErr := o.GetLocal(ctx, digest, obj)
	if getErr != ErrNotFound || o.RemoteStore == nil {
		return getErr
	}

	// Call out to the remote database as the next layer of caches.
	dat, err := o.FetchRemote(ctx, storageRef, isBlock)
	if err != nil {
		return err
	}
	if dat == nil {
		return ErrNotFound
	}

	// Attempt to decode and decrypt the wrapper.
	if objWrapper == nil {
		objWrapper = &pbobject.ObjectWrapper{}
	}
	if err := proto.Unmarshal(dat, objWrapper); err != nil {
		return err
	}

	if err := objWrapper.DecodeToObject(obj, encConf); err != nil {
		return err
	}

	// Write to the cache the data and confirm the digest.
	// TODO: should store params be a argument?
	return o.StoreLocal(o.ctx, obj, &digest, StoreParams{})
}

// StoreObject digests, seals, encrypts, and stores a object locally and remotely.
// Returns storageRef, object encoded data, and error.
func (o *ObjectStore) StoreObject(
	ctx context.Context,
	obj pbobject.Object,
	encConf pbobject.EncryptionConfig,
) (*storageref.StorageRef, []byte, error) {
	objWrapper, objData, err := pbobject.NewObjectWrapper(obj, encConf)
	if err != nil {
		return nil, nil, err
	}

	objBlob, err := proto.Marshal(objWrapper)
	if err != nil {
		return nil, nil, err
	}

	/*
		digest, err := o.LocalStore.DigestData(objData)
		if err != nil {
			return nil, nil, err
		}
	*/

	var digest []byte
	if err := o.LocalStore.StoreLocal(ctx, obj, &digest, StoreParams{}); err != nil {
		return nil, nil, err
	}

	if o.RemoteStore == nil {
		return &storageref.StorageRef{
			StorageType:  storageref.StorageType_StorageType_DIGEST,
			ObjectDigest: digest,
		}, objData, nil
	}

	storageRef, isBlock, err := o.RemoteStore.StoreRemote(ctx, objBlob)
	if err != nil {
		return nil, nil, err
	}

	refType := storageref.IPFSRefType_IPFSRefType_OBJECT
	if isBlock {
		refType = storageref.IPFSRefType_IPFSRefType_BLOCK
	}

	return &storageref.StorageRef{
		StorageType:  storageref.StorageType_StorageType_IPFS,
		ObjectDigest: digest,
		Ipfs: &storageref.StorageRefIPFS{
			Reference:   storageRef,
			IpfsRefType: refType,
		},
	}, objData, nil
}
