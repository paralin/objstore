package ipfs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/aperturerobotics/objstore"
	api "github.com/ipfs/go-ipfs-api"
)

// MaxBlockSize is the maximum size of a single block before we use the merkledag.
// 256kb is also the block size used by IPFS.
const MaxBlockSize = 1024 * 256

// RemoteStore implements objstore.RemoteStore
type RemoteStore struct {
	shell *api.Shell
}

// NewRemoteStore builds a new remote store.
func NewRemoteStore(sh *api.Shell) *RemoteStore {
	return &RemoteStore{shell: sh}
}

type noopCloser struct {
	io.Reader
}

// Close noop.
func (n noopCloser) Close() error {
	return nil
}

// FetchRemote returns a blob from blob storage given the storage reference string.
func (r *RemoteStore) FetchRemote(ctx context.Context, storageRef string, isBlock bool) ([]byte, error) {
	if isBlock {
		return r.shell.BlockGet(storageRef)
	}

	rc, err := r.shell.Cat(storageRef)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return ioutil.ReadAll(rc)
}

// StoreRemote stores a blob in blob storage and returns the storage reference string and if the data was stored as a block.
func (r *RemoteStore) StoreRemote(ctx context.Context, blob []byte) (string, bool, error) {

	if len(blob) <= MaxBlockSize {
		ref, err := r.shell.BlockPut(blob, "raw", "sha2-256", -1)
		return ref, true, err
	}

	ref, err := r.shell.Add(bytes.NewReader(blob))
	return ref, false, err
}

// _ is a type assertion
var _ objstore.RemoteStore = &RemoteStore{}
