package e2e

import (
	"context"
	"testing"

	"github.com/aperturerobotics/inca"
	"github.com/aperturerobotics/objstore"
	"github.com/aperturerobotics/objstore/db/inmem"
	"github.com/aperturerobotics/objstore/ipfs"
	"github.com/aperturerobotics/objstore/localdb"
	"github.com/aperturerobotics/pbobject"
	"github.com/aperturerobotics/storageref"
	"github.com/aperturerobotics/timestamp"

	api "github.com/ipfs/go-ipfs-api"
)

// TestGetOrFetch tests getting or fetching an object from IPFS storage.
// TODO: refactor to possibly use storageref as an argument to GetOrFetch?
func TestGetOrFetch(t *testing.T) {
	ctx := context.Background()
	sh := api.NewLocalShell()
	if sh == nil {
		t.Fatal("unable to connect to local ipfs")
	}

	localStore := localdb.NewLocalDb(inmem.NewInmemDb())
	objStore := objstore.NewObjectStore(ctx, localStore, nil)

	genesisTs := timestamp.Now()
	genesis := &inca.Genesis{
		ChainId:   "test-chain",
		Timestamp: &genesisTs,
	}

	encConf := pbobject.EncryptionConfig{Context: ctx}
	storageRef, objData, err := objStore.StoreObject(ctx, genesis, encConf)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("storage reference: %s", storageRef)
	defer sh.Unpin(storageRef.GetIpfs().GetReference())

	digest, err := localStore.DigestData(objData)
	if err != nil {
		t.Fatal(err.Error())
	}

	outp := &inca.Genesis{}
	if err := objStore.GetOrFetch(
		ctx,
		digest,
		storageRef.GetIpfs().GetReference(),
		storageRef.GetIpfs().GetIpfsRefType() == storageref.IPFSRefType_IPFSRefType_BLOCK,
		outp,
		encConf,
	); err != nil {
		t.Fatal(err.Error())
	}

	t.Logf("retrieved from storage: %s", outp.String())
}
