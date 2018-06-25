package btree

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/aperturerobotics/objstore"
	"github.com/aperturerobotics/pbobject"
	"github.com/aperturerobotics/storageref"
)

// operationCtx tracks dirty nodes and manages flushing them to the db.
type operationCtx struct {
	ctx        context.Context
	mtx        *sync.Mutex
	flushOnce  sync.Once
	dirtyQueue *queue.PriorityQueue
	memNodeID  uint32
	rootNod    *Root
	root       *memNode
	rootNodRef **storageref.StorageRef
}

// newOperationCtx sets up a new operation context.
func newOperationCtx(
	ctx context.Context,
	opMtx *sync.Mutex,
	rootNod *Root,
	root *memNode,
	rootNodRef **storageref.StorageRef,
) *operationCtx {
	if opMtx != nil {
		opMtx.Lock()
	}

	return &operationCtx{
		ctx:        ctx,
		root:       root,
		mtx:        opMtx,
		dirtyQueue: queue.NewPriorityQueue(0, false),
		rootNod:    rootNod,
		rootNodRef: rootNodRef,
	}
}

// GetNextID returns the next memory node ID.
func (o *operationCtx) GetNextID() uint32 {
	return atomic.AddUint32(&o.memNodeID, 1)
}

// PushDirtyNode pushes a dirty node to the queue.
func (o *operationCtx) PushDirtyNode(n ...*memNode) error {
	for _, ni := range n {
		if err := o.dirtyQueue.Put(ni); err != nil {
			return err
		}
	}

	return nil
}

// GetContext returns the context.
func (o *operationCtx) GetContext() context.Context {
	return o.ctx
}

// Flush flushes the operation context.
func (o *operationCtx) Flush(
	objStore *objstore.ObjectStore,
	encConf pbobject.EncryptionConfig,
	freeList *sync.Pool,
	outErr *error,
) (retErr error) {
	if outErr != nil {
		if e := *outErr; e != nil {
			return e
		}

		defer func() {
			*outErr = retErr
		}()
	}

	ctx := o.ctx
	defer o.dirtyQueue.Dispose()
	defer o.mtx.Unlock()

	for {
		vals, _ := o.dirtyQueue.Get(10)
		if len(vals) == 0 {
			break
		}

		for _, v := range vals {
			mn := v.(*memNode)

			// sweep nil refs
			for i := 0; i < len(mn.node.ChildrenRefs); i++ {
				ref := mn.node.ChildrenRefs[i]
				if ref == nil {
					mn.node.ChildrenRefs[i] = mn.node.ChildrenRefs[len(mn.node.ChildrenRefs)-1]
					mn.node.ChildrenRefs[len(mn.node.ChildrenRefs)-1] = nil
					mn.node.ChildrenRefs = mn.node.ChildrenRefs[:len(mn.node.ChildrenRefs)-1]
					i--
				}
			}

			nodRef, _, err := objStore.StoreObject(ctx, mn.node, encConf)
			if err != nil {
				return err
			}

			if mn.parent == nil {
				// root node
				continue
			}

			pn := mn.parent.node
			pn.ChildrenRefs[mn.parentIdx] = nodRef
			if err := o.PushDirtyNode(mn.parent); err != nil {
				return err
			}

			if freeList != nil {
				freeList.Put(mn.node)
			}
		}
	}

	rootRef, _, err := objStore.StoreObject(ctx, o.root.node, encConf)
	if err != nil {
		return err
	}

	o.rootNod.RootNodeRef = rootRef

	rootNodRef, _, err := objStore.StoreObject(ctx, o.rootNod, encConf)
	if err != nil {
		return err
	}

	*o.rootNodRef = rootNodRef
	return nil
}
