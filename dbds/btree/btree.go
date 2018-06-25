package btree

import (
	"context"
	"sort"
	"sync"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/aperturerobotics/objstore"
	"github.com/aperturerobotics/pbobject"
	"github.com/aperturerobotics/storageref"
)

// maxNodeChildren is the maximum number children nodes of a node.
const maxNodeChildren = 16

// BTree is an implementation of a objstore backed BTree.
// The key is a string, and the value is a storageref.
type BTree struct {
	mtx sync.Mutex

	objStore   *objstore.ObjectStore
	rootNodRef *storageref.StorageRef
	rootNod    *Root
	root       *memNode
	encConf    pbobject.EncryptionConfig

	opCtx    *operationCtx
	freeList sync.Pool
}

// NewBTree builds a new btree, writing state to the db.
// Any errors writing initial state will be returned.
func NewBTree(
	ctx context.Context,
	objStore *objstore.ObjectStore,
	encConf pbobject.EncryptionConfig,
) (*BTree, error) {
	rootNode := &Node{}
	rootNode.Leaf = true
	rootRef, _, err := objStore.StoreObject(ctx, rootNode, encConf)
	if err != nil {
		return nil, err
	}

	bt := &BTree{
		objStore: objStore,
		encConf:  encConf,
		freeList: sync.Pool{New: func() interface{} { return &Node{} }},
	}

	rootMemNod := bt.newNode()
	rootMemNod.node = rootNode
	bt.root = rootMemNod

	rootNod := &Root{
		RootNodeRef: rootRef,
	}
	rootNodRef, _, err := objStore.StoreObject(ctx, rootNod, encConf)
	if err != nil {
		return nil, err
	}
	bt.rootNod = rootNod
	bt.rootNodRef = rootNodRef

	return bt, nil
}

// LoadBTree loads a b tree by following a storage reference.
func LoadBTree(
	ctx context.Context,
	objStore *objstore.ObjectStore,
	encConf pbobject.EncryptionConfig,
	rootRef *storageref.StorageRef,
) (*BTree, error) {
	ctx = objstore.WithObjStore(ctx, objStore)
	rootNod := &Root{}
	if err := rootRef.FollowRef(ctx, nil, rootNod, nil); err != nil {
		return nil, err
	}

	bt := &BTree{
		objStore:   objStore,
		rootNod:    rootNod,
		rootNodRef: rootRef,
		encConf:    encConf,
		freeList:   sync.Pool{New: func() interface{} { return &Node{} }},
	}

	rootMemNode, err := bt.followNodeRef(rootNod.GetRootNodeRef())
	if err != nil {
		return nil, err
	}
	bt.root = rootMemNode

	return bt, nil
}

// Len returns the number of items in the tree.
func (b *BTree) Len() int {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return int(b.rootNod.GetLength())
}

// newNode builds a new node.
func (b *BTree) newNode() *memNode {
	fget := b.freeList.Get()
	var n *Node
	if fget != nil {
		n = fget.(*Node)
		n.Reset()
	} else {
		n = &Node{}
	}

	var parentIdx int
	var depth int // parent == nil -> depth=0 root

	mn := &memNode{
		id:    b.opCtx.GetNextID(),
		depth: depth,
		node:  n,
	}
	b.opCtx.PushDirtyNode(mn)
	return mn
}

// GetRootNodeRef returns the reference to the root node.
func (b *BTree) GetRootNodeRef() *storageref.StorageRef {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.rootNodRef
}

// ReplaceOrInsert replaces or inserts an item, if replacing, returns the value.
func (b *BTree) ReplaceOrInsert(
	ctx context.Context,
	key string,
	val *storageref.StorageRef,
) (rref *storageref.StorageRef, rerr error) {
	if key == "" {
		return nil, nil
	}

	item := &Item{Key: key, Ref: val}
	b.opCtx = newOperationCtx(ctx, &b.mtx, b.rootNod, b.root, &b.rootNodRef)
	defer b.opCtx.Flush(b.objStore, b.encConf, &b.freeList, &rerr)

	if b.rootNod.Length == 0 {
		b.root.node.Items = append(b.root.node.Items, item)
		b.rootNod.Length++
		return nil, nil
	}

	if len(b.root.node.Items) >= b.maxItems() {
		i2, s := b.splitNode(b.root, b.maxItems()/2)
		oldRoot := b.root

		b.root = b.newNode()
		b.root.node.Items = append(b.root.node.Items, i2)
		b.root.node.ChildrenRefs = []*storageref.StorageRef{nil, nil}

		oldRoot.setParent(b.root, 0)
		s.setParent(b.root, 1)

		b.opCtx.PushDirtyNode(s, oldRoot)
	}

	out, err := b.insertToNode(b.root, item, b.maxItems())
	if err != nil {
		return nil, err
	}

	if out == nil {
		b.rootNod.Length++
	}

	return out.GetRef(), nil
}

// insertToNode inserts an item as a child of this node, making sure no nodes in
// the subtree exceed maxItems items. If an equivalent item is
// found/replaced by insert, it will be returned.
func (b *BTree) insertToNode(n *memNode, item *Item, maxItems int) (*Item, error) {
	defer b.opCtx.PushDirtyNode(n)

	i, found := b.findInNode(n, item)
	if found {
		out := n.node.Items[i]
		n.node.Items[i] = item
		return out, nil
	}

	if len(n.node.GetChildrenRefs()) == 0 {
		b.insertInNodeAtIdx(n, item, i)
		return nil, nil
	}

	cii := i
	ci, err := b.followChildRef(n, i)
	if err != nil {
		return nil, err
	}

	if b.maybeSplitNodeChild(n, ci, i, maxItems) {
		inTree := n.node.Items[i]
		switch {
		case item.Less(inTree):
			// no change
		case inTree.Less(item):
			i++
		default:
			out := n.node.Items[i]
			n.node.Items[i] = item
			return out, nil
		}
	}

	if cii != i {
		ci, err = b.followChildRef(n, i)
		if err != nil {
			return nil, err
		}
	}

	return b.insertToNode(ci, item, maxItems)
}

// maybeSplitNodeChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred
func (b *BTree) maybeSplitNodeChild(n, iChild *memNode, i, maxItems int) bool {
	if len(iChild.node.Items) < maxItems {
		return false
	}

	item, second := b.splitNode(iChild, maxItems/2)
	b.insertInNodeAtIdx(n, item, i)
	b.insertChildInNodeAtIdx(n, second, i+1)

	b.opCtx.PushDirtyNode(iChild, second, n)
	return true
}

// splitNode splits the given node at the given index. The current node shrinks.
// This function returns the item that existed at that index and a new node
// containing all items / children after it.
func (b *BTree) splitNode(n *memNode, i int) (*Item, *memNode) {
	item := n.node.Items[i]
	next := b.newNode()
	next.node.Items = append(next.node.Items, n.node.Items[i+1:]...)
	n.node.Items = n.node.Items[:i]

	if len(n.node.ChildrenRefs) > 0 {
		next.node.ChildrenRefs = append(next.node.ChildrenRefs, n.node.ChildrenRefs[i+1:]...)
		n.node.ChildrenRefs = n.node.ChildrenRefs[:i]
	}

	return item, next
}

// insertChildInNodeAtIdx inserts a child in a node at an index.
func (b *BTree) insertChildInNodeAtIdx(n, child *memNode, i int) {
	s := n.node.ChildrenRefs
	if n.loadedChildren != nil {
		nextLoadedChildren := make(map[int]*memNode, len(n.loadedChildren))
		dirty := false
		for lci, lc := range n.loadedChildren {
			if lci >= i {
				lc.parentIdx++
			}
			nextLoadedChildren[lc.parentIdx] = lc
			dirty = true
		}
		if dirty {
			n.loadedChildren = nextLoadedChildren
		}
	}
	s = append(s, nil)
	if i < len(s) {
		copy(s[i+1:], s[i:])
	}
	s[i] = nil
	child.setParent(n, i)
	n.node.ChildrenRefs = s
}

// insertInNodeAtIdx inserts an item in a node at an index.
func (b *BTree) insertInNodeAtIdx(n *memNode, item *Item, i int) {
	s := n.node.Items
	s = append(s, nil)
	if i < len(s) {
		copy(s[i+1:], s[i:])
	}
	s[i] = item
	n.node.Items = s
}

// removeInNodeAtIdx removes an item in a node at an index.
func (b *BTree) removeInNodeAtIdx(n *memNode, idx int) *Item {
	s := n.node.Items
	item := s[idx]
	copy(s[idx:], s[idx+1:])
	s[len(s)-1] = nil
	s = s[:len(s)-1]
	return item
}

// findInNode finds where an item should be inserted/replaced in a node.
func (b *BTree) findInNode(n *memNode, item *Item) (index int, found bool) {
	s := n.node.Items
	i := sort.Search(len(s), func(i int) bool {
		return item.Less(s[i])
	})
	if i > 0 && s[i-1].GetKey() == item.GetKey() {
		return i - 1, true
	}
	return i, false
}

// followChildRef looks up the child at the index.
func (b *BTree) followChildRef(n *memNode, i int) (*memNode, error) {
	if n.loadedChildren != nil {
		if c, ok := n.loadedChildren[i]; ok {
			return c, nil
		}
	}

	mn, err := b.followNodeRef(n.node.ChildrenRefs[i])
	if err != nil {
		return nil, err
	}

	mn.setParent(n, i)
	return mn, nil
}

// followNodeRef loads a node reference into memory.
func (b *BTree) followNodeRef(ref *storageref.StorageRef) (*memNode, error) {
	if ref.IsEmpty() {
		return nil, nil
	}

	ctx := objstore.WithObjStore(b.opCtx.ctx, b.objStore)
	n := b.newNode()
	if err := ref.FollowRef(ctx, nil, n.node, nil); err != nil {
		return nil, err
	}

	return n, nil
}

// memNode is a node in memory.
type memNode struct {
	id             uint32
	parent         *memNode
	parentIdx      int
	depth          int
	node           *Node
	loadedChildren map[int]*memNode
}

// assertLoadedChildren asserts that loaded children is set.
func (m *memNode) assertLoadedChildren() {
	if m.loadedChildren == nil {
		m.loadedChildren = make(map[int]*memNode)
	}
}

// setParent links the memnode to its parent.
func (m *memNode) setParent(parent *memNode, i int) {
	parent.assertLoadedChildren()
	parent.loadedChildren[i] = m
	m.parent = parent
	m.parentIdx = i
	m.depth = parent.depth + 1
}

// Compare returns a bool that can be used to determine
// ordering in the priority queue.  Assuming the queue
// is in ascending order, this should return > logic.
// Return 1 to indicate this object is greater than the
// the other logic, 0 to indicate equality, and -1 to indicate
// less than other.
func (n *memNode) Compare(other queue.Item) int {
	on := other.(*memNode)
	depthCmp := n.depth - on.depth
	if depthCmp == 0 {
		return int(n.id) - int(on.id)
	}

	return depthCmp
}

// maxItems is the max number of items to store in a node
func (t *BTree) maxItems() int {
	degree := 2
	return degree*2 - 1
}

// Less compares two items.
func (i *Item) Less(o *Item) bool {
	return i.GetKey() < o.GetKey()
}
