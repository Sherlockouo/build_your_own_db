package btree

import (
	"Build_Your_Own_DB_In_Go/utils"
	"bytes"
	"encoding/binary"
)

/*
	The structure of the btree
	| type | nkeys | pointers | offsets  | key-values
	| 2B   |  2B   | nkeys*8B | nkeys*2B |...
	This is the format of the KV pair. Lengths followed by data.
	| klen | vlen | key | val |
	| 2B   |  2B  | ... | ... |
*/

type BNode struct {
	data []byte
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nkeys(), "idx < node.nkeys()")
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	utils.Assert(idx < node.nkeys(), "idx < node.nkeys()")
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

//• The offset is relative to the position of the first KV pair.
//• The offset of the first KV pair is always zero, so it is not stored in the list.
//• We store the offset to the end of the last KV pair in the offset list, which is used to
//determine the size of the node.

// calc the offset of the pos
func offsetPos(node BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nkeys(), "1 <= idx && idx <= node.nkeys()")
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys(), "idx <= node.nkeys()")
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys(), "idx < node.nkeys()")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys(), "idx <node.nkeys()")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// return the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node, // thus it's always less than or equal to the key.
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
	//
	//nkeys := node.nkeys()
	//l, r := uint16(1), uint16(math.MaxUint16)
	//for l < r && r < nkeys {
	//	mid := uint16((l + r) >> 1)
	//	cmp := bytes.Compare(node.getKey(mid), key)
	//	if cmp == 0 {
	//		return mid
	//	} else if cmp < 0 {
	//		l = mid
	//	} else {
	//		r = mid
	//	}
	//
	//}
	//return l
}

// insert new KVs to newNode's idx position
func leafInsert(
	new BNode,
	old BNode,
	idx uint16,
	key []byte,
	val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(
	new BNode,
	old BNode,
	idx uint16,
	key []byte,
	val []byte) {
	updatePtr := new.getPtr(idx)

	updateIdx := idx

	if idx-1 < 0 {
		updateIdx = 0
	}

	nodeAppendKV(new, updateIdx, updatePtr, key, val)

}

// The nodeAppendRange function copies keys from an old node to a new node.
// append to newNode's dstNew, with oldNode[srcOld:n]
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	utils.Assert(srcOld+n <= old.nkeys(), "srcOld + n <= old.nkeys()")
	utils.Assert(dstNew+n <= new.nkeys(), "dstNew + n <= new.nkeys()")

	if n == 0 {
		return
	}

	// pointers
	for i := uint16(16); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	// offset idx should start from 1
	for i := uint16(1); i < n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

const (

	// HEADER
	HEADER = 4

	// BTREE_PAGE_SIZE the page size is defined to be 4K bytes, larger page size such as 8K or 16K also works
	BTREE_PAGE_SIZE = 4096

	// BTREE_MAX_KEY_SIZE with the constraints that a node with a single KV pair always fits on a single page,\
	// for effeciancy and neat design the max key size
	BTREE_MAX_KEY_SIZE = 1000

	// BRTEE_MAX_VAL_SIZE the max value size
	BRTEE_MAX_VAL_SIZE = 3000
)

type BTree struct {
	root uint64 // pointer (a nonzero page number)

	// callbacks for managing on-disk pages
	get func(uint642 uint64) BNode // dereference a pointer

	new func(node BNode) uint64 // allocate a page

	del func(uint642 uint64) // deallocate a page
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node type!")
	}
	return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)

	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
// split old to left and right
func nodeSplit2(left BNode, right BNode, old BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		right.data = old.data[:BTREE_PAGE_SIZE]
		return
	}
	splitIdx := old.nkeys()
	for i := old.nkeys(); i >= uint16(0); i-- {
		if old.kvPos(i) < BTREE_PAGE_SIZE {
			nodeAppendRange(right, old, 0, 0, i)
			splitIdx = i
			break
		}
	}
	nodeAppendRange(left, old, 0, splitIdx, old.nkeys()-splitIdx)
}

// Inserting keys increase the node size, causing it to exceed the page size. In this case,
// the node is split into multiple smaller nodes.
// split a node if it's too big. the result are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, 1*BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftLeft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftLeft, middle, left)
	utils.Assert(leftLeft.nbytes() <= BTREE_PAGE_SIZE, "leftLeft.nbytes() <= BTREE_PAGE_SIZE")
	return 3, [3]BNode{leftLeft, middle, right}
}

// replace a link with multiple links
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)

}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// where to find the key ?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		// delete the key in the leaf
		newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(newNode, node, idx)
		return newNode
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node type!")
	}
}

// The difference is that we need to merge nodes instead of splitting nodes.
// A node may be merged into one of its left or right siblings.
// The nodeReplace* functions are for updating links.
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{}
	}
	tree.del(kptr)

	newNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}

	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		utils.Assert(updated.nkeys() > 0, "updated.nkeys() > 0")
		nodeReplaceKidN(tree, newNode, node, idx, updated)
	}
	return newNode
}

// merge left right to new Node
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, 0, 0, right.nkeys())
}

// The conditions for merging are:
// 1. The node is smaller than 1/4 of a page (this is arbitrary).
// 2. Has a sibling and the merged result does not exceed one page.
// ? what is the idx stands for; idx is the one which has been deleted
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		// left sibling
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	//
	if idx+1 < node.nkeys() {
		// right sibling
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), node, node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
