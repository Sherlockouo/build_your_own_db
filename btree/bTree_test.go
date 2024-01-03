package btree

import (
	"Build_Your_Own_DB_In_Go/utils"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) get(key string) {
	res := c.tree.Get([]byte(key))
	println("res {}", string(res))
}
func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				utils.Assert(ok, "ok ?")
				return node
			},
			new: func(node BNode) uint64 {
				utils.Assert(node.nbytes() <= BTREE_PAGE_SIZE, "node.nbytes() <= BTREE_PAGE_SIZE")
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				utils.Assert(pages[key].data == nil, "pages[key].data == nil")
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				utils.Assert(ok, "ok ")
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func TestBTree_Insert(t *testing.T) {
	c := newC()
	c.add("a", "b")
	c.get("a")
}

func TestBTree_Delete(t *testing.T) {
	c := newC()
	c.add("a", "this a description")
	c.get("a")
	c.del("a")
	c.get("a")
}
