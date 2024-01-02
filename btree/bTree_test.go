package btree

import "testing"

func TestPanic(t *testing.T) {
	b := &BNode{}
	b.getPtr(12)
}
