package kv

import (
	"Build_Your_Own_DB_In_Go/btree"
	"fmt"
	"os"
	"syscall"
)

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree btree.BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmaap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can bo non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*btree.BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}
