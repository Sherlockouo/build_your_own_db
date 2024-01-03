package kv

import (
	"Build_Your_Own_DB_In_Go/btree"
	"Build_Your_Own_DB_In_Go/utils"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// create the initial mmap that covers the whole file
func mmapInit(fp *os.File) (int, []byte, error) {
	file, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if file.Size()%btree.BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not multiple of page size.")
	}

	mmapSize := 64 << 20
	utils.Assert(mmapSize%btree.BTREE_PAGE_SIZE == 0, "mmapSize % btree.BTREE_PAGE_SIZE == 0")
	for mmapSize < int(file.Size()) {
		mmapSize <<= 2
	}
	// mmapSize can be larger than the file
	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(file.Size()), chunk, nil
}
