//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmapFile(file *os.File) (data string, cleanup func(), err error) {
	fi, err := file.Stat()
	if err != nil {
		return "", nil, err
	}
	fileSize := fi.Size()

	b, err := syscall.Mmap(
		int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED,
	)
	if err != nil {
		return "", nil, err
	}

	cleanup = func() {
		if err = syscall.Munmap(b); err != nil {
			fmt.Printf("ERR: unmapping file: %v\n", err)
		}
	}

	return unsafe.String(&b[0], len(b)), cleanup, nil
}
