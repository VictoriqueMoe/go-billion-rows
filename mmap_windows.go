//go:build windows

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

	h, err := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return "", nil, fmt.Errorf("creating file mapping: %v", err)
	}
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		return "", nil, fmt.Errorf("mapping view of file: %v", err)
	}

	cleanup = func() {
		syscall.UnmapViewOfFile(addr)
		syscall.CloseHandle(h)
	}

	data = unsafe.String((*byte)(unsafe.Pointer(addr)), fileSize)

	return data, cleanup, nil
}
