// +build linux
package main

import (
	"syscall"
)

func Fallocate(fd uintptr, mode uint32, off int64, len int64) (err error) {
	return syscall.Fallocate(fd, mode, off, len)
}
