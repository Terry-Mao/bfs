// +build linux
package os

import (
	"syscall"
)

func Fallocate(fd uintptr, mode uint32, off int64, len int64) (err error) {
	return syscall.Fallocate(int(fd), mode, off, len)
}
