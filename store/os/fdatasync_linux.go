// +build linux
package os

import (
	"syscall"
)

func Fdatasync(fd uintptr) (err error) {
	return syscall.Fdatasync(int(fd))
}
