// +build linux
package os

/*
#define _GNU_SOURCE
#include <fcntl.h>
#include <linux/falloc.h>
*/
import "C"

import (
	"syscall"
)

const (
	FALLOC_FL_KEEP_SIZE = uint32(C.FALLOC_FL_KEEP_SIZE)
)

func Fallocate(fd uintptr, mode uint32, off int64, size int64) error {
	return syscall.Fallocate(int(fd), mode, off, size)
}
