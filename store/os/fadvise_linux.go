package os

// +build linux

/*
#define _XOPEN_SOURCE 600
#include <fcntl.h>
*/
import "C"

import (
	"syscall"
)

const (
	POSIX_FADV_DONTNEED = int(C.POSIX_FADV_DONTNEED)
)

func Fadvise(fd uintptr, off int64, len int64, advise int) error {
	return syscall.Errno(C.posix_fadvise(C.int(fd), C.__off_t(off), C.__off_t(len), C.int(advise)))
}
