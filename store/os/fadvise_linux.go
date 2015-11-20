// +build linux
package os

/*
#define _XOPEN_SOURCE 600
#include <unistd.h>
#include <fcntl.h>
*/
import "C"

import (
	"syscall"
)

const (
	POSIX_FADV_NORMAL     = int(C.POSIX_FADV_NORMAL)
	POSIX_FADV_SEQUENTIAL = int(C.POSIX_FADV_SEQUENTIAL)
	POSIX_FADV_RANDOM     = int(C.POSIX_FADV_RANDOM)
	POSIX_FADV_NOREUSE    = int(C.POSIX_FADV_NOREUSE)
	POSIX_FADV_WILLNEED   = int(C.POSIX_FADV_WILLNEED)
	POSIX_FADV_DONTNEED   = int(C.POSIX_FADV_DONTNEED)
)

func Fadvise(fd uintptr, off int64, size int64, advise int) (err error) {
	var errno int
	if errno = int(C.posix_fadvise(C.int(fd), C.__off_t(off), C.__off_t(size), C.int(advise))); errno != 0 {
		err = syscall.Errno(errno)
	}
	return
}
