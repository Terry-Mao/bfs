// +build linux
package os

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

func Fadvise(fd uintptr, off int64, size int64, advise int) (err error) {
	var errno int
	if errno = int(C.posix_fadvise(C.int(fd), C.off_t(off), C.off_t(size), C.int(advise))); errno != 0 {
		err = syscall.Errno(errno)
	}
	return
}
