// +build darwin
package os

const (
	POSIX_FADV_NORMAL     = 0
	POSIX_FADV_SEQUENTIAL = 0
	POSIX_FADV_RANDOM     = 0
	POSIX_FADV_NOREUSE    = 0
	POSIX_FADV_WILLNEED   = 0
	POSIX_FADV_DONTNEED   = 0
)

func Fadvise(fd uintptr, off int64, len int64, advise int) (err error) {
	return
}
