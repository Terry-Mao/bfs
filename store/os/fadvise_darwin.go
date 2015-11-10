// +build darwin
package os

const (
	POSIX_FADV_DONTNEED = 0
)

func Fadvise(fd uintptr, off int64, len int64, advise int) (err error) {
	return
}
