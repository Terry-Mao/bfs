// +build darwin
package os

const (
	SYNC_FILE_RANGE_WAIT_BEFORE = 1
	SYNC_FILE_RANGE_WRITE       = 2
	SYNC_FILE_RANGE_WAIT_AFTER  = 4
)

func Syncfilerange(fd uintptr, off int64, n int64, flags int) (err error) {
	return
}
