// +build darwin
package os

const (
	FALLOC_FL_KEEP_SIZE = 0x01 /* default is extend size */
)

func Fallocate(fd uintptr, mode uint32, off int64, len int64) (err error) {
	return
}
