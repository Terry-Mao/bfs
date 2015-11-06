// +build darwin
package os

func Fallocate(fd uintptr, mode uint32, off int64, len int64) (err error) {
	return
}
