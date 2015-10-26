// +build darwin
package main

func Fallocate(fd uintptr, mode uint32, off int64, len int64) (err error) {
	return
}
