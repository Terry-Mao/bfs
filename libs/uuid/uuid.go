package uuid

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const (
	Size = 16
)

var (
	ErrUUIDSize = errors.New("uuid size error")
)

// New generate a uuid.
func New() (str string, err error) {
	var (
		n    int
		uuid = make([]byte, Size)
	)
	if n, err = io.ReadFull(rand.Reader, uuid); err != nil {
		return
	}
	if n != Size {
		return "", ErrUUIDSize
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	str = fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
	return
}
