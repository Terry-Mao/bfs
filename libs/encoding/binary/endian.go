package binary

import (
	"bufio"
)

var BigEndian bigEndian

type bigEndian struct{}

func (bigEndian) Uint16(b []byte) uint16 { return uint16(b[1]) | uint16(b[0])<<8 }

func (bigEndian) PutUint16(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func (bigEndian) Int32(b []byte) int32 {
	return int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0])<<24
}

func (bigEndian) Uint32(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func (bigEndian) PutUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func (bigEndian) WriteUint32(w *bufio.Writer, v uint32) (err error) {
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}

func (bigEndian) PutInt32(b []byte, v int32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func (bigEndian) WriteInt32(w *bufio.Writer, v int32) (err error) {
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}

func (bigEndian) Int64(b []byte) int64 {
	return int64(b[7]) | int64(b[6])<<8 | int64(b[5])<<16 | int64(b[4])<<24 |
		int64(b[3])<<32 | int64(b[2])<<40 | int64(b[1])<<48 | int64(b[0])<<56
}

func (bigEndian) Uint64(b []byte) uint64 {
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

func (bigEndian) PutInt64(b []byte, v int64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func (bigEndian) WriteInt64(w *bufio.Writer, v int64) (err error) {
	if err = w.WriteByte(byte(v >> 56)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 48)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 40)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 32)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 24)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return
	}
	err = w.WriteByte(byte(v))
	return
}
