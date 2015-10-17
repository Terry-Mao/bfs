package main

import (
	"bytes"
	"fmt"
	log "github.com/golang/glog"
	"hash/crc32"
)

const (
	needleCookieSize = 8
	needleKeySize    = 8
	needleFlagSize   = 1
	needleSizeSize   = 4
	needleMagicSize  = 4
	NeedleHeaderSize = needleMagicSize + needleCookieSize + needleKeySize +
		needleFlagSize + needleSizeSize
	NeedleFlagOffset   = needleCookieSize + needleKeySize
	needleChecksumSize = 4
	NeedleFooterSize   = needleMagicSize + needleChecksumSize // +padding
	NeedleMaxSize      = 10 * 1024 * 1024
	needleSizeMask     = int64(0xFF)
	needleOffsetBit    = 32
	// our offset is aligned with padding size(8)
	// so a uint32 can store 4GB * 8 offset
	NeedlePaddingSize = 8
	// flags
	NeedleStatusOK  = byte(0)
	NeedleStatusDel = byte(1)
	// del offset
	NeedleCacheDelOffset = uint32(0)
)

var (
	needlePadding = [][]byte{
		nil, // ignore
		[]byte{0},
		[]byte{0, 0},
		[]byte{0, 0, 0},
		[]byte{0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0},
	}
	crc32Table = crc32.MakeTable(crc32.Koopman)
	// magic number
	needleHeaderMagic = []byte{0x12, 0x34, 0x56, 0x78}
	needleFooterMagic = []byte{0x87, 0x65, 0x43, 0x21}
	// flag
	NeedleStatusDelBytes = []byte{NeedleStatusDel}
)

// NeedleCache needle meta data in memory.
// high 32bit = Offset
// medium 16bit noused
// low 16bit = size
type NeedleCache int64

// NewNeedleCache new a needle cache.
func NewNeedleCache(size int32, offset uint32) NeedleCache {
	return NeedleCache(int64(offset)<<needleOffsetBit + int64(size))
}

// Value get needle meta data.
func (n NeedleCache) Value() (size int32, offset uint32) {
	size, offset = int32(int64(n)&needleSizeMask), uint32(n>>needleOffsetBit)
	return
}

// Needle
type Needle struct {
	HeaderMagic []byte
	Cookie      int64
	Key         int64
	Flag        byte
	Size        int32
	Data        []byte
	FooterMagic []byte
	Checksum    uint32
	PaddingSize int32
	Padding     []byte
	DataSize    int
}

func (n *Needle) String() string {
	return fmt.Sprintf(`
-----------------------------
HeaderMagic:    %v
Cookie:         %d
Key:            %d
Flag:           %d
Size:           %d

Data:           %v
FooterMagic:    %v
Checksum:       %d
Padding:        %v
-----------------------------
	`, n.HeaderMagic, n.Cookie, n.Key, n.Flag, n.Size, n.Data, n.FooterMagic,
		n.Checksum, n.Padding)
}

// FillNeedleBuf fill needle buf with photo.
func FillNeedleBuf(key, cookie int64, data, buf []byte) (size int32) {
	var (
		n        int
		padding  int32
		checksum = crc32.Update(0, crc32Table, data)
	)
	size = int32(NeedleHeaderSize + len(data) + NeedleFooterSize)
	padding = NeedlePaddingSize - (size % NeedlePaddingSize)
	size += padding
	// header
	copy(buf[:needleMagicSize], needleHeaderMagic)
	n += needleMagicSize
	BigEndian.PutInt64(buf[n:], cookie)
	n += needleCookieSize
	BigEndian.PutInt64(buf[n:], key)
	n += needleKeySize
	buf[n] = NeedleStatusOK
	n += needleFlagSize
	BigEndian.PutInt32(buf[n:], int32(len(data)))
	n += needleSizeSize
	// data
	copy(buf[n:], data)
	n += len(data)
	// footer
	copy(buf[n:], needleFooterMagic)
	n += needleMagicSize
	BigEndian.PutUint32(buf[n:], checksum)
	n += needleChecksumSize
	copy(buf[n:], needlePadding[padding])
	return
}

// ParseNeedleHeader parse a needle header part.
func ParseNeedleHeader(buf []byte, n *Needle) (err error) {
	var bn int
	n.HeaderMagic = buf[:needleMagicSize]
	if bytes.Compare(n.HeaderMagic, needleHeaderMagic) != 0 {
		err = ErrNeedleHeaderMagic
		return
	}
	bn += needleMagicSize
	n.Cookie = BigEndian.Int64(buf[bn:])
	bn += needleCookieSize
	n.Key = BigEndian.Int64(buf[bn:])
	bn += needleKeySize
	n.Flag = buf[bn]
	if n.Flag != NeedleStatusOK && n.Flag != NeedleStatusDel {
		err = ErrNeedleFlag
		return
	}
	bn += needleFlagSize
	n.Size = BigEndian.Int32(buf[bn:])
	if n.Size > NeedleMaxSize || n.Size < 1 {
		err = ErrNeedleSize
		return
	}
	n.PaddingSize = NeedlePaddingSize - ((NeedleHeaderSize + n.Size +
		NeedleFooterSize) % NeedlePaddingSize)
	n.DataSize = int(n.Size + n.PaddingSize + NeedleFooterSize)
	return
}

// ParseNeedleData parse a needle data part.
func ParseNeedleData(buf []byte, n *Needle) (err error) {
	var (
		bn       int32
		checksum uint32
	)
	n.Data = buf[:n.Size]
	bn += n.Size
	n.FooterMagic = buf[bn : bn+needleMagicSize]
	if bytes.Compare(n.FooterMagic, needleFooterMagic) != 0 {
		err = ErrNeedleFooterMagic
		return
	}
	bn += needleMagicSize
	checksum = crc32.Update(0, crc32Table, n.Data)
	n.Checksum = BigEndian.Uint32(buf[bn : bn+needleChecksumSize])
	if n.Checksum != checksum {
		err = ErrNeedleChecksum
		return
	}
	bn += needleChecksumSize
	n.Padding = buf[bn : bn+n.PaddingSize]
	log.Infof("padding: %d, %v vs %v\n", n.PaddingSize, n.Padding, needlePadding[n.PaddingSize])
	if bytes.Compare(n.Padding, needlePadding[n.PaddingSize]) != 0 {
		err = ErrNeedlePaddingNotMatch
		return
	}
	return
}

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * NeedlePaddingSize
}

// NeedleOffset get needle aligned offset.
func NeedleOffset(n int) uint32 {
	return uint32(n) / NeedlePaddingSize
}
