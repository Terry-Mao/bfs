package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
)

// Needle stored int super block, aligned to 8bytes.
//
// needle file format:
//  ---------------
// | super   block |
//  ---------------
// |     needle    |		   ----------------
// |     needle    |          |  magic (int32) |
// |     needle    | ---->    |  cookie (int32)|
// |     needle    |          |  key (int64)   |
// |     needle    |          |  flag (byte)   |
// |     needle    |          |  size (int32)  |
// |     needle    |          |  data (bytes)  |
// |     needle    |          |  magic (int32) |
// |     needle    |          | checksum(int32)|
// |     needle    |          | padding (bytes)|
// |     ......    |           ----------------
// |     ......    |             int bigendian
//
// field     | explanation
// ---------------------------------------------------------
// magic     | header magic number used for checksum
// cookie    | random number to mitigate brute force lookups
// key       | 64bit photo id
// flag      | signifies deleted status
// size      | data size
// data      | the actual photo data
// magic     | footer magic number used for checksum
// checksum  | used to check integrity
// padding   | total needle size is aligned to 8 bytes

const (
	NeedleMaxSize = 5 * 1024 * 1024 // 5MB

	NeedleIntBuf     = 8
	needleCookieSize = 8
	needleKeySize    = 8
	needleFlagSize   = 1
	needleSizeSize   = 4
	needleMagicSize  = 4
	NeedleHeaderSize = needleMagicSize + needleCookieSize + needleKeySize +
		needleFlagSize + needleSizeSize
	NeedleFlagOffset   = needleMagicSize + needleCookieSize + needleKeySize
	needleChecksumSize = 4
	NeedleFooterSize   = needleMagicSize + needleChecksumSize // +padding
	// our offset is aligned with padding size(8)
	// so a uint32 can store 4GB * 8 offset
	// if you want a block much more larger, modify this constant, but must
	// bigger than 8
	NeedlePaddingSize = 8
	// flags
	NeedleStatusOK  = byte(0)
	NeedleStatusDel = byte(1)
	// display
	needleDisplayData = 16
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

// Needle is a photo data stored in disk.
type Needle struct {
	HeaderMagic []byte
	Cookie      int64
	Key         int64
	Flag        byte
	Size        int32 // raw data size
	Data        []byte
	FooterMagic []byte
	Checksum    uint32
	Padding     []byte
	PaddingSize int32
	TotalSize   int32 // total needle write size
	// used in peek
	DataSize int // data-part size
}

// ParseNeedleHeader parse a needle header part.
func (n *Needle) ParseHeader(buf []byte) (err error) {
	var bn int
	n.HeaderMagic = buf[:needleMagicSize]
	if !bytes.Equal(n.HeaderMagic, needleHeaderMagic) {
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
	n.TotalSize = NeedleHeaderSize + n.Size + NeedleFooterSize
	n.PaddingSize = NeedlePaddingSize - (n.TotalSize % NeedlePaddingSize)
	n.TotalSize += n.PaddingSize
	n.DataSize = int(n.Size + n.PaddingSize + NeedleFooterSize)
	return
}

// ParseNeedleData parse a needle data part.
func (n *Needle) ParseData(buf []byte) (err error) {
	var (
		bn       int32
		checksum uint32
	)
	n.Data = buf[:n.Size]
	bn += n.Size
	n.FooterMagic = buf[bn : bn+needleMagicSize]
	if !bytes.Equal(n.FooterMagic, needleFooterMagic) {
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
	if !bytes.Equal(n.Padding, needlePadding[n.PaddingSize]) {
		err = ErrNeedlePadding
	}
	return
}

func (n *Needle) String() string {
	var dn = needleDisplayData
	if len(n.Data) < dn {
		dn = len(n.Data)
	}
	return fmt.Sprintf(`
-----------------------------
HeaderMagic:    %v
Cookie:         %d
Key:            %d
Flag:           %d
Size:           %d

Data:           %v...
FooterMagic:    %v
Checksum:       %d
Padding:        %v
-----------------------------
	`, n.HeaderMagic, n.Cookie, n.Key, n.Flag, n.Size, n.Data[:dn],
		n.FooterMagic, n.Checksum, n.Padding)
}

// Parse parse needle from data.
func (n *Needle) Parse(key, cookie int64, data []byte) (err error) {
	var dataSize = int32(len(data))
	n.TotalSize = int32(NeedleHeaderSize + dataSize + NeedleFooterSize)
	n.PaddingSize = NeedlePaddingSize - (n.TotalSize % NeedlePaddingSize)
	n.TotalSize += n.PaddingSize
	if n.TotalSize > NeedleMaxSize {
		err = ErrNeedleTooLarge
		return
	}
	n.Key = key
	n.Cookie = cookie
	n.Size = dataSize
	n.Data = data
	n.Checksum = crc32.Update(0, crc32Table, data)
	n.Padding = needlePadding[n.PaddingSize]
	return
}

// Write write needle into bufio.
func (n *Needle) Write(w *bufio.Writer) (err error) {
	// header
	// magic
	if _, err = w.Write(needleHeaderMagic); err != nil {
		return
	}
	// cookie
	if err = BigEndian.WriteInt64(w, n.Cookie); err != nil {
		return
	}
	// key
	if err = BigEndian.WriteInt64(w, n.Key); err != nil {
		return
	}
	// flag
	if err = w.WriteByte(NeedleStatusOK); err != nil {
		return
	}
	// size
	if err = BigEndian.WriteInt32(w, n.Size); err != nil {
		return
	}
	// data
	if _, err = w.Write(n.Data); err != nil {
		return
	}
	// footer
	// magic
	if _, err = w.Write(needleFooterMagic); err != nil {
		return
	}
	// checksum
	if err = BigEndian.WriteUint32(w, n.Checksum); err != nil {
		return
	}
	// padding
	_, err = w.Write(n.Padding)
	return
}

// Fill fill buffer with needle data.
func (n *Needle) Fill(buf []byte) {
	var bn int
	// --- header ---
	// magic
	copy(buf[:needleMagicSize], needleHeaderMagic)
	bn += needleMagicSize
	// cookie
	BigEndian.PutInt64(buf[bn:], n.Cookie)
	bn += needleCookieSize
	// key
	BigEndian.PutInt64(buf[bn:], n.Key)
	bn += needleKeySize
	// flag
	buf[bn] = NeedleStatusOK
	bn += needleFlagSize
	// size
	BigEndian.PutInt32(buf[bn:], n.Size)
	bn += needleSizeSize
	// data
	copy(buf[bn:], n.Data)
	bn += len(n.Data)
	// --- footer ---
	// magic
	copy(buf[bn:], needleFooterMagic)
	bn += needleMagicSize
	// checksum
	BigEndian.PutUint32(buf[bn:], n.Checksum)
	bn += needleChecksumSize
	// padding
	copy(buf[bn:], n.Padding)
	return
}

// NeedleOffset convert offset to needle offset.
func NeedleOffset(offset int64) uint32 {
	return uint32(offset / NeedlePaddingSize)
}
