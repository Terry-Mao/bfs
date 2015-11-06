package needle

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/Terry-Mao/bfs/store/encoding/binary"
	"github.com/Terry-Mao/bfs/store/errors"
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
	// size
	magicSize    = 4
	cookieSize   = 4
	keySize      = 8
	flagSize     = 1
	sizeSize     = 4
	checksumSize = 4

	// header is constant = 21
	HeaderSize = magicSize + cookieSize + keySize + flagSize + sizeSize
	// footer is constant = 8 (no padding)
	FooterSize = magicSize + checksumSize

	// WARN our offset is aligned with padding size(8)
	// so a uint32 can store 4GB * 8 offset
	// if you want a block much more larger, modify this constant, but must
	// bigger than 8
	PaddingSize  = 8
	paddingAlign = PaddingSize - 1
	paddingByte  = byte(0)

	// flags
	FlagOK     = byte(0)
	FlagDel    = byte(1)
	FlagOffset = magicSize + cookieSize + keySize
	// display
	displayData = 16
)

var (
	padding = [][]byte{nil}
	// crc32 checksum table, goroutine safe
	crc32Table = crc32.MakeTable(crc32.Koopman)
	// magic number
	headerMagic = []byte{0x12, 0x34, 0x56, 0x78}
	footerMagic = []byte{0x87, 0x65, 0x43, 0x21}
	// flag
	FlagDelBytes = []byte{FlagDel}
	// needle min size, which data is one byte
	MinSize = align(HeaderSize + FooterSize + 1)
)

// init the padding table
func init() {
	var i int
	for i = 1; i < PaddingSize; i++ {
		padding = append(padding, bytes.Repeat([]byte{paddingByte}, i))
	}
	return
}

// Needle is a photo data stored in disk.
type Needle struct {
	HeaderMagic []byte
	Cookie      int32
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
	n.HeaderMagic = buf[:magicSize]
	if !bytes.Equal(n.HeaderMagic, headerMagic) {
		err = errors.ErrNeedleHeaderMagic
		return
	}
	bn += magicSize
	n.Cookie = binary.BigEndian.Int32(buf[bn:])
	bn += cookieSize
	n.Key = binary.BigEndian.Int64(buf[bn:])
	bn += keySize
	n.Flag = buf[bn]
	if n.Flag != FlagOK && n.Flag != FlagDel {
		err = errors.ErrNeedleFlag
		return
	}
	bn += flagSize
	n.Size = binary.BigEndian.Int32(buf[bn:])
	n.TotalSize = HeaderSize + n.Size + FooterSize
	n.PaddingSize = align(n.TotalSize) - n.TotalSize
	n.TotalSize += n.PaddingSize
	n.DataSize = int(n.Size + n.PaddingSize + FooterSize)
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
	n.FooterMagic = buf[bn : bn+magicSize]
	if !bytes.Equal(n.FooterMagic, footerMagic) {
		err = errors.ErrNeedleFooterMagic
		return
	}
	bn += magicSize
	checksum = crc32.Update(0, crc32Table, n.Data)
	n.Checksum = binary.BigEndian.Uint32(buf[bn : bn+checksumSize])
	if n.Checksum != checksum {
		err = errors.ErrNeedleChecksum
		return
	}
	bn += checksumSize
	n.Padding = buf[bn : bn+n.PaddingSize]
	if !bytes.Equal(n.Padding, padding[n.PaddingSize]) {
		err = errors.ErrNeedlePadding
	}
	return
}

func (n *Needle) String() string {
	var dn = displayData
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
`, n.HeaderMagic, n.Cookie, n.Key, n.Flag, n.Size, n.Data[:dn], n.FooterMagic,
		n.Checksum, n.Padding)
}

// Parse parse needle from data.
func (n *Needle) Parse(key int64, cookie int32, data []byte) {
	var dataSize = int32(len(data))
	n.TotalSize = int32(HeaderSize + dataSize + FooterSize)
	n.PaddingSize = align(n.TotalSize) - n.TotalSize
	n.TotalSize += n.PaddingSize
	n.HeaderMagic = headerMagic
	n.Key = key
	n.Cookie = cookie
	n.Size = dataSize
	n.Data = data
	n.FooterMagic = footerMagic
	n.Checksum = crc32.Update(0, crc32Table, data)
	n.Padding = padding[n.PaddingSize]
	return
}

// Write write needle into bufio.
func (n *Needle) Write(w *bufio.Writer) (err error) {
	// header
	// magic
	if _, err = w.Write(n.HeaderMagic); err != nil {
		return
	}
	// cookie
	if err = binary.BigEndian.WriteInt32(w, n.Cookie); err != nil {
		return
	}
	// key
	if err = binary.BigEndian.WriteInt64(w, n.Key); err != nil {
		return
	}
	// flag
	if err = w.WriteByte(FlagOK); err != nil {
		return
	}
	// size
	if err = binary.BigEndian.WriteInt32(w, n.Size); err != nil {
		return
	}
	// data
	if _, err = w.Write(n.Data); err != nil {
		return
	}
	// footer
	// magic
	if _, err = w.Write(n.FooterMagic); err != nil {
		return
	}
	// checksum
	if err = binary.BigEndian.WriteUint32(w, n.Checksum); err != nil {
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
	copy(buf[:magicSize], n.HeaderMagic)
	bn += magicSize
	// cookie
	binary.BigEndian.PutInt32(buf[bn:], n.Cookie)
	bn += cookieSize
	// key
	binary.BigEndian.PutInt64(buf[bn:], n.Key)
	bn += keySize
	// flag
	buf[bn] = FlagOK
	bn += flagSize
	// size
	binary.BigEndian.PutInt32(buf[bn:], n.Size)
	bn += sizeSize
	// data
	copy(buf[bn:], n.Data)
	bn += len(n.Data)
	// --- footer ---
	// magic
	copy(buf[bn:], n.FooterMagic)
	bn += magicSize
	// checksum
	binary.BigEndian.PutUint32(buf[bn:], n.Checksum)
	bn += checksumSize
	// padding
	copy(buf[bn:], n.Padding)
	return
}

// NeedleOffset convert offset to needle offset.
func NeedleOffset(offset int64) uint32 {
	return uint32(offset / PaddingSize)
}

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * PaddingSize
}

// align get aligned size.
func align(d int32) int32 {
	return (d + paddingAlign) & ^paddingAlign
}
