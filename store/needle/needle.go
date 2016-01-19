package needle

import (
	"bytes"
	"fmt"
	"github.com/Terry-Mao/bfs/libs/encoding/binary"
	"github.com/Terry-Mao/bfs/libs/errors"
	"hash/crc32"
	"io"
)

// Needle stored int super block, aligned to 8bytes.
//
// needle file format:
//  --------------
// | super  block |
//  --------------
// |    needle    |		    ----------------
// |    needle    |        |  magic (int32) |
// |    needle    | ---->  |  cookie (int32)|
// |    needle    |        |  key (int64)   |
// |    needle    |        |  flag (byte)   |
// |    needle    |        |  size (int32)  |
// |    needle    |        |  data (bytes)  |
// |    needle    |        |  magic (int32) |
// |    needle    |        | checksum(int32)|
// |    needle    |        | padding (bytes)|
// |    ......    |         ----------------
// |    ......    |             int bigendian
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
	_magicSize    = 4
	_cookieSize   = 4
	_keySize      = 8
	_flagSize     = 1
	_sizeSize     = 4
	_checksumSize = 4

	// offset
	// header
	_magicOffset  = 0
	_cookieOffset = _magicOffset + _magicSize
	_keyOffset    = _cookieOffset + _cookieSize
	_flagOffset   = _keyOffset + _keySize
	_sizeOffset   = _flagOffset + _flagSize
	_dataOffset   = _sizeOffset + _sizeSize

	KeyOffset  = _keyOffset
	FlagOffset = _flagOffset
	// footer
	_checksumOffset = _magicOffset + _magicSize
	_paddingOffset  = _checksumOffset + _checksumSize

	// header is constant = 21
	_headerSize = _magicSize + _cookieSize + _keySize + _flagSize + _sizeSize
	// footer is constant = 8 (no padding)
	_footerSize = _magicSize + _checksumSize

	HeaderSize = _headerSize
	FooterSize = _footerSize

	// WARN our offset is aligned with padding size(8)
	// so a uint32 can store 4GB * 8 offset
	// if you want a block more larger, modify this constant, but must bigger
	// than 8
	PaddingSize   = 8
	_paddingAlign = PaddingSize - 1
	_paddingByte  = byte(0)

	// flags
	FlagOK  = byte(0)
	FlagDel = byte(1)

	// display
	displayData = 16
)

var (
	_padding = [][]byte{nil}
	// crc32 checksum table, goroutine safe
	_crc32Table = crc32.MakeTable(crc32.Koopman)
	// magic number
	_headerMagic = []byte{0x12, 0x34, 0x56, 0x78}
	_footerMagic = []byte{0x87, 0x65, 0x43, 0x21}
	// flag
	FlagDelBytes = []byte{FlagDel}
	// needle min size, which data is one byte
	MinSize = align(_headerSize + _footerSize + 1)
)

// init the padding table
func init() {
	var i int
	for i = 1; i < PaddingSize; i++ {
		_padding = append(_padding, bytes.Repeat([]byte{_paddingByte}, i))
	}
	return
}

// Needles is needle list.
type Needles struct {
	Items     []Needle
	Buffer    []byte
	TotalSize int32
}

// Write Write needle into buffer.
func (ns *Needles) Write(n *Needle) (err error) {
	var (
		headerOffset = ns.TotalSize
		dataOffset   = headerOffset + _headerSize
		footerOffset = dataOffset + n.Size
		endOffset    = footerOffset + n.FooterSize
	)
	if err = n.WriteHeader(ns.Buffer[headerOffset:dataOffset]); err == nil {
		if err = n.WriteData(ns.Buffer[dataOffset:footerOffset]); err == nil {
			err = n.WriteFooter(ns.Buffer[footerOffset:endOffset])
		}
	}
	ns.TotalSize += n.TotalSize
	return
}

// WriteFrom Write needle from io.Reader into buffer.
func (ns *Needles) WriteFrom(n *Needle, rd io.Reader) (err error) {
	var (
		headerOffset = ns.TotalSize
		dataOffset   = headerOffset + _headerSize
		footerOffset = dataOffset + n.Size
		endOffset    = footerOffset + n.FooterSize
	)
	if err = n.WriteHeader(ns.Buffer[headerOffset:dataOffset]); err == nil {
		if _, err = rd.Read(ns.Buffer[dataOffset:footerOffset]); err == nil {
			// data
			n.Data = ns.Buffer[dataOffset:footerOffset]
			// checksum
			n.Checksum = crc32.Update(0, _crc32Table, n.Data)
			err = n.WriteFooter(ns.Buffer[footerOffset:endOffset])
		}
	}
	ns.TotalSize += n.TotalSize
	return
}

// Needle is a photo data stored in disk.
type Needle struct {
	HeaderMagic []byte
	Cookie      int32
	Key         int64
	Flag        byte
	Size        int32 // data size
	Data        []byte
	FooterMagic []byte
	Checksum    uint32
	Padding     []byte
	PaddingSize int32
	TotalSize   int32
	FooterSize  int32
	// used in peek
	IncrOffset uint32
	Buffer     []byte // needle buffer holder
}

func (n *Needle) calcSize() {
	n.TotalSize = int32(_headerSize + n.Size + _footerSize)
	n.PaddingSize = align(n.TotalSize) - n.TotalSize
	n.TotalSize += n.PaddingSize
	n.FooterSize = _footerSize + n.PaddingSize
	n.IncrOffset = NeedleOffset(int64(n.TotalSize))
}

// InitSize parse needle from specified size.
func (n *Needle) InitSize(key int64, cookie, size int32) {
	n.Size = size
	n.calcSize()
	n.Key = key
	n.Cookie = cookie
	n.HeaderMagic = _headerMagic
	n.FooterMagic = _footerMagic
	n.Padding = _padding[n.PaddingSize]
	return
}

// Init parse needle from data.
func (n *Needle) Init(key int64, cookie int32, data []byte) {
	n.InitSize(key, cookie, int32(len(data)))
	n.Data = data
	n.Checksum = crc32.Update(0, _crc32Table, data)
	return
}

// ParseHeader parse a needle header part.
func (n *Needle) ParseHeader(buf []byte) (err error) {
	if len(buf) != _headerSize {
		return errors.ErrNeedleHeaderSize
	}
	// magic
	n.HeaderMagic = buf[_magicOffset:_cookieOffset]
	if !bytes.Equal(n.HeaderMagic, _headerMagic) {
		return errors.ErrNeedleHeaderMagic
	}
	// cookie
	n.Cookie = binary.BigEndian.Int32(buf[_cookieOffset:_keyOffset])
	// key
	n.Key = binary.BigEndian.Int64(buf[_keyOffset:_flagOffset])
	// flag
	n.Flag = buf[_flagOffset]
	if n.Flag != FlagOK && n.Flag != FlagDel {
		return errors.ErrNeedleFlag
	}
	// size
	n.Size = binary.BigEndian.Int32(buf[_sizeOffset:_dataOffset])
	n.calcSize()
	return
}

// ParseData parse a needle data part.
func (n *Needle) ParseData(buf []byte) (err error) {
	if len(buf) != int(n.Size) {
		return errors.ErrNeedleDataSize
	}
	// data
	n.Data = buf
	// checksum
	n.Checksum = crc32.Update(0, _crc32Table, n.Data)
	return
}

// ParseFooter parse a needle footer part.
func (n *Needle) ParseFooter(buf []byte) (err error) {
	if len(buf) != int(_footerSize+n.PaddingSize) {
		return errors.ErrNeedleFooterSize
	}
	// magic
	n.FooterMagic = buf[_magicOffset:_checksumOffset]
	if !bytes.Equal(n.FooterMagic, _footerMagic) {
		return errors.ErrNeedleFooterMagic
	}
	if n.Checksum != binary.BigEndian.Uint32(buf[_checksumOffset:_paddingOffset]) {
		return errors.ErrNeedleChecksum
	}
	// padding
	n.Padding = buf[_paddingOffset : _paddingOffset+n.PaddingSize]
	if !bytes.Equal(n.Padding, _padding[n.PaddingSize]) {
		return errors.ErrNeedlePadding
	}
	return
}

// Parse Parse needle from buf bytes.
func (n *Needle) Parse() (err error) {
	var dataOffset = _headerSize + n.Size
	if err = n.ParseHeader(n.Buffer[:_headerSize]); err == nil {
		if err = n.ParseData(n.Buffer[_headerSize:dataOffset]); err == nil {
			err = n.ParseFooter(n.Buffer[dataOffset:n.TotalSize])
		}
	}
	return
}

// WriteHeader write needle header into buf bytes.
func (n *Needle) WriteHeader(buf []byte) (err error) {
	if len(buf) != int(_headerSize) {
		return errors.ErrNeedleHeaderSize
	}
	// magic
	copy(buf[_magicOffset:_cookieOffset], n.HeaderMagic)
	// cookie
	binary.BigEndian.PutInt32(buf[_cookieOffset:_keyOffset], n.Cookie)
	// key
	binary.BigEndian.PutInt64(buf[_keyOffset:_flagOffset], n.Key)
	// flag
	buf[_flagOffset] = FlagOK
	// size
	binary.BigEndian.PutInt32(buf[_sizeOffset:_dataOffset], n.Size)
	return
}

// WriteData write needle data into buf bytes.
func (n *Needle) WriteData(buf []byte) (err error) {
	if len(buf) != int(n.Size) {
		return errors.ErrNeedleDataSize
	}
	copy(buf, n.Data)
	return
}

// WriteFooter write needle header into buf bytes.
func (n *Needle) WriteFooter(buf []byte) (err error) {
	if len(buf) != int(_footerSize+n.PaddingSize) {
		return errors.ErrNeedleFooterSize
	}
	// magic
	copy(buf[_magicOffset:_checksumOffset], n.FooterMagic)
	// checksum
	binary.BigEndian.PutUint32(buf[_checksumOffset:_paddingOffset], n.Checksum)
	// padding
	copy(buf[_paddingOffset:_paddingOffset+n.PaddingSize], n.Padding)
	return
}

// Write write needle into buffer.
func (n *Needle) Write() (err error) {
	var dataOffset = _headerSize + n.Size
	if err = n.WriteHeader(n.Buffer[:_headerSize]); err == nil {
		if err = n.WriteData(n.Buffer[_headerSize:dataOffset]); err == nil {
			err = n.WriteFooter(n.Buffer[dataOffset:n.TotalSize])
		}
	}
	return
}

// WriteFrom Write needle from io.Reader into buffer.
func (n *Needle) WriteFrom(rd io.Reader) (err error) {
	var dataOffset = _headerSize + n.Size
	if err = n.WriteHeader(n.Buffer[:_headerSize]); err == nil {
		if _, err = rd.Read(n.Buffer[_headerSize:dataOffset]); err == nil {
			// data
			n.Data = n.Buffer[_headerSize:dataOffset]
			// checksum
			n.Checksum = crc32.Update(0, _crc32Table, n.Data)
			err = n.WriteFooter(n.Buffer[dataOffset:n.TotalSize])
		}
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
TotalSize:      %d

---- head
HeaderSize:     %d
HeaderMagic:    %v
Cookie:         %d
Key:            %d
Flag:           %d
Size:           %d

---- data
Data:           %v...

---- foot
FooterSize:     %d
FooterMagic:    %v
Checksum:       %d
Padding:        %v
-----------------------------
`, n.TotalSize, _headerSize, n.HeaderMagic, n.Cookie, n.Key, n.Flag, n.Size,
		n.Data[:dn], n.FooterSize, n.FooterMagic, n.Checksum, n.Padding)
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
	return (d + _paddingAlign) & ^_paddingAlign
}

// Size get a needle size with meta data.
func Size(n int) int {
	return int(align(_headerSize + int32(n) + _footerSize))
}
