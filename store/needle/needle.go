package needle

import (
	"bfs/libs/encoding/binary"
	"bfs/libs/errors"
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"syscall"
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
	// footer
	_magicSize  = 4
	_cookieSize = 4
	_keySize    = 8
	_flagSize   = 1
	_sizeSize   = 4
	// data
	// footer
	// magic
	_checksumSize = 4
	// padding

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
	// _magicOffset  = 0
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
	_displayData = 16
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

	_pageSize = syscall.Getpagesize()
	_bufPool  = sync.Pool{
		New: func() interface{} {
			return make([]byte, _pageSize) // 4kb
		},
	}
)

// init the padding table
func init() {
	var i int
	for i = 1; i < PaddingSize; i++ {
		_padding = append(_padding, bytes.Repeat([]byte{_paddingByte}, i))
	}
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
	Offset     uint32
	buffer     []byte // needle buffer holder
}

// NewWriter new a read needle.
func NewWriter(key int64, cookie, size int32) *Needle {
	var n = new(Needle)
	n.Key = key
	n.Cookie = cookie
	n.Size = size
	n.init()
	n.newBuffer()
	return n
}

func (n *Needle) InitWriter(key int64, cookie, size int32) {
	n.Key = key
	n.Cookie = cookie
	n.Size = size
	n.init()
	n.newBuffer()
}

// NewReader new a write needle.
func NewReader(key, nc int64) *Needle {
	var n = new(Needle)
	n.Key = key
	n.Offset, n.TotalSize = Cache(nc)
	n.newBuffer()
	return n
}

// Close close a needle.
func (n *Needle) Close() {
	n.freeBuffer()
}

// newBuffer new a needle buffer by needle totalsize.
func (n *Needle) newBuffer() {
	if n.TotalSize <= int32(_pageSize) {
		n.buffer = _bufPool.Get().([]byte)
	} else {
		n.buffer = make([]byte, n.TotalSize)
	}
}

// free free needle buffer.
func (n *Needle) freeBuffer() {
	if n.buffer != nil && len(n.buffer) <= _pageSize {
		_bufPool.Put(n.buffer)
	}
}

// Buffer get needle buffer, usually call after WriteFrom.
func (n *Needle) Buffer() []byte {
	return n.buffer[:n.TotalSize]
}

// calcSize calc the needle meta size.
func (n *Needle) calcSize() {
	n.TotalSize = int32(_headerSize + n.Size + _footerSize)
	n.PaddingSize = align(n.TotalSize) - n.TotalSize
	n.TotalSize += n.PaddingSize
	n.FooterSize = _footerSize + n.PaddingSize
	n.IncrOffset = NeedleOffset(int64(n.TotalSize))
}

// Init parse needle from specified size.
func (n *Needle) init() {
	n.calcSize()
	n.Flag = FlagOK
	n.HeaderMagic = _headerMagic
	n.FooterMagic = _footerMagic
	n.Padding = _padding[n.PaddingSize]
	return
}

// parseHeader parse a needle header part.
func (n *Needle) parseHeader(buf []byte) (err error) {
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
	if n.Size < 0 {
		return errors.ErrNeedleSize
	}
	n.calcSize()
	return
}

// parseData parse a needle data part.
func (n *Needle) parseData(buf []byte) (err error) {
	if len(buf) != int(n.Size) {
		return errors.ErrNeedleDataSize
	}
	// data
	n.Data = buf
	// checksum
	n.Checksum = crc32.Update(0, _crc32Table, n.Data)
	return
}

// parseFooter parse a needle footer part.
func (n *Needle) parseFooter(buf []byte) (err error) {
	if len(buf) != int(n.FooterSize) {
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

// writeHeader write needle header into buf bytes.
func (n *Needle) writeHeader(buf []byte) (err error) {
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
	buf[_flagOffset] = n.Flag
	// size
	binary.BigEndian.PutInt32(buf[_sizeOffset:_dataOffset], n.Size)
	return
}

// writeFooter write needle header into buf bytes.
func (n *Needle) writeFooter(buf []byte) (err error) {
	if len(buf) != int(n.FooterSize) {
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

// ParseFrom read from bufio.Reader and parse needle.
// used in scan block by bufio.Reader.
func (n *Needle) ParseFrom(rd *bufio.Reader) (err error) {
	var (
		dataOffset   int32
		footerOffset int32
		endOffset    int32
		data         []byte
	)
	// header
	if data, err = rd.Peek(_headerSize); err != nil {
		return
	}
	if err = n.parseHeader(data); err != nil {
		return
	}
	dataOffset = _headerSize
	footerOffset = dataOffset + n.Size
	endOffset = footerOffset + n.FooterSize
	// no discard, get all needle buffer
	if data, err = rd.Peek(int(n.TotalSize)); err != nil {
		return
	}
	if err = n.parseData(data[dataOffset:footerOffset]); err != nil {
		return
	}
	// footer
	if err = n.parseFooter(data[footerOffset:endOffset]); err != nil {
		return
	}
	n.buffer = data
	_, err = rd.Discard(int(n.TotalSize))
	return
}

// parse Parse needle from inner buffer, usually call after ReadAt.
func (n *Needle) Parse() (err error) {
	var dataOffset int32
	if err = n.parseHeader(n.buffer[:_headerSize]); err == nil {
		dataOffset = _headerSize + n.Size
		if err = n.parseData(n.buffer[_headerSize:dataOffset]); err == nil {
			err = n.parseFooter(n.buffer[dataOffset:n.TotalSize])
		}
	}
	return
}

// ReadFrom read from io.Reader and write into needle buffer.
func (n *Needle) ReadFrom(rd io.Reader) (err error) {
	var (
		dataOffset int32
		data       []byte
	)
	dataOffset = _headerSize + n.Size
	data = n.buffer[_headerSize:dataOffset]
	if err = n.writeHeader(n.buffer[:_headerSize]); err == nil {
		if _, err = rd.Read(data); err == nil {
			n.Data = data
			n.Checksum = crc32.Update(0, _crc32Table, data)
			err = n.writeFooter(n.buffer[dataOffset:n.TotalSize])
		}
	}
	return
}

func (n *Needle) String() string {
	var dn = _displayData
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
