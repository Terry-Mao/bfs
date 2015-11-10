package block

import (
	"bufio"
	"bytes"
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	myos "github.com/Terry-Mao/bfs/store/os"
	log "github.com/golang/glog"
	"io"
	"os"
)

// Super block has a header.
// super block header format:
//  --------------
// | magic number |   ---- 4bytes
// | version      |   ---- 1byte
// | padding      |   ---- aligned with needle padding size (for furtuer  used)
//  --------------
//

const (
	// size
	headerSize  = needle.PaddingSize
	magicSize   = 4
	verSize     = 1
	paddingSize = headerSize - magicSize - verSize
	// offset
	headerOffset  = headerSize
	magicOffset   = 0
	verOffset     = magicOffset + verSize
	paddingOffset = verOffset + paddingSize
	paddingByte   = byte(0)
	// ver
	Ver1 = byte(1)
	// limits
	// offset aligned 8 bytes, 4GB * needle_padding_size
	maxSize   = 4 * 1024 * 1024 * 1024 * needle.PaddingSize
	maxOffset = 4294967295
)

var (
	magic   = []byte{0xab, 0xcd, 0xef, 0x00}
	ver     = []byte{Ver1}
	padding = bytes.Repeat([]byte{paddingByte}, paddingSize)
)

// An Volume contains one superblock and many needles.
type SuperBlock struct {
	r       *os.File
	w       *os.File
	bw      *bufio.Writer
	File    string `json:"file"`
	Offset  uint32 `json:"offset"`
	LastErr error  `json:"last_err"`
	buf     []byte
	bufSize int
	sync    int
	// meta
	Magic []byte `json:"-"`
	Ver   byte   `json:"ver"`
	// status
	closed bool
	write  int
}

// NewSuperBlock creae a new super block.
func NewSuperBlock(file string, buf, sync int) (b *SuperBlock, err error) {
	b = &SuperBlock{}
	b.closed = false
	b.File = file
	b.bufSize = buf
	b.Offset = needle.NeedleOffset(headerSize)
	b.buf = make([]byte, buf)
	b.sync = sync
	if b.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		b.Close()
		return nil, err
	}
	if b.r, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		b.Close()
		return nil, err
	}
	b.bw = bufio.NewWriterSize(b.w, buf)
	if err = b.init(); err != nil {
		log.Errorf("block: %s init() error(%v)", file, err)
		b.Close()
		return nil, err
	}
	return
}

// init init block file, add/parse meta info.
func (b *SuperBlock) init() (err error) {
	var stat os.FileInfo
	if stat, err = b.r.Stat(); err != nil {
		log.Errorf("block: %s Stat() error(%v)", b.File, err)
		return
	}
	if stat.Size() == 0 {
		// falloc(FALLOC_FL_KEEP_SIZE)
		if err = myos.Fallocate(b.w.Fd(), myos.FALLOC_FL_KEEP_SIZE, 0, maxSize); err != nil {
			log.Errorf("block: %s Fallocate() error(%s)", b.File, err)
			return
		}
		if err = b.writeMeta(); err != nil {
			log.Errorf("block: %s writeMeta() error(%v)", b.File, err)
			return
		}
	} else {
		if err = b.parseMeta(); err != nil {
			log.Errorf("block: %s parseMeta() error(%v)", b.File, err)
			return
		}
		if _, err = b.w.Seek(headerOffset, os.SEEK_SET); err != nil {
			log.Errorf("block: %s Seek() error(%v)", b.File, err)
			return
		}
		b.Offset = needle.NeedleOffset(headerOffset)
	}
	return
}

// writeMeta write block meta info.
func (b *SuperBlock) writeMeta() (err error) {
	// magic
	if _, err = b.w.Write(magic); err != nil {
		return
	}
	// ver
	if _, err = b.w.Write(ver); err != nil {
		return
	}
	// padding
	_, err = b.w.Write(padding)
	return
}

// parseMeta parse block meta info.
func (b *SuperBlock) parseMeta() (err error) {
	if _, err = b.r.Read(b.buf[:headerSize]); err != nil {
		return
	}
	b.Magic = b.buf[magicOffset : magicOffset+magicSize]
	b.Ver = b.buf[verOffset : verOffset+verSize][0]
	if !bytes.Equal(b.Magic, magic) {
		return errors.ErrSuperBlockMagic
	}
	if b.Ver == Ver1 {
		return errors.ErrSuperBlockVer
	}
	return
}

// avvailable check block has enough space.
func (b *SuperBlock) available(incrOffset uint32) (err error) {
	if maxOffset-incrOffset < b.Offset {
		err = errors.ErrSuperBlockNoSpace
		b.LastErr = err
	}
	return
}

// Add append a photo to the block.
func (b *SuperBlock) Add(n *needle.Needle) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	var incrOffset = needle.NeedleOffset(int64(n.TotalSize))
	if err = b.available(incrOffset); err != nil {
		return
	}
	if err = n.Write(b.bw); err != nil {
		b.LastErr = err
		return
	}
	b.write++
	if err = b.Flush(); err != nil {
		return
	}
	b.Offset += incrOffset
	return
}

// Write start add needles to the block, must called after start a transaction.
func (b *SuperBlock) Write(n *needle.Needle) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	var incrOffset = needle.NeedleOffset(int64(n.TotalSize))
	if err = b.available(incrOffset); err != nil {
		return
	}
	if err = n.Write(b.bw); err != nil {
		b.LastErr = err
		return
	}
	b.write++
	b.Offset += incrOffset
	return
}

// Flush flush writer buffer.
func (b *SuperBlock) Flush() (err error) {
	var fd uintptr
	if b.LastErr != nil {
		return b.LastErr
	}
	// write may be less than request, we call flush in a loop
	if err = b.bw.Flush(); err != nil {
		b.LastErr = err
		log.Errorf("block: %s Flush() error(%v)", b.File, err)
		return
	}
	// append N times call flush then clean the os page cache
	// page cache no used here...
	// after upload a photo, we cache in user-level.
	if b.write%b.sync != 0 {
		return
	}
	if err = b.w.Sync(); err != nil {
		b.LastErr = err
		log.Errorf("block: %s Fdatasync() error(%v)", b.File, err)
		return
	}
	fd = b.w.Fd()
	if err = myos.Fadvise(fd, 0, needle.BlockOffset(b.Offset), myos.POSIX_FADV_DONTNEED); err != nil {
		b.LastErr = err
		log.Errorf("block: %s Fadvise() error(%v)", b.File, err)
		return
	}
	return
}

// Repair repair the specified offset needle without update current offset.
func (b *SuperBlock) Repair(offset uint32, buf []byte) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	_, err = b.w.WriteAt(buf, needle.BlockOffset(offset))
	b.LastErr = err
	return
}

// Get get a needle from super block.
func (b *SuperBlock) Get(offset uint32, buf []byte) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	_, err = b.r.ReadAt(buf, needle.BlockOffset(offset))
	b.LastErr = err
	return
}

// Del logical del a needls, only update the flag to it.
func (b *SuperBlock) Del(offset uint32) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	// WriteAt won't update the file offset.
	_, err = b.w.WriteAt(needle.FlagDelBytes, needle.BlockOffset(offset)+needle.FlagOffset)
	b.LastErr = err
	return
}

// Scan scan a block file.
func (b *SuperBlock) Scan(r *os.File, offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	var (
		data   []byte
		so, eo uint32
		n      = &needle.Needle{}
		rd     = bufio.NewReaderSize(r, b.bufSize)
	)
	if offset == 0 {
		offset = needle.NeedleOffset(headerOffset)
	}
	so, eo = offset, offset
	log.Infof("scan block: %s from offset: %d", b.File, offset)
	if _, err = r.Seek(needle.BlockOffset(offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File)
		return
	}
	for {
		if data, err = rd.Peek(needle.HeaderSize); err != nil {
			break
		}
		if err = n.ParseHeader(data); err != nil {
			break
		}
		if _, err = rd.Discard(needle.HeaderSize); err != nil {
			break
		}
		if data, err = rd.Peek(n.DataSize); err != nil {
			break
		}
		if err = n.ParseData(data); err != nil {
			break
		}
		if _, err = rd.Discard(n.DataSize); err != nil {
			break
		}
		if log.V(1) {
			log.Info(n.String())
		}
		eo += needle.NeedleOffset(int64(n.TotalSize))
		if err = fn(n, so, eo); err != nil {
			break
		}
		so = eo
	}
	if err == io.EOF {
		log.Infof("scan block: %s to offset: %d [ok]", b.File, eo)
		err = nil
	} else {
		log.Infof("scan block: %s to offset: %d error(%v) [failed]", b.File, eo, err)
	}
	return
}

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	if err = b.Scan(b.r, offset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if err1 = fn(n, so, eo); err1 == nil {
			b.Offset = eo
		}
		return
	}); err != nil {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(needle.BlockOffset(b.Offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
	}
	return
}

// Compact compact the orig block, copy to disk dst block.
func (b *SuperBlock) Compact(offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	var r *os.File
	if r, err = os.OpenFile(b.File, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		return
	}
	if err = b.Scan(r, offset, func(n *needle.Needle, so, eo uint32) error {
		return fn(n, so, eo)
	}); err != nil {
		r.Close()
		return
	}
	if err = r.Close(); err != nil {
		log.Errorf("block: %s Close() error(%v)", b.File, err)
	}
	return
}

// Open open the closed superblock, must called after NewSuperBlock.
func (b *SuperBlock) Open() (err error) {
	if !b.closed {
		return
	}
	if b.w, err = os.OpenFile(b.File, os.O_WRONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		return
	}
	b.bw.Reset(b.w)
	if b.r, err = os.OpenFile(b.File, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		b.Close()
		return
	}
	if err = b.init(); err != nil {
		log.Errorf("block: %s init error(%v)", b.File, err)
		b.Close()
		return
	}
	b.closed = false
	b.LastErr = nil
	return
}

// Close close the superblock.
func (b *SuperBlock) Close() {
	var err error
	if b.w != nil {
		if err = b.Flush(); err != nil {
			log.Errorf("block: %s flush error(%v)", b.File, err)
		}
		if err = b.w.Sync(); err != nil {
			log.Errorf("block: %s sync error(%v)", b.File, err)
		}
		if err = b.w.Close(); err != nil {
			log.Errorf("block: %s close error(%v)", b.File, err)
		}
		b.w = nil
	}
	if b.r != nil {
		if err = b.r.Close(); err != nil {
			log.Errorf("block: %s close error(%v)", b.File, err)
		}
		b.r = nil
	}
	b.closed = true
	b.LastErr = errors.ErrSuperBlockClosed
	return
}

// Destroy destroy the block.
func (b *SuperBlock) Destroy() {
	if !b.closed {
		b.Close()
	}
	os.Remove(b.File)
	return
}
