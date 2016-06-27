package block

import (
	"bfs/libs/errors"
	"bfs/store/conf"
	"bfs/store/needle"
	myos "bfs/store/os"
	"bufio"
	"bytes"
	log "github.com/golang/glog"
	"io"
	"os"
	"syscall"
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
	_headerSize  = needle.PaddingSize
	_magicSize   = 4
	_verSize     = 1
	_paddingSize = _headerSize - _magicSize - _verSize
	// offset
	_headerOffset  = _headerSize
	_magicOffset   = 0
	_verOffset     = _magicOffset + _verSize
	_paddingOffset = _verOffset + _paddingSize
	_paddingByte   = byte(0)
	// ver
	Ver1 = byte(1)
	// limits
	// offset aligned 8 bytes, 4GB * needle_padding_size
	_maxSize   = 4 * 1024 * 1024 * 1024 * needle.PaddingSize
	_maxOffset = 4294967295
)

var (
	_magic    = []byte{0xab, 0xcd, 0xef, 0x00}
	_ver      = []byte{Ver1}
	_padding  = bytes.Repeat([]byte{_paddingByte}, _paddingSize)
	_pagesize = syscall.Getpagesize()
)

// An Volume contains one superblock and many needles.
type SuperBlock struct {
	r       *os.File
	w       *os.File
	conf    *conf.Config
	File    string `json:"file"`
	Offset  uint32 `json:"offset"`
	Size    int64  `json:"size"`
	LastErr error  `json:"last_err"`
	Ver     byte   `json:"ver"`
	magic   []byte `json:"-"`
	Padding uint32 `json:"padding"`
	// status
	closed     bool
	write      int
	syncOffset uint32
}

// NewSuperBlock creae a new super block.
func NewSuperBlock(file string, c *conf.Config) (b *SuperBlock, err error) {
	b = &SuperBlock{}
	b.conf = c
	b.File = file
	b.closed = false
	b.write = 0
	b.syncOffset = 0
	b.Padding = needle.PaddingSize
	if b.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		b.Close()
		return nil, err
	}
	if b.r, err = os.OpenFile(file, os.O_RDONLY|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		b.Close()
		return nil, err
	}
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
	if b.Size = stat.Size(); b.Size == 0 {
		if err = myos.Fallocate(b.w.Fd(), myos.FALLOC_FL_KEEP_SIZE, 0, _maxSize); err != nil {
			log.Errorf("block: %s Fallocate() error(%s)", b.File, err)
			return
		}
		if err = b.writeMeta(); err != nil {
			log.Errorf("block: %s writeMeta() error(%v)", b.File, err)
			return
		}
		b.Size = _headerSize
	} else {
		if err = b.parseMeta(); err != nil {
			log.Errorf("block: %s parseMeta() error(%v)", b.File, err)
			return
		}
		if _, err = b.w.Seek(_headerOffset, os.SEEK_SET); err != nil {
			log.Errorf("block: %s Seek() error(%v)", b.File, err)
			return
		}
	}
	b.Offset = needle.NeedleOffset(_headerOffset)
	return
}

// writeMeta write block meta info.
func (b *SuperBlock) writeMeta() (err error) {
	// magic
	if _, err = b.w.Write(_magic); err != nil {
		return
	}
	// ver
	if _, err = b.w.Write(_ver); err != nil {
		return
	}
	// padding
	_, err = b.w.Write(_padding)
	return
}

// parseMeta parse block meta info.
func (b *SuperBlock) parseMeta() (err error) {
	var buf = make([]byte, _headerSize)
	if _, err = b.r.Read(buf[:_headerSize]); err != nil {
		return
	}
	b.magic = buf[_magicOffset : _magicOffset+_magicSize]
	b.Ver = buf[_verOffset : _verOffset+_verSize][0]
	if !bytes.Equal(b.magic, _magic) {
		return errors.ErrSuperBlockMagic
	}
	if b.Ver == Ver1 {
		return errors.ErrSuperBlockVer
	}
	// b.magic = nil // avoid memory leak
	return
}

// Write write needle to the block.
func (b *SuperBlock) Write(n *needle.Needle) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	if _maxOffset-n.IncrOffset < b.Offset {
		err = errors.ErrSuperBlockNoSpace
		return
	}
	if _, err = b.w.Write(n.Buffer()); err == nil {
		err = b.flush(false)
	} else {
		b.LastErr = err
		return
	}
	b.Offset += n.IncrOffset
	b.Size += int64(n.TotalSize)
	return
}

// flush flush writer buffer.
func (b *SuperBlock) flush(force bool) (err error) {
	var (
		fd     uintptr
		offset int64
		size   int64
	)
	if b.write++; !force && b.write < b.conf.Block.SyncWrite {
		return
	}
	b.write = 0
	offset = needle.BlockOffset(b.syncOffset)
	size = needle.BlockOffset(b.Offset - b.syncOffset)
	fd = b.w.Fd()
	if b.conf.Block.Syncfilerange {
		if err = myos.Syncfilerange(fd, offset, size, myos.SYNC_FILE_RANGE_WRITE); err != nil {
			log.Errorf("block: %s Syncfilerange() error(%v)", b.File, err)
			b.LastErr = err
			return
		}
	} else {
		if err = myos.Fdatasync(fd); err != nil {
			log.Errorf("block: %s Fdatasync() error(%v)", b.File, err)
			b.LastErr = err
			return
		}
	}
	if err = myos.Fadvise(fd, offset, size, myos.POSIX_FADV_DONTNEED); err == nil {
		b.syncOffset = b.Offset
	} else {
		log.Errorf("block: %s Fadvise() error(%v)", b.File, err)
		b.LastErr = err
	}
	return
}

// WriteAt write a needle by specified offset;
func (b *SuperBlock) WriteAt(offset uint32, n *needle.Needle) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	if _, err = b.w.WriteAt(n.Buffer(), needle.BlockOffset(offset)); err != nil {
		b.LastErr = err
	}
	return
}

// ReadAt read a needle by specified offset, before call it, must set needle
// TotalSize.
func (b *SuperBlock) ReadAt(n *needle.Needle) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	if _, err = b.r.ReadAt(n.Buffer(), needle.BlockOffset(n.Offset)); err == nil {
		err = n.Parse()
	} else {
		b.LastErr = err
	}
	return
}

// Delete logical del a needls, only update the flag to it.
func (b *SuperBlock) Delete(offset uint32) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	// WriteAt won't update the file offset.
	if _, err = b.w.WriteAt(needle.FlagDelBytes,
		needle.BlockOffset(offset)+needle.FlagOffset); err != nil {
		b.LastErr = err
	}
	return
}

// Scan scan a block file.
func (b *SuperBlock) Scan(r *os.File, offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	var (
		so, eo uint32
		bso    int64
		fi     os.FileInfo
		fd     = r.Fd()
		n      = new(needle.Needle)
		rd     = bufio.NewReaderSize(r, b.conf.Block.BufferSize)
	)
	if offset == 0 {
		offset = needle.NeedleOffset(_headerOffset)
	}
	so, eo = offset, offset
	bso = needle.BlockOffset(so)
	// advise sequential read
	if fi, err = r.Stat(); err != nil {
		log.Errorf("block: %s Stat() error(%v)", b.File)
		return
	}
	if err = myos.Fadvise(fd, bso, fi.Size(), myos.POSIX_FADV_SEQUENTIAL); err != nil {
		log.Errorf("block: %s Fadvise() error(%v)", b.File)
		return
	}
	log.Infof("scan block: %s from offset: %d", b.File, offset)
	if _, err = r.Seek(bso, os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File)
		return
	}
	for {
		if err = n.ParseFrom(rd); err != nil {
			if err != io.EOF {
				log.Errorf("block: parse needle from offset: %d:%d error(%v)", so, eo, err)
			}
			break
		}
		if n.TotalSize > int32(b.conf.BlockMaxSize) {
			log.Errorf("scan block: %s error(%v)", n, errors.ErrNeedleSize)
			err = errors.ErrNeedleSize
			break
		}
		if log.V(1) {
			log.Info(n.String())
		}
		eo += n.IncrOffset
		if err = fn(n, so, eo); err != nil {
			log.Errorf("block: callback from offset: %d:%d error(%v)", so, eo, err)
			break
		}
		so = eo
	}
	if err == io.EOF {
		// advise no need page cache
		if err = myos.Fadvise(fd, bso, needle.BlockOffset(eo-so), myos.POSIX_FADV_DONTNEED); err != nil {
			log.Errorf("block: %s Fadvise() error(%v)", b.File)
			return
		}
		log.Infof("scan block: %s to offset: %d [ok]", b.File, eo)
		err = nil
	} else {
		log.Infof("scan block: %s to offset: %d error(%v) [failed]", b.File, eo, err)
	}
	return
}

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	var rsize int64
	// WARN block may be no left data, must update block offset first
	if offset == 0 {
		offset = needle.NeedleOffset(_headerOffset)
	}
	b.Offset = offset
	if err = b.Scan(b.r, offset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if err1 = fn(n, so, eo); err1 == nil {
			b.Offset = eo
		}
		return
	}); err != nil {
		return
	}
	// advise random read
	// POSIX_FADV_RANDOM disables file readahead entirely.
	// These changes affect the entire file, not just the specified region
	// (but other open file handles to the same file are unaffected).
	if err = myos.Fadvise(b.r.Fd(), 0, 0, myos.POSIX_FADV_RANDOM); err != nil {
		log.Errorf("block: %s Fadvise() error(%v)", b.File)
		return
	}
	rsize = needle.BlockOffset(b.Offset)
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(rsize, os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
		return
	}
	// recheck offset, keep size and offset consistency
	if b.Size != rsize {
		log.Warningf("block: %s [real size: %d, offset: %d] but [size: %d, offset: %d] not consistency, truncate file for force recovery, this may lost data",
			b.File, b.Size, needle.NeedleOffset(b.Size),
			rsize, b.Offset)
		// truncate file
		if err = b.w.Truncate(rsize); err != nil {
			log.Errorf("block: %s Truncate() error(%v)", b.File, err)
		}
	}
	return
}

// Compact compact the orig block, copy to disk dst block.
func (b *SuperBlock) Compact(offset uint32, fn func(*needle.Needle, uint32, uint32) error) (err error) {
	if b.LastErr != nil {
		return b.LastErr
	}
	var r *os.File
	if r, err = os.OpenFile(b.File, os.O_RDONLY|myos.O_NOATIME, 0664); err != nil {
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
	if b.w, err = os.OpenFile(b.File, os.O_WRONLY|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		return
	}
	if b.r, err = os.OpenFile(b.File, os.O_RDONLY|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		b.Close()
		return
	}
	if err = b.init(); err != nil {
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
		if err = b.flush(true); err != nil {
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
