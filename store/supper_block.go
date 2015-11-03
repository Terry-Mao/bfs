package main

import (
	"bufio"
	"bytes"
	log "github.com/golang/glog"
	"io"
	"os"
)

// Super block has a header.
// super block header format:
//  --------------
// | magic number |   ---- 4bytes
// | version      |   ---- 1byte
// | padding      |   ---- aligned with needle padding size (for futer used)
//  --------------
//

const (
	// size
	superBlockHeaderSize  = NeedlePaddingSize
	superBlockMagicSize   = 4
	superBlockVerSize     = 1
	superBlockPaddingSize = superBlockHeaderSize - superBlockMagicSize - superBlockVerSize
	// offset
	superBlockHeaderOffset  = superBlockHeaderSize
	superBlockMagicOffset   = 0
	superBlockVerOffset     = superBlockMagicOffset + superBlockVerSize
	superBlockPaddingOffset = superBlockVerOffset + superBlockPaddingSize
	// ver
	superBlockVer1 = byte(1)
	// limits
	// offset aligned 8 bytes, 4GB * needle_padding_size
	superBlockMaxSize   = 4 * 1024 * 1024 * 1024 * NeedlePaddingSize
	superBlockMaxOffset = 4294967295
)

var (
	superBlockMagic   = []byte{0xab, 0xcd, 0xef, 0x00}
	superBlockVer     = []byte{superBlockVer1}
	superBlockPadding = bytes.Repeat([]byte{0x0}, superBlockPaddingSize)
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
	// meta
	Magic []byte `json:"-"`
	Ver   byte   `json:"ver"`
}

// NewSuperBlock creae a new super block.
func NewSuperBlock(file string) (b *SuperBlock, err error) {
	b = &SuperBlock{}
	b.File = file
	b.Offset = NeedleOffset(superBlockHeaderSize)
	b.buf = make([]byte, NeedleMaxSize)
	if b.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		return
	}
	if b.r, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		goto failed
	}
	b.bw = bufio.NewWriterSize(b.w, NeedleMaxSize)
	if err = b.init(); err != nil {
		log.Errorf("block: %s init() error(%v)", file, err)
		goto failed
	}
	return
failed:
	b.w.Close()
	if b.r != nil {
		b.r.Close()
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
		if err = Fallocate(b.w.Fd(), 1, 0, superBlockMaxSize); err != nil {
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
		if _, err = b.w.Seek(superBlockHeaderOffset, os.SEEK_SET); err != nil {
			log.Errorf("block: %s Seek() error(%v)", b.File, err)
			return
		}
		b.Offset = NeedleOffset(superBlockHeaderOffset)
	}
	return
}

// writeMeta write block meta info.
func (b *SuperBlock) writeMeta() (err error) {
	// magic
	if _, err = b.w.Write(superBlockMagic); err != nil {
		return
	}
	// ver
	if _, err = b.w.Write(superBlockVer); err != nil {
		return
	}
	// padding
	_, err = b.w.Write(superBlockPadding)
	return
}

// parseMeta parse block meta info.
func (b *SuperBlock) parseMeta() (err error) {
	if _, err = b.r.Read(b.buf[:superBlockHeaderSize]); err != nil {
		return
	}
	b.Magic = b.buf[superBlockMagicOffset : superBlockMagicOffset+superBlockMagicSize]
	b.Ver = b.buf[superBlockVerOffset : superBlockVerOffset+superBlockVerSize][0]
	if !bytes.Equal(b.Magic, superBlockMagic) {
		err = ErrSuperBlockMagic
		return
	}
	if b.Ver == superBlockVer1 {
		err = ErrSuperBlockVer
	}
	return
}

// Open open the closed superblock, must called after NewSuperBlock.
func (b *SuperBlock) Open() (err error) {
	if b.w, err = os.OpenFile(b.File, os.O_WRONLY|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		return
	}
	b.bw.Reset(b.w)
	if b.r, err = os.OpenFile(b.File, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		goto failed
	}
	if err = b.init(); err != nil {
		log.Errorf("block: %s init error(%v)", b.File, err)
		goto failed
	}
	return
failed:
	b.w.Close()
	if b.r != nil {
		b.r.Close()
	}
	return
}

// avvailable check block has enough space.
func (b *SuperBlock) available(incrOffset uint32) (err error) {
	if superBlockMaxOffset-incrOffset < b.Offset {
		err = ErrSuperBlockNoSpace
		b.LastErr = err
	}
	return
}

// Add append a photo to the block.
func (b *SuperBlock) Add(n *Needle) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	var incrOffset = NeedleOffset(int64(n.TotalSize))
	if err = b.available(incrOffset); err != nil {
		return
	}
	if err = n.Write(b.bw); err != nil {
		b.LastErr = err
		return
	}
	if err = b.Flush(); err != nil {
		return
	}
	b.Offset += incrOffset
	return
}

// Write start add needles to the block, must called after start a transaction.
func (b *SuperBlock) Write(n *Needle) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	var incrOffset = NeedleOffset(int64(n.TotalSize))
	if err = b.available(incrOffset); err != nil {
		return
	}
	if err = n.Write(b.bw); err != nil {
		b.LastErr = err
		return
	}
	b.Offset += incrOffset
	return
}

// Flush flush writer buffer.
func (b *SuperBlock) Flush() (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	// write may be less than request, we call flush in a loop
	if err = b.bw.Flush(); err != nil {
		b.LastErr = err
		log.Errorf("block: %s Flush() error(%v)", b.File, err)
		return
	}
	// TODO append N times call flush then clean the os page cache
	// page cache no used here...
	// after upload a photo, we cache in user-level.
	return
}

// Repair repair the specified offset needle without update current offset.
func (b *SuperBlock) Repair(offset uint32, buf []byte) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	_, err = b.w.WriteAt(buf, blockOffset(offset))
	b.LastErr = err
	return
}

// Get get a needle from super block.
func (b *SuperBlock) Get(offset uint32, buf []byte) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	_, err = b.r.ReadAt(buf, blockOffset(offset))
	b.LastErr = err
	return
}

// Del logical del a needls, only update the flag to it.
func (b *SuperBlock) Del(offset uint32) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	// WriteAt won't update the file offset.
	_, err = b.w.WriteAt(NeedleStatusDelBytes, blockOffset(offset)+NeedleFlagOffset)
	b.LastErr = err
	return
}

// Scan scan a block file.
func (b *SuperBlock) Scan(r *os.File, offset uint32, fn func(*Needle, uint32, uint32) error) (err error) {
	var (
		soffset, eoffset uint32
		data             []byte
		n                = &Needle{}
		rd               = bufio.NewReaderSize(r, NeedleMaxSize)
	)
	if offset == 0 {
		offset = NeedleOffset(superBlockHeaderOffset)
	}
	soffset, eoffset = offset, offset
	log.Infof("scan block: %s from offset: %d", b.File, offset)
	if _, err = r.Seek(blockOffset(offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File)
		return
	}
	for {
		if data, err = rd.Peek(NeedleHeaderSize); err != nil {
			break
		}
		if err = n.ParseHeader(data); err != nil {
			break
		}
		if _, err = rd.Discard(NeedleHeaderSize); err != nil {
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
		eoffset += NeedleOffset(int64(n.TotalSize))
		if err = fn(n, soffset, eoffset); err != nil {
			break
		}
		soffset = eoffset
	}
	if err != io.EOF {
		log.Infof("scan block: %s to offset: %d error(%v) [failed]", b.File, eoffset, err)
	} else {
		err = nil
		log.Infof("scan block: %s to offset: %d [ok]", b.File, eoffset)
	}
	return
}

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(offset uint32, fn func(*Needle, uint32, uint32) error) (err error) {
	if err = b.Scan(b.r, offset, func(n *Needle, so, eo uint32) (err1 error) {
		if err1 = fn(n, so, eo); err != nil {
			b.Offset = eo
		}
		return
	}); err != nil {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(blockOffset(b.Offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
	}
	return
}

// Compact compact the orig block, copy to disk dst block.
func (b *SuperBlock) Compact(offset uint32, fn func(*Needle, uint32, uint32) error) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	var r *os.File
	if r, err = os.OpenFile(b.File, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", b.File, err)
		return
	}
	if err = b.Scan(r, offset, func(n *Needle, so, eo uint32) (err1 error) {
		err1 = fn(n, so, eo)
		return
	}); err != nil {
		r.Close()
		return
	}
	if err = r.Close(); err != nil {
		log.Errorf("block: %s Close() error(%v)", b.File, err)
	}
	return
}

func (b *SuperBlock) Close() {
	var err error
	if err = b.Flush(); err != nil {
		log.Errorf("block: %s flush error(%v)", b.File, err)
	}
	if err = b.w.Sync(); err != nil {
		log.Errorf("block: %s sync error(%v)", b.File, err)
	}
	if err = b.w.Close(); err != nil {
		log.Errorf("block: %s close error(%v)", b.File, err)
	}
	if err = b.r.Close(); err != nil {
		log.Errorf("block: %s close error(%v)", b.File, err)
	}
	return
}

// blockOffset get super block file offset.
func blockOffset(offset uint32) int64 {
	return int64(offset) * NeedlePaddingSize
}
