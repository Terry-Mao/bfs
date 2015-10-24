package main

import (
	"bufio"
	"bytes"
	log "github.com/golang/glog"
	"io"
	"os"
)

const (
	// offset
	superBlockHeaderOffset = 8
	// size
	superBlockHeaderSize  = 8
	superBlockMagicSize   = 4
	superBlockVerSize     = 1
	superBlockPaddingSize = superBlockHeaderSize - superBlockMagicSize -
		superBlockVerSize
	// offset
	superBlockMagicOffset   = 0
	superBlockVerOffset     = superBlockMagicOffset + superBlockVerSize
	superBlockPaddingOffset = superBlockVerOffset + superBlockPaddingSize
	// ver
	superBlockVer1 = byte(1)
	// limits
	// 32GB, offset aligned 8 bytes, 4GB * 8
	superBlockMaxSize   = 4 * 1024 * 1024 * 1024 * 8
	superBlockMaxOffset = 4294967295
)

var (
	superBlockMagic   = []byte{0xab, 0xcd, 0xef, 0x00}
	superBlockVer     = []byte{superBlockVer1}
	superBlockPadding = []byte{0x00, 0x00, 0x00}
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

// NewSuperBlock new a super block struct.
func NewSuperBlock(file string) (b *SuperBlock, err error) {
	b = &SuperBlock{}
	b.File = file
	b.buf = make([]byte, NeedleMaxSize)
	if b.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_WRONLY|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	b.bw = bufio.NewWriterSize(b.w, NeedleMaxSize)
	if b.r, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDONLY, 0664) error(%v)", file, err)
		goto failed
	}
	if err = b.init(); err != nil {
		log.Errorf("block: %s init error(%v)", file, err)
		goto failed
	}
	return
failed:
	if b.w != nil {
		b.w.Close()
	}
	if b.r != nil {
		b.r.Close()
	}
	return
}

// init block file, add/parse meta info
func (b *SuperBlock) init() (err error) {
	var (
		stat os.FileInfo
	)
	if stat, err = b.r.Stat(); err != nil {
		log.Errorf("block: %s Stat() error(%v)", b.File, err)
		return
	}
	// new file
	if stat.Size() == 0 {
		// magic
		if _, err = b.w.Write(superBlockMagic); err != nil {
			return
		}
		// ver
		if _, err = b.w.Write(superBlockVer); err != nil {
			return
		}
		// padding
		if _, err = b.w.Write(superBlockPadding); err != nil {
			return
		}
	} else {
		if _, err = b.r.Read(b.buf[:superBlockHeaderSize]); err != nil {
			return
		}
		// parse block meta data
		// check magic
		b.Magic = b.buf[superBlockMagicOffset : superBlockMagicOffset+superBlockMagicSize]
		b.Ver = byte(b.buf[superBlockVerOffset : superBlockVerOffset+superBlockVerSize][0])
		if !bytes.Equal(b.Magic, superBlockMagic) {
			err = ErrSuperBlockMagic
			return
		}
		if b.Ver == superBlockVer1 {
			err = ErrSuperBlockVer
			return
		}
		if _, err = b.w.Seek(superBlockHeaderOffset, os.SEEK_SET); err != nil {
			log.Errorf("block: %s Seek() error(%v)", b.File, err)
			return
		}
	}
	b.Offset = NeedleOffset(superBlockHeaderOffset)
	return
}

// writeNeedle get a needle size by data.
func (b *SuperBlock) writeNeedle(key, cookie int64, data []byte) (size int32, incrOffset uint32, err error) {
	var (
		padding  int32
		dataSize = int32(len(data))
	)
	if padding, size, err = NeedleSize(dataSize); err != nil {
		// if err is needle too large don't set last error
		return
	}
	incrOffset = NeedleOffset(int64(size))
	if superBlockMaxOffset-incrOffset < b.Offset {
		err = ErrSuperBlockNoSpace
		b.LastErr = err
		return
	}
	if err = WriteNeedle(b.bw, padding, dataSize, key, cookie, data); err != nil {
		b.LastErr = err
	}
	return
}

// Add append a photo to the block.
func (b *SuperBlock) Add(key, cookie int64, data []byte) (offset uint32, size int32, err error) {
	var incrOffset uint32
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	if size, incrOffset, err = b.writeNeedle(key, cookie, data); err != nil {
		return
	}
	if err = b.Flush(); err != nil {
		return
	}
	offset = b.Offset
	b.Offset += incrOffset
	log.V(1).Infof("add a needle, key: %d, cookie: %d, offset: %d, size: %d, b.offset: %d", key, cookie, offset, size, b.Offset)
	return
}

// Write start add needles to the block, must called after start a transaction.
func (b *SuperBlock) Write(key, cookie int64, data []byte) (offset uint32, size int32, err error) {
	var incrOffset uint32
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	if size, incrOffset, err = b.writeNeedle(key, cookie, data); err != nil {
		return
	}
	offset = b.Offset
	b.Offset += incrOffset
	return
}

// Flush flush writer buffer.
func (b *SuperBlock) Flush() (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	for {
		// write may be less than request, we call flush in a loop
		if err = b.bw.Flush(); err != nil && err != io.ErrShortWrite {
			b.LastErr = err
			log.Errorf("block: %s Flush() error(%v)", b.File, err)
			return
		} else if err == io.ErrShortWrite {
			continue
		}
		// TODO append N times call flush then clean the os page cache
		// page cache no used here...
		// after upload a photo, we cache in user-level.
		break
	}
	return
}

// Repair repair the specified offset needle without update current offset.
func (b *SuperBlock) Repair(key, cookie int64, data []byte, size int32, offset uint32) (err error) {
	var (
		nsize    int32
		padding  int32
		dataSize = int32(len(data))
	)
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	if padding, nsize, err = NeedleSize(dataSize); err != nil {
		return
	}
	if nsize != size {
		err = ErrSuperBlockRepairSize
		return
	}
	FillNeedle(padding, dataSize, key, cookie, data, b.buf)
	_, err = b.w.WriteAt(b.buf[:nsize], BlockOffset(offset))
	b.LastErr = err
	return
}

// Get get a needle from super block.
func (b *SuperBlock) Get(offset uint32, buf []byte) (err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	_, err = b.r.ReadAt(buf, BlockOffset(offset))
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
	_, err = b.w.WriteAt(NeedleStatusDelBytes, BlockOffset(offset)+NeedleFlagOffset)
	b.LastErr = err
	return
}

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(needles map[int64]NeedleCache, indexer *Indexer, offset int64) (err error) {
	var (
		size    int32
		data    []byte
		rd      *bufio.Reader
		n       = &Needle{}
		nc      NeedleCache
		noffset uint32
	)
	log.Infof("block: %s recovery from offset: %d", b.File, offset)
	if offset == 0 {
		offset = superBlockHeaderOffset
	}
	noffset = NeedleOffset(offset)
	if _, err = b.r.Seek(offset, os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File)
		return
	}
	rd = bufio.NewReaderSize(b.r, NeedleMaxSize)
	for {
		// header
		if data, err = rd.Peek(NeedleHeaderSize); err != nil {
			break
		}
		if err = n.ParseHeader(data); err != nil {
			break
		}
		if _, err = rd.Discard(NeedleHeaderSize); err != nil {
			break
		}
		// data
		if data, err = rd.Peek(n.DataSize); err != nil {
			break
		}
		if err = n.ParseData(data); err != nil {
			break
		}
		if _, err = rd.Discard(n.DataSize); err != nil {
			break
		}
		size = int32(NeedleHeaderSize + n.DataSize)
		if n.Flag == NeedleStatusOK {
			if err = indexer.Add(n.Key, noffset, size); err != nil {
				break
			}
			nc = NewNeedleCache(noffset, size)
		} else {
			nc = NewNeedleCache(NeedleCacheDelOffset, size)
		}
		needles[n.Key] = nc
		log.V(1).Infof("block add offset: %d, size: %d to needles cache", noffset, size)
		log.V(1).Info(n.String())
		noffset += NeedleOffset(int64(size))
	}
	if err == io.EOF {
		err = nil
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(BlockOffset(noffset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
	}
	return
}

// Compress compress the orig block, copy to disk dst block.
func (b *SuperBlock) Compress(offset int64, v *Volume) (noffset int64, err error) {
	var (
		data []byte
		r    *os.File
		rd   *bufio.Reader
		n    *Needle
	)
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	n = &Needle{}
	log.Infof("block: %s compress", b.File)
	if r, err = os.OpenFile(b.File, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDONLY, 0664) error(%v)", b.File, err)
		return
	}
	if offset == 0 {
		offset = superBlockHeaderOffset
	}
	if _, err = r.Seek(offset, os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
		return
	}
	rd = bufio.NewReaderSize(r, NeedleMaxSize)
	for {
		// header
		if data, err = rd.Peek(NeedleHeaderSize); err != nil {
			break
		}
		if err = n.ParseHeader(data); err != nil {
			break
		}
		if _, err = rd.Discard(NeedleHeaderSize); err != nil {
			break
		}
		// data
		if data, err = rd.Peek(n.DataSize); err != nil {
			break
		}
		if err = n.ParseData(data); err != nil {
			break
		}
		if _, err = rd.Discard(n.DataSize); err != nil {
			break
		}
		offset += int64(NeedleHeaderSize + n.DataSize)
		log.V(1).Info(n.String())
		// skip delete needle
		if n.Flag == NeedleStatusDel {
			continue
		}
		// multi append
		if err = v.Write(n.Key, n.Cookie, n.Data); err != nil {
			break
		}
	}
	if err != io.EOF {
		return
	}
	if err = v.Flush(); err != nil {
		return
	}
	if err = r.Close(); err != nil {
		return
	}
	noffset = offset
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

// BlockOffset get super block file offset.
func BlockOffset(offset uint32) int64 {
	return int64(offset) * NeedlePaddingSize
}
