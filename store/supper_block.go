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

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(needles map[int64]int64, indexer *Indexer, offset uint32) (err error) {
	var (
		n    = &Needle{}
		nc   int64
		rd   *bufio.Reader
		size int32
		data []byte
	)
	log.Infof("block: %s recovery from offset: %d", b.File, offset)
	if offset == 0 {
		offset = NeedleOffset(superBlockHeaderOffset)
	}
	if _, err = b.r.Seek(blockOffset(offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File)
		return
	}
	rd = bufio.NewReaderSize(b.r, NeedleMaxSize)
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
		size = int32(NeedleHeaderSize + n.DataSize)
		if n.Flag == NeedleStatusOK {
			if err = indexer.Write(n.Key, offset, size); err != nil {
				break
			}
			nc = NeedleCache(offset, size)
		} else {
			nc = NeedleCache(NeedleCacheDelOffset, size)
		}
		needles[n.Key] = nc
		if log.V(1) {
			log.Infof("block add offset: %d, size: %d to needles cache", offset, size)
			log.Info(n.String())
		}
		offset += NeedleOffset(int64(size))
	}
	if err == io.EOF {
		err = nil
	}
	if err = indexer.Flush(); err != nil {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(blockOffset(offset), os.SEEK_SET); err != nil {
		log.Errorf("block: %s Seek() error(%v)", b.File, err)
	}
	return
}

// Compress compress the orig block, copy to disk dst block.
func (b *SuperBlock) Compress(offset int64, v *Volume) (noffset int64, err error) {
	if b.LastErr != nil {
		err = b.LastErr
		return
	}
	var (
		r    *os.File
		rd   *bufio.Reader
		data []byte
		n    = &Needle{}
	)
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
		offset += int64(n.TotalSize)
		if log.V(1) {
			log.Info(n.String())
		}
		// skip delete needle
		if n.Flag == NeedleStatusDel {
			continue
		}
		// multi append
		if err = v.Write(n); err != nil {
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

// blockOffset get super block file offset.
func blockOffset(offset uint32) int64 {
	return int64(offset) * NeedlePaddingSize
}
