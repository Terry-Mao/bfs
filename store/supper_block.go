package main

import (
	"bufio"
	log "github.com/golang/glog"
	"io"
	"os"
)

const (
	SupperBlockSize = 16
)

// An Volume contains one superblock and many needles.
type SuperBlock struct {
	r        *os.File
	w        *os.File
	file     string
	offset   uint32
	MagicNum int32
	Version  byte
	buf      [NeedleMaxSize]byte
	// TODO stat
}

func NewSuperBlock(file string) (s *SuperBlock, err error) {
	s = &SuperBlock{}
	s.file = file
	if s.w, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0664); err != nil {
		return
	}
	if s.r, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		return
	}
	return
}

// AppendNeedle append a photo to the block.
func (b *SuperBlock) Append(key, cookie int64, data []byte) (size int32, offset uint32, err error) {
	var n int
	size = FillNeedleBuf(key, cookie, data, b.buf[:])
	if n, err = b.w.Write(b.buf[:size]); err != nil {
		return
	}
	offset = b.offset
	b.offset += NeedleOffset(n)
	// TODO append N times call flush then clean the os page cache
	// page cache no used here...
	// after upload a photo, we cache in user-level.
	return
}

// Needle get a needle from super block.
func (b *SuperBlock) Read(offset uint32, buf []byte) (err error) {
	if _, err = b.r.ReadAt(buf, BlockOffset(offset)); err != nil {
		return
	}
	return
}

// Del logical del a needls, only update the flag to it.
func (b *SuperBlock) Del(offset uint32) (err error) {
	// WriteAt won't update the file offset.
	if _, err = b.w.WriteAt(NeedleStatusDelBytes, BlockOffset(offset)+NeedleFlagOffset); err != nil {
		return
	}
	return
}

// Dump parse supper block file and dump print for debug.
// ONLY DEBUG!!!!
func (b *SuperBlock) Dump() (err error) {
	var (
		rd   *bufio.Reader
		data []byte
		n    = &Needle{}
	)
	if _, err = b.r.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	rd = bufio.NewReaderSize(b.r, NeedleMaxSize)
	for {
		// header
		if data, err = rd.Peek(NeedleHeaderSize); err != nil {
			break
		}
		if err = ParseNeedleHeader(data, n); err != nil {
			break
		}
		if _, err = rd.Discard(NeedleHeaderSize); err != nil {
			break
		}
		// data
		if data, err = rd.Peek(n.DataSize); err != nil {
			break
		}
		if err = ParseNeedleData(data, n); err != nil {
			break
		}
		if _, err = rd.Discard(n.DataSize); err != nil {
			break
		}
		log.Info(n.String())
	}
	if err == io.EOF {
		err = nil
	}
	return
}

// Recovery recovery needles map from super block.
func (b *SuperBlock) Recovery(needles map[int64]NeedleCache, indexer *Indexer, offset int64) (err error) {
	var (
		rd      *bufio.Reader
		data    []byte
		size    int32
		noffset uint32
		n       = &Needle{}
	)
	log.Infof("start super block recovery, offset: %d\n", offset)
	if _, err = b.r.Seek(offset, os.SEEK_SET); err != nil {
		return
	}
	rd = bufio.NewReaderSize(b.r, NeedleMaxSize)
	for {
		// header
		if data, err = rd.Peek(NeedleHeaderSize); err != nil {
			break
		}
		if err = ParseNeedleHeader(data, n); err != nil {
			break
		}
		if _, err = rd.Discard(NeedleHeaderSize); err != nil {
			break
		}
		// data
		if data, err = rd.Peek(n.DataSize); err != nil {
			break
		}
		if err = ParseNeedleData(data, n); err != nil {
			break
		}
		if _, err = rd.Discard(n.DataSize); err != nil {
			break
		}
		log.Info(n.String())
		size = int32(NeedleHeaderSize + n.DataSize)
		noffset += NeedleOffset(int(size))
		needles[n.Key] = NewNeedleCache(size, noffset)
		indexer.Add(n.Key, noffset, size)
	}
	if err == io.EOF {
		err = nil
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = b.w.Seek(BlockOffset(noffset), os.SEEK_SET); err != nil {
		return
	}
	return
}
