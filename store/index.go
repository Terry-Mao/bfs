package main

import (
	"bufio"
	"fmt"
	log "github.com/golang/glog"
	"os"
)

const (
	// signal command
	signalNum   = 1
	indexFinish = 0
	indexReady  = 1
	// index size
	indexKeySize    = 8
	indexOffsetSize = 4
	indexSizeSize   = 4
	indexSize       = indexKeySize + indexOffsetSize + indexSizeSize
	// index offset
	indexKeyOffset    = 0
	indexOffsetOffset = indexKeyOffset + indexKeySize
	indexSizeOffset   = indexOffsetOffset + indexOffsetSize
)

type Indexer struct {
	f       *os.File
	bw      *bufio.Writer
	bufSize int
	signal  chan int
	ring    *Ring
}

type Index struct {
	Key    int64
	Offset uint32
	Size   int32
}

func (i *Index) String() string {
	return fmt.Sprintf(`
-----------------------------
Key:            %d
Offset:         %d
Size:           %d
-----------------------------
	`, i.Key, i.Offset, i.Size)
}

func NewIndexer(file string, chNum, buf int) (indexer *Indexer, err error) {
	indexer = &Indexer{}
	indexer.signal = make(chan int, signalNum)
	indexer.ring = NewRing(chNum)
	if indexer.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		return
	}
	indexer.bw = bufio.NewWriterSize(indexer.f, buf)
	indexer.bufSize = buf
	go indexer.write()
	return
}

func (i *Indexer) Ready() bool {
	return (<-i.signal) == indexReady
}

func (i *Indexer) Signal() {
	// just ignore duplication signal
	select {
	case i.signal <- indexReady:
	default:
	}
}

// Add add a index data to ring, signal bg goroutine merge to disk.
func (i *Indexer) Add(key int64, offset uint32, size int32) (err error) {
	var (
		index *Index
	)
	if index, err = i.ring.Set(); err != nil {
		return
	}
	index.Key = key
	index.Offset = offset
	index.Size = size
	i.ring.SetAdv()
	i.Signal()
	return
}

// write merge from ring index data, then write to disk.
func (i *Indexer) write() {
	var (
		err   error
		index *Index
		buf   = make([]byte, indexSize)
	)
	for {
		if !i.Ready() {
			return
		}
		for {
			if index, err = i.ring.Get(); err != nil {
				// must be empty error
				break
			}
			// merge index buffer
			BigEndian.PutInt64(buf[indexKeyOffset:], index.Key)
			BigEndian.PutUint32(buf[indexOffsetOffset:], index.Offset)
			BigEndian.PutInt32(buf[indexSizeOffset:], index.Size)
			if _, err = i.bw.Write(buf); err != nil {
				return
			}
			i.ring.GetAdv()
		}
		// write to disk
		if err = i.bw.Flush(); err != nil {
			return
		}
		// TODO append N times call flush then clean the os page cache
		// page cache no used here...
		// after upload a photo, we cache in user-level.
	}
	return
}

func (i *Indexer) Recovery(needles map[int64]NeedleCache) (noffset uint32, err error) {
	var (
		rd     *bufio.Reader
		data   []byte
		offset int64
		ix     = &Index{}
	)
	if offset, err = i.f.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	rd = bufio.NewReaderSize(i.f, i.bufSize)
	for {
		// parse data
		if data, err = rd.Peek(indexSize); err != nil {
			break
		}
		ix.Key = BigEndian.Int64(data)
		ix.Offset = BigEndian.Uint32(data[indexOffsetOffset:])
		ix.Size = BigEndian.Int32(data[indexSizeOffset:])
		// check
		if ix.Offset%NeedlePaddingSize != 0 {
			err = ErrIndexOffset
			break
		}
		if ix.Size > NeedleMaxSize {
			err = ErrIndexSize
			break
		}
		if _, err = rd.Discard(indexSize); err != nil {
			break
		}
		log.Info(ix.String())
		offset += int64(indexSize)
		needles[ix.Key] = NewNeedleCache(ix.Size, ix.Offset)
		// save this for recovery supper block
		noffset = ix.Offset + NeedleOffset(int(ix.Size))
	}
	// reset b.w offset, discard left space which can't parse to a needle
	log.V(1).Infof("index seek offset: %d\n", offset)
	if _, err = i.f.Seek(offset, os.SEEK_SET); err != nil {
		return
	}
	return
}
