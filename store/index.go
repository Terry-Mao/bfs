package main

import (
	"bufio"
	"fmt"
	log "github.com/golang/glog"
	"io"
	"os"
)

// Index for fast recovery super block needle cache in memory, index is async
// append the needle meta data.
//
// index file format:
//  ---------------
// | super   block |
//  ---------------
// |     needle    |		   ----------------
// |     needle    |          |  key (int64)   |
// |     needle    | ---->    |  offset (uint) |
// |     needle    |          |  size (int32)  |
// |     ......    |           ----------------
// |     ......    |             int bigendian
//
// field     | explanation
// --------------------------------------------------
// key       | needle key (photo id)
// offset    | needle offset in super block (aligned)
// size      | needle data size

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

// IndexerStatus is the indexer status.
type IndexerStatus struct {
	Status  int
	SastErr error
}

// Indexer used for fast recovery super block needle cache.
type Indexer struct {
	f       *os.File
	bw      *bufio.Writer
	bufSize int
	signal  chan int
	ring    *Ring
	File    string
	buf     [indexSize]byte
	Status  IndexerStatus
}

// Index index data.
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

// NewIndexer new a indexer for async merge index data to disk.
func NewIndexer(file string, ring, buf int) (indexer *Indexer, err error) {
	indexer = &Indexer{}
	indexer.signal = make(chan int, signalNum)
	indexer.ring = NewRing(ring)
	indexer.File = file
	if indexer.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDWR|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	indexer.bw = bufio.NewWriterSize(indexer.f, buf)
	indexer.bufSize = buf
	go indexer.write()
	return
}

// ready wake up indexer write goroutine if ready.
func (i *Indexer) ready() bool {
	return (<-i.signal) == indexReady
}

// Signal wake up indexer write goroutine merge index data.
func (i *Indexer) Signal() {
	// just ignore duplication signal
	select {
	case i.signal <- indexReady:
	default:
	}
}

// fill file indexer buf with index data.
func (i *Indexer) fill(key int64, offset uint32, size int32) {
	BigEndian.PutInt64(i.buf[:], key)
	BigEndian.PutUint32(i.buf[indexOffsetOffset:], offset)
	BigEndian.PutInt32(i.buf[indexSizeOffset:], size)
	return
}

// Add append a index data to ring, signal bg goroutine merge to disk.
func (i *Indexer) Add(key int64, offset uint32, size int32) (err error) {
	if err = i.Append(key, offset, size); err != nil {
		return
	}
	i.Signal()
	return
}

// Append append a index data to ring.
func (i *Indexer) Append(key int64, offset uint32, size int32) (err error) {
	var (
		index *Index
	)
	if index, err = i.ring.Set(); err != nil {
		log.Errorf("index ring buffer full")
		return
	}
	index.Key = key
	index.Offset = offset
	index.Size = size
	i.ring.SetAdv()
	return
}

// Write append index needle to disk, WARN can't concurrency with write.
func (i *Indexer) Write(key int64, offset uint32, size int32) (err error) {
	i.fill(key, offset, size)
	if _, err = i.bw.Write(i.buf[:]); err != nil {
		return
	}
	return
}

// Flush flush writer buffer.
func (i *Indexer) Flush() (err error) {
	if err = i.bw.Flush(); err != nil {
		return
	}
	// TODO append N times call flush then clean the os page cache
	// page cache no used here...
	// after upload a photo, we cache in user-level.
	return
}

func (i *Indexer) merge() (err error) {
	var index *Index
	for {
		if index, err = i.ring.Get(); err != nil {
			err = nil
			break
		}
		// merge index buffer
		i.fill(index.Key, index.Offset, index.Size)
		if _, err = i.bw.Write(i.buf[:]); err != nil {
			log.Errorf("index write error(%v)", err)
			break
		}
		i.ring.GetAdv()
	}
	return
}

// write merge from ring index data, then write to disk.
func (i *Indexer) write() {
	var (
		err error
	)
	log.Infof("start index: %s merge write goroutine", i.File)
	for {
		if !i.ready() {
			log.Info("signal index write goroutine exit")
			break
		}
		if err = i.merge(); err != nil {
			log.Errorf("index merge error(%v)", err)
			break
		}
		if err = i.Flush(); err != nil {
			break
		}
	}
	if err = i.merge(); err != nil {
		log.Errorf("index merge error(%v)", err)
	}
	if err = i.f.Sync(); err != nil {
		log.Errorf("index file sync error(%v)", err)
	}
	if err = i.f.Close(); err != nil {
		log.Errorf("index file close error(%v)", err)
	}
	log.Errorf("index write goroutine exit")
	return
}

// Recovery recovery needle cache meta data in memory, index file  will stop
// at the right parse data offset.
func (i *Indexer) Recovery(needles map[int64]NeedleCache) (noffset uint32, err error) {
	var (
		rd     *bufio.Reader
		data   []byte
		offset int64
		ix     = &Index{}
	)
	if offset, err = i.f.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("index seek offset error(%v)", err)
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
		if ix.Size > NeedleMaxSize {
			log.Warningf("index parse size: %d > %d", ix.Size, NeedleMaxSize)
			break
		}
		if _, err = rd.Discard(indexSize); err != nil {
			break
		}
		log.V(1).Info(ix.String())
		offset += int64(indexSize)
		needles[ix.Key] = NewNeedleCache(ix.Offset, ix.Size)
		// save this for recovery supper block
		noffset = ix.Offset + NeedleOffset(int(ix.Size))
	}
	if err != io.EOF {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	log.V(1).Infof("right index seek offset: %d\n", offset)
	if _, err = i.f.Seek(offset, os.SEEK_SET); err != nil {
		log.Errorf("index reset offset error(%v)", err)
	}
	return
}

// Close close the indexer file.
func (i *Indexer) Close() {
	close(i.signal)
	return
}
