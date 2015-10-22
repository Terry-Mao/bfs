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

// Indexer used for fast recovery super block needle cache.
type Indexer struct {
	f      *os.File
	bw     *bufio.Writer
	signal chan int
	ring   *Ring
	file   string
}

// Index index data.
type Index struct {
	Key    int64
	Offset uint32
	Size   int32
}

// parse parse buffer into indexer.
func (i *Index) parse(buf []byte) {
	i.Key = BigEndian.Int64(buf)
	i.Offset = BigEndian.Uint32(buf[indexOffsetOffset:])
	i.Size = BigEndian.Int32(buf[indexSizeOffset:])
	return
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
func NewIndexer(file string, ring int) (indexer *Indexer, err error) {
	indexer = &Indexer{}
	indexer.signal = make(chan int, signalNum)
	indexer.ring = NewRing(ring)
	indexer.file = file
	if indexer.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDWR|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	indexer.bw = bufio.NewWriterSize(indexer.f, NeedleMaxSize)
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

// writeIndex write index data into bufio.
func writeIndex(w *bufio.Writer, key int64, offset uint32, size int32) (err error) {
	if err = BigEndian.WriteInt64(w, key); err != nil {
		return
	}
	if err = BigEndian.WriteUint32(w, offset); err != nil {
		return
	}
	err = BigEndian.WriteInt32(w, size)
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
	err = writeIndex(i.bw, key, offset, size)
	return
}

// Flush flush writer buffer.
func (i *Indexer) Flush() (err error) {
	for {
		// write may be less than request, we call flush in a loop
		if err = i.bw.Flush(); err != nil && err != io.ErrShortWrite {
			log.Errorf("index: %s Flush() error(%v)", i.file, err)
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

// merge get index data from ring then write to disk.
func (i *Indexer) merge() (err error) {
	var index *Index
	for {
		if index, err = i.ring.Get(); err != nil {
			err = nil
			break
		}
		// merge index buffer
		if err = writeIndex(i.bw, index.Key, index.Offset, index.Size); err != nil {
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
	log.Infof("index: %s merge write goroutine", i.file)
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
		log.Errorf("index: %s Sync() error(%v)", i.file, err)
	}
	err = i.f.Close()
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
	log.Infof("index: %s recovery", i.file)
	if offset, err = i.f.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("index: %s Seek() error(%v)", i.file, err)
		return
	}
	rd = bufio.NewReaderSize(i.f, NeedleMaxSize)
	for {
		// parse data
		if data, err = rd.Peek(indexSize); err != nil {
			break
		}
		ix.parse(data)
		// check
		if ix.Size > NeedleMaxSize || ix.Size < 1 {
			log.Errorf("index parse size: %d > %d or %d < 1", ix.Size, NeedleMaxSize, ix.Size)
			break
		}
		if _, err = rd.Discard(indexSize); err != nil {
			break
		}
		log.V(1).Info(ix.String())
		offset += int64(indexSize)
		needles[ix.Key] = NewNeedleCache(ix.Offset, ix.Size)
		// save this for recovery supper block
		noffset = ix.Offset + NeedleOffset(int64(ix.Size))
	}
	if err != io.EOF {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = i.f.Seek(offset, os.SEEK_SET); err != nil {
		log.Errorf("index: %s Seek() error(%v)", i.file, err)
	}
	log.Infof("index: %s recovery [ok]", i.file)
	return
}

// Close close the indexer file.
func (i *Indexer) Close() {
	close(i.signal)
	return
}
