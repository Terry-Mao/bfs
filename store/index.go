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
	signalNum  = 1
	indexReady = 1
	// index size
	indexKeySize    = 8
	indexOffsetSize = 4
	indexSizeSize   = 4
	indexSize       = indexKeySize + indexOffsetSize + indexSizeSize
	// index offset
	indexKeyOffset    = 0
	indexOffsetOffset = indexKeyOffset + indexKeySize
	indexSizeOffset   = indexOffsetOffset + indexOffsetSize

	indexMaxSize = 100 * 1024 * 1024 // 100mb
)

// Indexer used for fast recovery super block needle cache.
type Indexer struct {
	f       *os.File
	bw      *bufio.Writer
	sigNum  int
	signal  chan int
	ring    *Ring
	File    string `json:"file"`
	LastErr error  `json:"last_err"`
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
func NewIndexer(file string, ring int) (i *Indexer, err error) {
	var (
		stat os.FileInfo
	)
	i = &Indexer{}
	i.signal = make(chan int, signalNum)
	i.ring = NewRing(ring)
	i.sigNum = ring / 2
	i.File = file
	if i.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		return
	}
	if stat, err = i.f.Stat(); err != nil {
		log.Errorf("index: %s Stat() error(%v)", i.File, err)
		return
	}
	if stat.Size() == 0 {
		// falloc(FALLOC_FL_KEEP_SIZE)
		if err = Fallocate(i.f.Fd(), 1, 0, indexMaxSize); err != nil {
			log.Errorf("Fallocate(i.f.Fd(), 1, 0, 100MB) error(err)", err)
			return
		}
	}
	i.bw = bufio.NewWriterSize(i.f, NeedleMaxSize)
	go i.write()
	return
}

// Open open the closed indexer, must called after NewIndexer.
func (i *Indexer) Open() (err error) {
	i.signal = make(chan int, signalNum)
	if i.f, err = os.OpenFile(i.File, os.O_RDWR|os.O_CREATE, 0664); err !=
		nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", i.File, err)
		return
	}
	i.bw.Reset(i.f)
	go i.write()
	return
}

// Add append a index data to ring.
func (i *Indexer) Add(key int64, offset uint32, size int32) (err error) {
	var (
		index *Index
	)
	if i.LastErr != nil {
		err = i.LastErr
		return
	}
	if i.ring.Buffered() > i.sigNum {
		select {
		case i.signal <- indexReady:
		default:
		}
	}
	if index, err = i.ring.Set(); err != nil {
		i.LastErr = err
		return
	}
	index.Key = key
	index.Offset = offset
	index.Size = size
	i.ring.SetAdv()
	return
}

// Write append index needle to disk.
// WARN can't concurrency with merge and write.
// ONLY used in super block recovery!!!!!!!!!!!
func (i *Indexer) Write(key int64, offset uint32, size int32) (err error) {
	if i.LastErr != nil {
		err = i.LastErr
		return
	}
	if err = BigEndian.WriteInt64(i.bw, key); err != nil {
		i.LastErr = err
		return
	}
	if err = BigEndian.WriteUint32(i.bw, offset); err != nil {
		i.LastErr = err
		return
	}
	if err = BigEndian.WriteInt32(i.bw, size); err != nil {
		i.LastErr = err
	}
	return
}

// Flush flush writer buffer.
func (i *Indexer) Flush() (err error) {
	if i.LastErr != nil {
		err = i.LastErr
		return
	}
	if err = i.bw.Flush(); err != nil {
		i.LastErr = err
		log.Errorf("index: %s Flush() error(%v)", i.File, err)
		return
	}
	// TODO append N times call flush then clean the os page cache
	// page cache no used here...
	// after upload a photo, we cache in user-level.
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
		if err = i.Write(index.Key, index.Offset, index.Size); err != nil {
			log.Errorf("index: %s Write() error(%v)", i.File, err)
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
	for {
		if !((<-i.signal) == indexReady) {
			break
		}
		if err = i.merge(); err != nil {
			break
		}
		if err = i.Flush(); err != nil {
			break
		}
	}
	i.merge()
	i.Flush()
	if err = i.f.Sync(); err != nil {
		log.Errorf("index: %s Sync() error(%v)", i.File, err)
	}
	if err = i.f.Close(); err != nil {
		log.Errorf("index: %s Close() error(%v)", i.File, err)
	}
	return
}

// Recovery recovery needle cache meta data in memory, index file  will stop
// at the right parse data offset.
func (i *Indexer) Recovery(fn func(*Index) error) (noffset uint32, err error) {
	var (
		rd     *bufio.Reader
		data   []byte
		offset int64
		ix     = &Index{}
	)
	log.Infof("index: %s recovery", i.File)
	if offset, err = i.f.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("index: %s Seek() error(%v)", i.File, err)
		return
	}
	rd = bufio.NewReaderSize(i.f, NeedleMaxSize)
	for {
		if data, err = rd.Peek(indexSize); err != nil {
			break
		}
		ix.parse(data)
		if ix.Size > NeedleMaxSize || ix.Size < 1 {
			log.Errorf("index parse size: %d error", ix.Size)
			err = ErrIndexSize
			break
		}
		if _, err = rd.Discard(indexSize); err != nil {
			break
		}
		if log.V(1) {
			log.Info(ix.String())
		}
		offset += int64(indexSize)
		if err = fn(ix); err != nil {
			break
		}
		// save this for recovery supper block
		noffset = ix.Offset + NeedleOffset(int64(ix.Size))
	}
	if err == io.EOF {
		// reset b.w offset, discard left space which can't parse to a needle
		if _, err = i.f.Seek(offset, os.SEEK_SET); err != nil {
			log.Errorf("index: %s Seek() error(%v)", i.File, err)
		} else {
			log.Infof("index: %s recovery [ok]", i.File)
			return
		}
	}
	log.Errorf("index: %s recovery [failed], error(%v)", i.File, err)
	return
}

// Close close the indexer file.
func (i *Indexer) Close() {
	close(i.signal)
	return
}
