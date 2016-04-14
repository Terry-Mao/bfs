package index

import (
	"bfs/libs/encoding/binary"
	"bfs/libs/errors"
	"bfs/store/conf"
	myos "bfs/store/os"
	"bufio"
	"fmt"
	log "github.com/golang/glog"
	"io"
	"os"
	"sync"
	"time"
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
	_finish = 0
	_ready  = 1
	// index size
	_keySize    = 8
	_offsetSize = 4
	_sizeSize   = 4
	// index size = 16
	_indexSize = _keySize + _offsetSize + _sizeSize
	// index offset
	_keyOffset    = 0
	_offsetOffset = _keyOffset + _keySize
	_sizeOffset   = _offsetOffset + _offsetSize
	// 100mb
	_fallocSize = 100 * 1024 * 1024
)

// Indexer used for fast recovery super block needle cache.
type Indexer struct {
	wg     sync.WaitGroup
	f      *os.File
	signal chan int
	ring   *Ring
	// buffer
	buf []byte
	bn  int
	//
	File    string `json:"file"`
	LastErr error  `json:"last_err"`
	Offset  int64  `json:"offset"`
	conf    *conf.Config
	// status
	syncOffset int64
	closed     bool
	write      int
}

// Index index data.
type Index struct {
	Key    int64
	Offset uint32
	Size   int32
}

// parse parse buffer into indexer.
func (i *Index) parse(buf []byte) (err error) {
	i.Key = binary.BigEndian.Int64(buf)
	i.Offset = binary.BigEndian.Uint32(buf[_offsetOffset:])
	i.Size = binary.BigEndian.Int32(buf[_sizeOffset:])
	if i.Size < 0 {
		return errors.ErrIndexSize
	}
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
func NewIndexer(file string, conf *conf.Config) (i *Indexer, err error) {
	var stat os.FileInfo
	i = &Indexer{}
	i.File = file
	i.closed = false
	i.syncOffset = 0
	i.conf = conf
	// must align size
	i.ring = NewRing(conf.Index.RingBuffer)
	i.bn = 0
	i.buf = make([]byte, conf.Index.BufferSize)
	if i.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
		return nil, err
	}
	if stat, err = i.f.Stat(); err != nil {
		log.Errorf("index: %s Stat() error(%v)", i.File, err)
		return nil, err
	}
	if stat.Size() == 0 {
		if err = myos.Fallocate(i.f.Fd(), myos.FALLOC_FL_KEEP_SIZE, 0, _fallocSize); err != nil {
			log.Errorf("index: %s fallocate() error(err)", i.File, err)
			i.Close()
			return nil, err
		}
	}
	i.wg.Add(1)
	i.signal = make(chan int, 1)
	go i.merge()
	return
}

// Signal signal the write job merge index data.
func (i *Indexer) Signal() {
	if i.closed {
		return
	}
	select {
	case i.signal <- _ready:
	default:
	}
}

// Add append a index data to ring.
func (i *Indexer) Add(key int64, offset uint32, size int32) (err error) {
	var index *Index
	if i.LastErr != nil {
		return i.LastErr
	}
	if index, err = i.ring.Set(); err != nil {
		i.LastErr = err
		return
	}
	index.Key = key
	index.Offset = offset
	index.Size = size
	i.ring.SetAdv()
	if i.ring.Buffered() > i.conf.Index.MergeWrite {
		i.Signal()
	}
	return
}

// Write append index needle to disk.
// WARN can't concurrency with merge and write.
// ONLY used in super block recovery!!!!!!!!!!!
func (i *Indexer) Write(key int64, offset uint32, size int32) (err error) {
	if i.LastErr != nil {
		return i.LastErr
	}
	if i.bn+_indexSize >= i.conf.Index.BufferSize {
		// buffer full
		if err = i.flush(true); err != nil {
			return
		}
	}
	binary.BigEndian.PutInt64(i.buf[i.bn:], key)
	i.bn += _keySize
	binary.BigEndian.PutUint32(i.buf[i.bn:], offset)
	i.bn += _offsetSize
	binary.BigEndian.PutInt32(i.buf[i.bn:], size)
	i.bn += _sizeSize
	err = i.flush(false)
	return
}

// flush the in-memory data flush to disk.
func (i *Indexer) flush(force bool) (err error) {
	var (
		fd     uintptr
		offset int64
		size   int64
	)
	if i.write++; !force && i.write < i.conf.Index.SyncWrite {
		return
	}
	if _, err = i.f.Write(i.buf[:i.bn]); err != nil {
		i.LastErr = err
		log.Errorf("index: %s Write() error(%v)", i.File, err)
		return
	}
	i.Offset += int64(i.bn)
	i.bn = 0
	i.write = 0
	offset = i.syncOffset
	size = i.Offset - i.syncOffset
	fd = i.f.Fd()
	if i.conf.Index.Syncfilerange {
		if err = myos.Syncfilerange(fd, offset, size, myos.SYNC_FILE_RANGE_WRITE); err != nil {
			i.LastErr = err
			log.Errorf("index: %s Syncfilerange() error(%v)", i.File, err)
			return
		}
	} else {
		if err = myos.Fdatasync(fd); err != nil {
			i.LastErr = err
			log.Errorf("index: %s Fdatasync() error(%v)", i.File, err)
			return
		}
	}
	if err = myos.Fadvise(fd, offset, size, myos.POSIX_FADV_DONTNEED); err == nil {
		i.syncOffset = i.Offset
	} else {
		log.Errorf("index: %s Fadvise() error(%v)", i.File, err)
		i.LastErr = err
	}
	return
}

// Flush flush writer buffer.
func (i *Indexer) Flush() (err error) {
	if i.LastErr != nil {
		return i.LastErr
	}
	err = i.flush(true)
	return
}

// mergeRing get index data from ring then write to disk.
func (i *Indexer) mergeRing() (err error) {
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

// merge merge from ring index data, then write to disk.
func (i *Indexer) merge() {
	var (
		err error
		sig int
	)
	log.Infof("index: %s write job start", i.File)
	for {
		select {
		case sig = <-i.signal:
		case <-time.After(i.conf.Index.MergeDelay.Duration):
			sig = _ready
		}
		if sig != _ready {
			break
		}
		if err = i.mergeRing(); err != nil {
			break
		}
		if err = i.flush(false); err != nil {
			break
		}
	}
	i.mergeRing()
	i.flush(true)
	i.wg.Done()
	log.Warningf("index: %s write job exit", i.File)
	return
}

// Scan scan a indexer file.
func (i *Indexer) Scan(r *os.File, fn func(*Index) error) (err error) {
	var (
		data []byte
		fi   os.FileInfo
		fd   = r.Fd()
		ix   = &Index{}
		rd   = bufio.NewReaderSize(r, i.conf.Index.BufferSize)
	)
	log.Infof("scan index: %s", i.File)
	// advise sequential read
	if fi, err = r.Stat(); err != nil {
		log.Errorf("index: %s Stat() error(%v)", i.File)
		return
	}
	if err = myos.Fadvise(fd, 0, fi.Size(), myos.POSIX_FADV_SEQUENTIAL); err != nil {
		log.Errorf("index: %s Fadvise() error(%v)", i.File)
		return
	}
	if _, err = r.Seek(0, os.SEEK_SET); err != nil {
		log.Errorf("index: %s Seek() error(%v)", i.File, err)
		return
	}
	for {
		if data, err = rd.Peek(_indexSize); err != nil {
			break
		}
		if err = ix.parse(data); err != nil {
			break
		}
		if ix.Size > int32(i.conf.BlockMaxSize) {
			log.Errorf("scan index: %s error(%v)", ix, errors.ErrIndexSize)
			err = errors.ErrIndexSize
			break
		}
		if _, err = rd.Discard(_indexSize); err != nil {
			break
		}
		if log.V(1) {
			log.Info(ix.String())
		}
		if err = fn(ix); err != nil {
			break
		}
	}
	if err == io.EOF {
		// advise no need page cache
		if err = myos.Fadvise(fd, 0, fi.Size(), myos.POSIX_FADV_DONTNEED); err == nil {
			err = nil
			log.Infof("scan index: %s [ok]", i.File)
			return
		} else {
			log.Errorf("index: %s Fadvise() error(%v)", i.File)
		}
	}
	log.Infof("scan index: %s error(%v) [failed]", i.File, err)
	return
}

// Recovery recovery needle cache meta data in memory, index file  will stop
// at the right parse data offset.
func (i *Indexer) Recovery(fn func(*Index) error) (err error) {
	if i.Scan(i.f, func(ix *Index) (err1 error) {
		if err1 = fn(ix); err1 == nil {
			i.Offset += int64(_indexSize)
		}
		return
	}); err != nil {
		return
	}
	// reset b.w offset, discard left space which can't parse to a needle
	if _, err = i.f.Seek(i.Offset, os.SEEK_SET); err != nil {
		log.Errorf("index: %s Seek() error(%v)", i.File, err)
	}
	return
}

// Open open the closed indexer, must called after NewIndexer.
func (i *Indexer) Open() (err error) {
	if !i.closed {
		return
	}
	if i.f, err = os.OpenFile(i.File, os.O_RDWR|myos.O_NOATIME, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", i.File, err)
		return
	}
	// reset buf
	i.bn = 0
	i.closed = false
	i.LastErr = nil
	i.wg.Add(1)
	go i.merge()
	return
}

// Close close the indexer file.
func (i *Indexer) Close() {
	var err error
	if i.signal != nil {
		i.signal <- _finish
		i.wg.Wait()
	}
	if i.f != nil {
		if err = i.flush(true); err != nil {
			log.Errorf("index: %s Flush() error(%v)", i.File, err)
		}
		if err = i.f.Sync(); err != nil {
			log.Errorf("index: %s Sync() error(%v)", i.File, err)
		}
		if err = i.f.Close(); err != nil {
			log.Errorf("index: %s Close() error(%v)", i.File, err)
		}
	}
	i.closed = true
	i.LastErr = errors.ErrIndexClosed
	return
}

// Destroy destroy the indexer.
func (i *Indexer) Destroy() {
	if !i.closed {
		i.Close()
	}
	os.Remove(i.File)
}
