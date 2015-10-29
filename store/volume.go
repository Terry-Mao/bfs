package main

import (
	log "github.com/golang/glog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// signal command
	volumeFinish   = 0
	volumeReady    = 1
	volumeDelChNum = 10240
	// del
	volumeDelMax = 50
	// needle cache
	needleCacheOffsetBit = 32
	// del offset
	NeedleCacheDelOffset = uint32(0)
	VolumeEmptyId        = -1
)

var (
	// del
	volumeDelTime = 1 * time.Minute
)

// NeedleCache needle meta data in memory.
// high 32bit = Offset
// low 32 bit = Size
// NeedleCache new a needle cache.
func NeedleCache(offset uint32, size int32) int64 {
	return int64(offset)<<needleCacheOffsetBit + int64(size)
}

// NeedleCacheValue get needle cache data.
func NeedleCacheValue(n int64) (offset uint32, size int32) {
	offset, size = uint32(n>>needleCacheOffsetBit), int32(n)
	return
}

// Uint32Slice deleted offset sort.
type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// An store server contains many logic Volume, volume is superblock container.
type Volume struct {
	lock          sync.RWMutex
	Id            int32       `json:"id"`
	Stats         *Stats      `json:"stats"`
	Block         *SuperBlock `json:"block"`
	Indexer       *Indexer    `json:"index"`
	needles       map[int64]int64
	signal        chan uint32
	bp            *sync.Pool // buffer pool
	np            *sync.Pool // needle struct pool
	Command       int        `json:"-"` // flag used in store
	Compact       bool       `json:"-"`
	compactOffset int64
	compactTime   int64
	compactKeys   []int64
}

// NewVolume new a volume and init it.
func NewVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	v = &Volume{}
	v.Id = id
	v.Stats = &Stats{}
	v.bp = &sync.Pool{}
	v.np = &sync.Pool{}
	if v.Block, err = NewSuperBlock(bfile); err != nil {
		log.Errorf("init super block: \"%s\" error(%v)", bfile, err)
		return
	}
	if v.Indexer, err = NewIndexer(ifile, 10240*5); err != nil {
		log.Errorf("init indexer: %s error(%v)", ifile, err)
		goto failed
	}
	v.needles = make(map[int64]int64)
	if err = v.init(); err != nil {
		goto failed
	}
	v.signal = make(chan uint32, volumeDelChNum)
	v.compactKeys = []int64{}
	go v.del()
	return
failed:
	v.Block.Close()
	if v.Indexer != nil {
		v.Indexer.Close()
	}
	return
}

// Open open the closed volume, must called after NewVolume.
func (v *Volume) Open() (err error) {
	v.signal = make(chan uint32, volumeDelChNum)
	if err = v.Block.Open(); err != nil {
		return
	}
	if err = v.Indexer.Open(); err != nil {
		goto failed
	}
	if err = v.init(); err != nil {
		goto failed
	}
	go v.del()
	return
failed:
	v.Block.Close()
	if v.Indexer != nil {
		v.Indexer.Close()
	}
	return
}

// init recovery super block from index or super block.
func (v *Volume) init() (err error) {
	var offset uint32
	// recovery from index
	if offset, err = v.Indexer.Recovery(
		func(ix *Index) error {
			v.needles[ix.Key] = NeedleCache(ix.Offset, ix.Size)
			return nil
		}); err != nil {
		return
	}
	// recovery from super block
	err = v.Block.Recovery(offset, func(n *Needle, bo uint32) (err1 error) {
		var (
			co uint32
		)
		if n.Flag == NeedleStatusOK {
			if err1 = v.Indexer.Write(n.Key, bo, n.TotalSize); err1 != nil {
				return
			}
			co = bo
		} else {
			co = NeedleCacheDelOffset
		}
		v.needles[n.Key] = NeedleCache(co, n.TotalSize)
		return
	})
	// flush index
	err = v.Indexer.Flush()
	return
}

// Lock lock the volume, used in multi write needles.
func (v *Volume) Lock() {
	v.lock.Lock()
}

// Unlock lock the volume, used in multi write needles.
func (v *Volume) Unlock() {
	v.lock.Unlock()
}

// Needle get a needle from sync.Pool.
func (v *Volume) Needle() (n *Needle) {
	var i interface{}
	if i = v.np.Get(); i != nil {
		n = i.(*Needle)
		return
	}
	return new(Needle)
}

// FreeNeedle free the needle to pool.
func (v *Volume) FreeNeedle(n *Needle) {
	v.np.Put(n)
}

// Buffer get a buffer from sync.Pool.
func (v *Volume) Buffer() (d []byte) {
	var i interface{}
	if i = v.bp.Get(); i != nil {
		d = i.([]byte)
		return
	}
	return make([]byte, NeedleMaxSize)
}

// FreeBuffer free the buffer to pool.
func (v *Volume) FreeBuffer(d []byte) {
	v.bp.Put(d)
}

// Get get a needle by key and cookie.
func (v *Volume) Get(key, cookie int64, buf []byte) (data []byte, err error) {
	var (
		now    = time.Now().UnixNano()
		ok     bool
		nc     int64
		size   int32
		offset uint32
		n      *Needle
	)
	v.lock.RLock()
	nc, ok = v.needles[key]
	v.lock.RUnlock()
	if !ok {
		err = ErrNoNeedle
		return
	}
	if offset, size = NeedleCacheValue(nc); offset == NeedleCacheDelOffset {
		err = ErrNeedleDeleted
		return
	}
	if log.V(1) {
		log.Infof("get needle, key: %d, cookie: %d, offset: %d, size: %d",
			key, cookie, offset, size)
	}
	// WARN pread syscall is atomic, so don't need lock
	if err = v.Block.Get(offset, buf[:size]); err != nil {
		return
	}
	n = v.Needle()
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		goto free
	}
	if err = n.ParseData(buf[NeedleHeaderSize:size]); err != nil {
		goto free
	}
	if log.V(2) {
		log.Infof("%v\n", buf[:size])
	}
	if log.V(1) {
		log.Infof("%v\n", n)
	}
	if n.Key != key {
		err = ErrNeedleKey
		goto free
	}
	if n.Cookie != cookie {
		err = ErrNeedleCookie
		goto free
	}
	// needles map may be out-dated, recheck
	if n.Flag == NeedleStatusDel {
		v.lock.Lock()
		v.needles[key] = NeedleCache(NeedleCacheDelOffset, size)
		v.lock.Unlock()
		err = ErrNeedleDeleted
		goto free
	}
	data = n.Data
	atomic.AddUint64(&v.Stats.TotalGetProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalReadBytes, uint64(size))
	atomic.AddUint64(&v.Stats.TotalGetDelay, uint64(time.Now().UnixNano()-now))
free:
	v.FreeNeedle(n)
	return
}

// Add add a new needle, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Add(key, cookie int64, data []byte) (err error) {
	var (
		now             = time.Now().UnixNano()
		ok              bool
		nc              int64
		size            int32
		offset, ooffset uint32
		n               = v.Needle()
	)
	if err = n.Parse(key, cookie, data); err != nil {
		return
	}
	size = n.TotalSize
	v.lock.Lock()
	offset = v.Block.Offset
	if err = v.Block.Add(n); err == nil {
		if err = v.Indexer.Add(key, offset, size); err == nil {
			nc, ok = v.needles[key]
			v.needles[key] = NeedleCache(offset, size)
		}
	}
	v.lock.Unlock()
	v.FreeNeedle(n)
	if err != nil {
		return
	}
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, size)
	}
	if ok {
		ooffset, _ = NeedleCacheValue(nc)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", key,
			ooffset, offset)
		err = v.asyncDel(ooffset)
	}
	atomic.AddUint64(&v.Stats.TotalAddProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(size))
	atomic.AddUint64(&v.Stats.TotalAddDelay, uint64(time.Now().UnixNano()-now))
	return
}

// Write add a new needle, if key exists append to super block, then update
// needle cache offset to new offset, Write is used for multi add needles.
// Get Needle
// Lock
// for {
//   Write
// }
// Unlock
// Free Needle
func (v *Volume) Write(n *Needle) (err error) {
	var (
		ok              bool
		nc              int64
		offset, ooffset uint32
		now             = time.Now().UnixNano()
	)
	offset = v.Block.Offset
	if err = v.Block.Write(n); err == nil {
		if err = v.Indexer.Add(n.Key, offset, n.TotalSize); err == nil {
			nc, ok = v.needles[n.Key]
			v.needles[n.Key] = NeedleCache(offset, n.TotalSize)
		}
	}
	if err != nil {
		return
	}
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, n.TotalSize)
	}
	if ok {
		ooffset, _ = NeedleCacheValue(nc)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", n.Key,
			ooffset, offset)
		err = v.asyncDel(ooffset)
	}
	atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
	atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-
		now))
	return
}

// Flush flush block&indexer buffer to disk, this is used for multi add needles.
func (v *Volume) Flush() (err error) {
	var now = time.Now().UnixNano()
	if err = v.Block.Flush(); err != nil {
		return
	}
	atomic.AddUint64(&v.Stats.TotalFlushProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalFlushDelay, uint64(time.Now().UnixNano()-
		now))
	return
}

// asyncDel signal the godel goroutine aync merge all offsets and del.
func (v *Volume) asyncDel(offset uint32) (err error) {
	// async update super block flag
	select {
	case v.signal <- offset:
	default:
		log.Errorf("volume: %d send signal failed", v.Id)
		err = ErrVolumeDel
		return
	}
	return
}

// Del logical delete a needle, update disk needle flag and memory needle
// cache offset to zero.
func (v *Volume) Del(key int64) (err error) {
	var (
		ok     bool
		nc     int64
		size   int32
		offset uint32
	)
	v.lock.Lock()
	nc, ok = v.needles[key]
	if ok {
		offset, size = NeedleCacheValue(nc)
		v.needles[key] = NeedleCache(NeedleCacheDelOffset, size)
		// when in compact, must save all del operations.
		if v.Compact {
			v.compactKeys = append(v.compactKeys, key)
		}
	}
	v.lock.Unlock()
	if ok {
		err = v.asyncDel(offset)
	} else {
		err = ErrNoNeedle
	}
	return
}

// del merge from volume signal, then update block needles flag.
func (v *Volume) del() {
	var (
		err     error
		now     int64
		offset  uint32
		offsets []uint32
	)
	for {
		select {
		case offset = <-v.signal:
			if offset == volumeFinish {
				return
			}
			// merge
			if offsets = append(offsets, offset); len(offsets) < volumeDelMax {
				continue
			}
		case <-time.After(volumeDelTime):
		}
		if len(offsets) == 0 {
			continue
		}
		// sort let the disk seqence write
		sort.Sort(Uint32Slice(offsets))
		for _, offset = range offsets {
			now = time.Now().UnixNano()
			if err = v.Block.Del(offset); err != nil {
				break
			}
			atomic.AddUint64(&v.Stats.TotalDelProcessed, 1)
			atomic.AddUint64(&v.Stats.TotalWriteBytes, 1)
			atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(
				time.Now().UnixNano()-now))
		}
		offsets = offsets[:0]
	}
	return
}

// Compact copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) StartCompact(nv *Volume) (err error) {
	v.lock.Lock()
	if v.Compact {
		err = ErrVolumeInCompact
	} else {
		v.Compact = true
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	v.compactTime = time.Now().UnixNano()
	v.compactOffset, err = v.Block.Compact(v.compactOffset,
		func(n *Needle) (err1 error) {
			err1 = nv.Write(n)
			return
		})
	if err = nv.Flush(); err != nil {
		return
	}
	atomic.AddUint64(&v.Stats.TotalCompactProcessed, 1)
	return
}

// StopCompact try append left block space and deleted needles when
// compacting, then reset compact flag, offset and compactKeys.
// if nv is nil, only reset compact status.
func (v *Volume) StopCompact(nv *Volume) (err error) {
	var (
		now = time.Now().UnixNano()
		key int64
	)
	v.lock.Lock()
	if nv != nil {
		v.compactOffset, err = v.Block.Compact(v.compactOffset,
			func(n *Needle) (err1 error) {
				err1 = nv.Write(n)
				return
			})
		if err = nv.Flush(); err != nil {
			return
		}
		for _, key = range v.compactKeys {
			if err = nv.Del(key); err != nil {
				goto failed
			}
		}
		atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(
			time.Now().UnixNano()-now))
	}
failed:
	v.Compact = false
	v.compactOffset = 0
	v.compactTime = 0
	v.compactKeys = v.compactKeys[:0]
	v.lock.Unlock()
	return
}

// Close close the volume.
func (v *Volume) Close() {
	v.lock.Lock()
	v.Block.Close()
	v.Indexer.Close()
	close(v.signal)
	v.lock.Unlock()
	return
}
