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
	lock           sync.RWMutex
	Id             int32       `json:"id"`
	Stats          *Stats      `json:"stats"`
	Block          *SuperBlock `json:"block"`
	Indexer        *Indexer    `json:"index"`
	needles        map[int64]int64
	signal         chan uint32
	bp             *sync.Pool // buffer pool
	np             *sync.Pool // needle struct pool
	Command        int        `json:"-"` // flag used in store
	Compress       bool       `json:"-"`
	compressOffset int64
	compressKeys   []int64
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
	v.compressKeys = []int64{}
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
	if offset, err = v.Indexer.Recovery(v.needles); err != nil {
		return
	}
	// recovery from super block
	err = v.Block.Recovery(v.needles, v.Indexer, BlockOffset(offset))
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
func (v *Volume) needle() (n *Needle) {
	var i interface{}
	if i = v.np.Get(); i != nil {
		n = i.(*Needle)
		return
	}
	return new(Needle)
}

// FreeNeedle free the needle to pool.
func (v *Volume) freeNeedle(n *Needle) {
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
	n = v.needle()
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		goto free
	}
	if err = n.ParseData(buf[NeedleHeaderSize:size]); err != nil {
		goto free
	}
	if log.V(1) {
		log.Infof("%v\n", buf[:size])
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
free:
	v.freeNeedle(n)
	return
}

// Add add a new needle, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Add(key, cookie int64, data []byte) (err error) {
	var (
		ok              bool
		nc              int64
		size, osize     int32
		offset, ooffset uint32
	)
	v.lock.Lock()
	nc, ok = v.needles[key]
	// add needle
	if offset, size, err = v.Block.Add(key, cookie, data); err != nil {
		v.lock.Unlock()
		return
	}
	if err = v.Indexer.Add(key, offset, size); err != nil {
		v.lock.Unlock()
		return
	}
	v.needles[key] = NeedleCache(offset, size)
	v.lock.Unlock()
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, size)
	}
	if ok {
		ooffset, osize = NeedleCacheValue(nc)
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		err = v.asyncDel(ooffset)
	}
	atomic.AddUint64(&v.Stats.TotalAddProcessed, 1)
	return
}

// Write add a new needle, if key exists append to super block, then update
// needle cache offset to new offset, Write is used for multi add needles.
func (v *Volume) Write(key, cookie int64, data []byte) (err error) {
	var (
		ok              bool
		nc              int64
		size, osize     int32
		offset, ooffset uint32
	)
	nc, ok = v.needles[key]
	if offset, size, err = v.Block.Write(key, cookie, data); err != nil {
		return
	}
	if err = v.Indexer.Add(key, offset, size); err != nil {
		return
	}
	v.needles[key] = NeedleCache(offset, size)
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, size)
	}
	if ok {
		ooffset, osize = NeedleCacheValue(nc)
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		err = v.asyncDel(ooffset)
	}
	atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
	return
}

// Flush flush block&indexer buffer to disk, this is used for multi add needles.
func (v *Volume) Flush() (err error) {
	if err = v.Block.Flush(); err != nil {
		return
	}
	atomic.AddUint64(&v.Stats.TotalFlushProcessed, 1)
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
	atomic.AddUint64(&v.Stats.TotalDelProcessed, 1)
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
		// when in compress, must save all del operations.
		if v.Compress {
			v.compressKeys = append(v.compressKeys, key)
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
			if err = v.Block.Del(offset); err != nil {
				break
			}
		}
		offsets = offsets[:0]
	}
	return
}

// Compress copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) StartCompress(nv *Volume) (err error) {
	v.lock.Lock()
	if v.Compress {
		err = ErrVolumeInCompress
	} else {
		v.Compress = true
	}
	v.lock.Unlock()
	if err == nil {
		v.compressOffset, err = v.Block.Compress(v.compressOffset, nv)
		atomic.AddUint64(&v.Stats.TotalCompressProcessed, 1)
	}
	return
}

// StopCompress try append left block space and deleted needles when
// compressing, then reset compress flag, offset and compressKeys.
// if nv is nil, only reset compress status.
func (v *Volume) StopCompress(nv *Volume) (err error) {
	var key int64
	v.lock.Lock()
	if nv != nil {
		if v.compressOffset, err = v.Block.Compress(v.compressOffset, nv); err != nil {
			goto failed
		}
		for _, key = range v.compressKeys {
			if err = nv.Del(key); err != nil {
				goto failed
			}
		}
	}
failed:
	v.Compress = false
	v.compressOffset = 0
	v.compressKeys = v.compressKeys[:0]
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
