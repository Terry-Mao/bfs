package main

import (
	"github.com/Terry-Mao/bfs/store/block"
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/index"
	"github.com/Terry-Mao/bfs/store/needle"
	"github.com/Terry-Mao/bfs/store/stat"
	log "github.com/golang/glog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// signal command
	volumeFinish  = 0
	volumeReady   = 1
	VolumeEmptyId = -1
)

// Uint32Slice deleted offset sort.
type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// An store server contains many logic Volume, volume is superblock container.
type Volume struct {
	wg   sync.WaitGroup
	lock sync.RWMutex

	Id      int32             `json:"id"`
	Stats   *stat.Stats       `json:"stats"`
	Block   *block.SuperBlock `json:"block"`
	Indexer *index.Indexer    `json:"index"`
	// data
	conf    *Config
	needles map[int64]int64
	bp      []*sync.Pool // buffer pool
	np      *sync.Pool   // needle pool
	ch      chan uint32
	// compact
	Compact       bool   `json:"compact"`
	CompactOffset uint32 `json:"compact_offset"`
	CompactTime   int64  `json:"compact_time"`
	compactKeys   []int64
	// status
	closed bool
	// used in store, control the volume update
	Command int `json: "-"`
}

// NewVolume new a volume and init it.
func NewVolume(id int32, bfile, ifile string, c *Config) (v *Volume, err error) {
	var i int
	v = &Volume{}
	v.Id = id
	v.conf = c
	v.closed = false
	v.Stats = &stat.Stats{}
	v.bp = make([]*sync.Pool, c.BatchMaxNum)
	v.bp[0] = nil
	for i = 1; i < c.BatchMaxNum; i++ {
		v.bp[i] = &sync.Pool{}
	}
	v.np = &sync.Pool{}
	if v.Block, err = block.NewSuperBlock(bfile, c.NeedleMaxSize*c.BatchMaxNum); err != nil {
		return
	}
	if v.Indexer, err = index.NewIndexer(ifile, c.IndexSigTime,
		c.IndexRingBuffer, c.IndexSigCnt, c.IndexBufferio); err != nil {
		v.Block.Close()
		return
	}
	v.needles = make(map[int64]int64, c.VolumeNeedleCache)
	v.ch = make(chan uint32, c.VolumeDelChan)
	v.compactKeys = []int64{}
	if err = v.init(); err != nil {
		v.Indexer.Close()
		v.Block.Close()
	} else {
		v.wg.Add(1)
		go v.del()
	}
	return
}

// init recovery super block from index or super block.
func (v *Volume) init() (err error) {
	var offset uint32
	// recovery from index
	if err = v.Indexer.Recovery(func(ix *index.Index) (err1 error) {
		v.needles[ix.Key] = needle.NewCache(ix.Offset, ix.Size)
		offset = ix.Offset + needle.NeedleOffset(int64(ix.Size))
		if ix.Size > int32(v.conf.NeedleMaxSize) || ix.Size < 0 {
			err1 = errors.ErrIndexSize
		}
		return
	}); err != nil {
		return
	}
	// recovery from super block
	err = v.Block.Recovery(offset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if n.TotalSize > int32(v.conf.NeedleMaxSize) || n.TotalSize < 0 {
			err1 = errors.ErrNeedleSize
			return
		}
		if n.Flag == needle.FlagOK {
			if err1 = v.Indexer.Write(n.Key, so, n.TotalSize); err1 != nil {
				return
			}
		} else {
			so = needle.CacheDelOffset
		}
		v.needles[n.Key] = needle.NewCache(so, n.TotalSize)
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
func (v *Volume) Needle() (n *needle.Needle) {
	var i interface{}
	if i = v.np.Get(); i != nil {
		n = i.(*needle.Needle)
		return
	}
	return new(needle.Needle)
}

// FreeNeedle free the needle to pool.
func (v *Volume) FreeNeedle(n *needle.Needle) {
	v.np.Put(n)
}

// Buffer get a buffer from sync.Pool.
func (v *Volume) Buffer(n int) (d []byte) {
	var (
		di interface{}
	)
	if di = v.bp[n].Get(); di != nil {
		d = di.([]byte)
		return
	}
	d = make([]byte, n*v.conf.NeedleMaxSize)
	return
}

// FreeBuffer free the buffer to pool.
func (v *Volume) FreeBuffer(n int, d []byte) {
	v.bp[n].Put(d)
}

// IsClosed reports whether the volume is closed.
func (v *Volume) IsClosed() bool {
	return v.closed
}

// ValidNeedle check the needle valid or not.
func (v *Volume) ValidNeedle(n *needle.Needle) (err error) {
	if n.TotalSize > int32(v.conf.NeedleMaxSize) {
		err = errors.ErrNeedleTooLarge
	}
	return
}

// Get get a needle by key and cookie.
func (v *Volume) Get(key int64, cookie int32, buf []byte) (data []byte, err error) {
	var (
		now    = time.Now().UnixNano()
		ok     bool
		nc     int64
		size   int32
		offset uint32
		n      *needle.Needle
	)
	v.lock.RLock()
	if !v.closed {
		nc, ok = v.needles[key]
	} else {
		err = errors.ErrVolumeClosed
	}
	v.lock.RUnlock()
	if err != nil {
		return
	}
	if !ok {
		err = errors.ErrNoNeedle
		return
	}
	// check len(buf) ?
	if offset, size = needle.Cache(nc); offset == needle.CacheDelOffset {
		err = errors.ErrNeedleDeleted
		return
	}
	if log.V(1) {
		log.Infof("get needle key: %d, cookie: %d, offset: %d, size: %d", key,
			cookie, offset, size)
	}
	// WARN pread syscall is atomic, so don't need lock
	if err = v.Block.Get(offset, buf[:size]); err != nil {
		return
	}
	n = v.Needle()
	if err = n.ParseHeader(buf[:needle.HeaderSize]); err != nil {
		goto free
	}
	if n.TotalSize != size {
		err = errors.ErrNeedleSize
		goto free
	}
	if err = n.ParseData(buf[needle.HeaderSize:size]); err != nil {
		goto free
	}
	if log.V(2) {
		log.Infof("%v\n", buf[:size])
	}
	if log.V(1) {
		log.Infof("%v\n", n)
	}
	if n.Key != key {
		err = errors.ErrNeedleKey
		goto free
	}
	if n.Cookie != cookie {
		err = errors.ErrNeedleCookie
		goto free
	}
	// needles map may be out-dated, recheck
	if n.Flag == needle.FlagDel {
		v.lock.Lock()
		v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
		v.lock.Unlock()
		err = errors.ErrNeedleDeleted
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
func (v *Volume) Add(n *needle.Needle) (err error) {
	var (
		now             = time.Now().UnixNano()
		ok              bool
		nc              int64
		offset, ooffset uint32
	)
	if n.TotalSize > int32(v.conf.NeedleMaxSize) {
		err = errors.ErrNeedleTooLarge
		return
	}
	v.lock.Lock()
	if !v.closed {
		offset = v.Block.Offset
		if err = v.Block.Add(n); err == nil {
			if err = v.Indexer.Add(n.Key, offset, n.TotalSize); err == nil {
				nc, ok = v.needles[n.Key]
				v.needles[n.Key] = needle.NewCache(offset, n.TotalSize)
			}
		}
	} else {
		err = errors.ErrVolumeClosed
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, n.TotalSize)
	}
	if ok {
		ooffset, _ = needle.Cache(nc)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", n.Key,
			ooffset, offset)
		if err = v.asyncDel(ooffset); err != nil {
			return
		}
	}
	atomic.AddUint64(&v.Stats.TotalAddProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
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
func (v *Volume) Write(n *needle.Needle) (err error) {
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
			v.needles[n.Key] = needle.NewCache(offset, n.TotalSize)
		}
	}
	if err != nil {
		return
	}
	if log.V(1) {
		log.Infof("add needle, offset: %d, size: %d", offset, n.TotalSize)
	}
	if ok {
		ooffset, _ = needle.Cache(nc)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", n.Key,
			ooffset, offset)
		err = v.asyncDel(ooffset)
	}
	atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
	atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
	return
}

// Flush flush block&indexer buffer to disk, this is used for multi add needles.
func (v *Volume) Flush() (err error) {
	var now = time.Now().UnixNano()
	if err = v.Block.Flush(); err != nil {
		return
	}
	atomic.AddUint64(&v.Stats.TotalFlushProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalFlushDelay, uint64(time.Now().UnixNano()-now))
	return
}

// asyncDel signal the godel goroutine aync merge all offsets and del.
func (v *Volume) asyncDel(offset uint32) (err error) {
	// async update super block flag
	select {
	case v.ch <- offset:
	default:
		log.Errorf("volume: %d send signal failed", v.Id)
		err = errors.ErrVolumeDel
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
	if !v.closed {
		nc, ok = v.needles[key]
		if ok {
			offset, size = needle.Cache(nc)
			v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
			// when in compact, must save all del operations.
			if v.Compact {
				v.compactKeys = append(v.compactKeys, key)
			}
		}
	} else {
		err = errors.ErrVolumeClosed
	}
	v.lock.Unlock()
	if err == nil {
		if ok {
			err = v.asyncDel(offset)
		} else {
			err = errors.ErrNoNeedle
		}
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
		case offset = <-v.ch:
			if offset != volumeFinish {
				if offsets = append(offsets, offset); len(offsets) < v.conf.VolumeSigCnt {
					continue
				}
			}
		case <-time.After(v.conf.VolumeSigTime):
			offset = volumeReady
		}
		if len(offsets) > 0 {
			// sort let the disk seqence write
			sort.Sort(Uint32Slice(offsets))
			for _, offset = range offsets {
				now = time.Now().UnixNano()
				if err = v.Block.Del(offset); err != nil {
					break
				}
				atomic.AddUint64(&v.Stats.TotalDelProcessed, 1)
				atomic.AddUint64(&v.Stats.TotalWriteBytes, 1)
				atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
			}
			offsets = offsets[:0]
		}
		// signal exit
		if offset == volumeFinish {
			break
		}
	}
	log.Warningf("volume: %d del job exit", v.Id)
	v.wg.Done()
	return
}

// compact compact v to new v.
func (v *Volume) compact(nv *Volume) (err error) {
	err = v.Block.Compact(v.CompactOffset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if n.TotalSize > int32(v.conf.NeedleMaxSize) || n.TotalSize < 0 {
			err1 = errors.ErrNeedleSize
			return
		}
		if n.Flag != needle.FlagDel {
			if err1 = nv.Write(n); err1 != nil {
				return
			}
		}
		v.CompactOffset = eo
		return
	})
	return
}

// Compact copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) StartCompact(nv *Volume) (err error) {
	v.lock.Lock()
	if !v.closed {
		if v.Compact {
			err = errors.ErrVolumeInCompact
		} else {
			v.Compact = true
		}
	} else {
		err = errors.ErrVolumeClosed
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	v.CompactTime = time.Now().UnixNano()
	if err = v.compact(nv); err != nil {
		return
	}
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
	if v.closed {
		err = errors.ErrVolumeClosed
		goto failed
	}
	if nv != nil {
		if err = v.compact(nv); err != nil {
			goto failed
		}
		if err = nv.Flush(); err != nil {
			goto failed
		}
		for _, key = range v.compactKeys {
			if err = nv.Del(key); err != nil {
				goto failed
			}
		}
		atomic.AddUint64(&v.Stats.TotalCompactDelay, uint64(time.Now().UnixNano()-now))
	}
failed:
	v.Compact = false
	v.CompactOffset = 0
	v.CompactTime = 0
	v.compactKeys = v.compactKeys[:0]
	v.lock.Unlock()
	return
}

// Open open the closed volume, must called after NewVolume.
func (v *Volume) Open() (err error) {
	v.lock.Lock()
	if v.closed {
		v.ch = make(chan uint32, v.conf.VolumeDelChan)
		if err = v.Block.Open(); err == nil {
			if err = v.Indexer.Open(); err == nil {
				if err = v.init(); err == nil {
					v.closed = false
					v.wg.Add(1)
					go v.del()
				}
			}
		}
	}
	v.lock.Unlock()
	if err != nil {
		if v.Block != nil {
			v.Block.Close()
		}
		if v.Indexer != nil {
			v.Indexer.Close()
		}
	}
	return
}

// Close close the volume.
func (v *Volume) Close() {
	v.lock.Lock()
	if !v.closed {
		close(v.ch)
		v.wg.Wait()
		v.Block.Close()
		v.Indexer.Close()
		v.closed = true
	}
	v.lock.Unlock()
	return
}
