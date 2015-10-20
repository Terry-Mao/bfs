package main

import (
	log "github.com/golang/glog"
	"sort"
	"sync"
	"time"
)

const (
	// signal command
	volumeFinish = 0
	volumeReady  = 1
	// 32GB, offset aligned 8 bytes, 4GB * 8
	VolumeMaxSize  = 4 * 1024 * 1024 * 1024 * 8
	volumeDelChNum = 10240
	// del
	volumeDelMax = 50
)

var (
	// del
	volumeDelTime = 1 * time.Minute
)

// IntSlice attaches the methods of Interface to []int, sorting in increasing order.
type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// An store server contains many logic Volume, volume is superblock container.
type Volume struct {
	Id      int32
	lock    sync.Mutex
	block   *SuperBlock
	indexer *Indexer
	needles map[int64]NeedleCache
	signal  chan uint32
	// TODO status
	ReadOnly bool

	// flag used in store control
	Store int
}

// NewVolume new a volume and init it.
func NewVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	v = &Volume{}
	v.Id = id
	if v.block, err = NewSuperBlock(bfile); err != nil {
		log.Errorf("init super block: \"%s\" error(%v)", bfile, err)
		return
	}
	if v.indexer, err = NewIndexer(ifile, 10240, 4*1024); err != nil {
		log.Errorf("init indexer: %s error(%v)", ifile, err)
		return
	}
	v.needles = make(map[int64]NeedleCache)
	if err = v.init(); err != nil {
		log.Errorf("volume: %d init error(%v)", id, err)
		return
	}
	v.signal = make(chan uint32, volumeDelChNum)
	go v.del()
	return
}

// init recovery super block from index or super block.
func (v *Volume) init() (err error) {
	var offset uint32
	// recovery from index
	log.Infof("try recovery from index: %s", v.indexer.File)
	if offset, err = v.indexer.Recovery(v.needles); err != nil {
		log.Info("recovery from index: %s error(%v)", v.indexer.File, err)
		return
	}
	log.Infof("finish recovery from index: %s", v.indexer.File)
	log.V(1).Infof("recovery offset: %d\n", offset)
	log.Infof("try recovery from indexsuper block %s", v.block.File)
	// recovery from super block
	if err = v.block.Recovery(v.needles, v.indexer, BlockOffset(offset)); err != nil {
		log.Errorf("recovery from super block: %s error(%v)", v.block.File, err)
		return
	}
	log.Infof("finish recovery from block: %s", v.indexer.File)
	return
}

// Get get a needle by key.
func (v *Volume) Get(key, cookie int64, buf []byte) (data []byte, err error) {
	var (
		ok          bool
		size        int32
		offset      uint32
		needleCache NeedleCache
		needle      = &Needle{}
	)
	// get a needle
	v.lock.Lock()
	needleCache, ok = v.needles[key]
	v.lock.Unlock()
	if !ok {
		err = ErrNoNeedle
		return
	}
	offset, size = needleCache.Value()
	log.V(1).Infof("get needle, key: %d, cookie: %d, offset: %d, size: %d", key, cookie, BlockOffset(offset), size)
	if offset == NeedleCacheDelOffset {
		err = ErrNeedleDeleted
		return
	}
	// WARN atomic read superblock, pread syscall is atomic
	if err = v.block.Get(offset, buf[:size]); err != nil {
		return
	}
	// parse needle
	// TODO repair
	if err = ParseNeedleHeader(buf[:NeedleHeaderSize], needle); err != nil {
		return
	}
	if err = ParseNeedleData(buf[NeedleHeaderSize:], needle); err != nil {
		return
	}
	log.Infof("%v\n", needle)
	// check needle
	if needle.Key != key {
		err = ErrNeedleKey
		return
	}
	if needle.Cookie != cookie {
		err = ErrNeedleCookie
		return
	}
	// if delete
	if needle.Flag == NeedleStatusDel {
		v.lock.Lock()
		v.needles[key] = NewNeedleCache(NeedleCacheDelOffset, size)
		v.lock.Unlock()
		err = ErrNeedleDeleted
		return
	}
	data = needle.Data
	return
}

// Add add a new needle, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Add(key, cookie int64, data []byte) (err error) {
	var (
		ok              bool
		size, osize     int32
		offset, ooffset uint32
		needleCache     NeedleCache
	)
	v.lock.Lock()
	needleCache, ok = v.needles[key]
	// add needle
	if offset, size, err = v.block.Add(key, cookie, data); err != nil {
		v.lock.Unlock()
		return
	}
	log.V(1).Infof("add needle, offset: %d, size: %d", offset, size)
	// update index
	if err = v.indexer.Add(key, offset, size); err != nil {
		v.lock.Unlock()
		return
	}
	v.needles[key] = NewNeedleCache(offset, size)
	v.lock.Unlock()
	if ok {
		ooffset, osize = needleCache.Value()
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		// set old file delete
		err = v.asyncDel(offset)
	}
	return
}

// Write add a new needle, if key exists append to super block, then update
// needle cache offset to new offset, Write is used for multi add needles.
func (v *Volume) Write(key, cookie int64, data []byte) (err error) {
	var (
		ok              bool
		size, osize     int32
		offset, ooffset uint32
		needleCache     NeedleCache
	)
	needleCache, ok = v.needles[key]
	// add needle
	if offset, size, err = v.block.Write(key, cookie, data); err != nil {
		return
	}
	log.V(1).Infof("add needle, offset: %d, size: %d", offset, size)
	// update index
	if err = v.indexer.Write(key, offset, size); err != nil {
		return
	}
	v.needles[key] = NewNeedleCache(offset, size)
	if ok {
		ooffset, osize = needleCache.Value()
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		// set old file delete
		err = v.asyncDel(offset)
	}
	return
}

// Flush flush block&indexer buffer to disk, this is used for multi add needles.
func (v *Volume) Flush() (err error) {
	if err = v.block.Flush(); err != nil {
		return
	}
	if err = v.indexer.Flush(); err != nil {
		return
	}
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
	}
	return
}

// Del logical delete a needle, update disk needle flag and memory needle
// cache offset to zero.
func (v *Volume) Del(key int64) (err error) {
	var (
		ok          bool
		size        int32
		offset      uint32
		needleCache NeedleCache
	)
	// get a needle, update the offset to del
	v.lock.Lock()
	needleCache, ok = v.needles[key]
	if ok {
		offset, size = needleCache.Value()
		v.needles[key] = NewNeedleCache(NeedleCacheDelOffset, size)
	}
	v.lock.Unlock()
	if ok {
		// async update super block flag
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
	log.Infof("start volume: %d del goroutine", v.Id)
	for {
		select {
		case offset = <-v.signal:
			if offset == volumeFinish {
				log.Info("signal volume del goroutine exit")
				break
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
			if err = v.block.Del(offset); err != nil {
				break
			}
		}
		offsets = offsets[:0]
	}
	log.Errorf("volume del goroutine exit")
	return
}

// Compress copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) Compress(nv *Volume) (err error) {
	// scan the whole super block, skip the del,error needle.
	// update pointer
	err = v.block.Compress(nv)
	return
}

// Close close the volume.
func (v *Volume) Close() {
	v.lock.Lock()
	v.block.Close()
	v.indexer.Close()
	close(v.signal)
	v.lock.Unlock()
	return
}
