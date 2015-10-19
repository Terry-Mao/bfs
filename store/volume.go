package main

import (
	log "github.com/golang/glog"
	"sync"
)

const (
	// 32GB, offset aligned 8 bytes, 4GB * 8
	VolumeMaxSize = 4 * 1024 * 1024 * 1024 * 8
)

// An store server contains many logic Volume, volume is superblock container.
type Volume struct {
	Id      int32
	lock    sync.Mutex
	block   *SuperBlock
	indexer *Indexer
	needles map[int64]NeedleCache
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
func (v *Volume) Get(key, cookie int64) (data []byte, err error) {
	var (
		ok          bool
		buf         []byte
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
	buf = make([]byte, size)
	// WARN atomic read superblock, pread syscall is atomic
	if err = v.block.Get(offset, buf); err != nil {
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
	if ok {
		ooffset, osize = needleCache.Value()
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		// set old file delete?
		v.block.Del(ooffset)
	}
	v.lock.Unlock()
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
	if !ok {
		err = ErrNoNeedle
	} else {
		offset, size = needleCache.Value()
		v.needles[key] = NewNeedleCache(NeedleCacheDelOffset, size)
		// update super block flag
		v.block.Del(offset)
	}
	v.lock.Unlock()
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

func (v *Volume) Close() {
	v.lock.Lock()
	v.block.Close()
	v.indexer.Close()
	v.lock.Unlock()
	return
}
