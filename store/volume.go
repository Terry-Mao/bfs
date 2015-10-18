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
	readOnly bool
	bulk     bool
	compress bool

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
	log.Info("try recovery from index: %s", v.indexer.File)
	if offset, err = v.indexer.Recovery(v.needles); err != nil {
		log.Info("recovery from index: %s error(%v)", v.indexer.File, err)
		return
	}
	log.Info("finish recovery from index: %s", v.indexer.File)
	log.V(1).Infof("recovery offset: %d\n", offset)
	log.Info("try recovery from indexsuper block %s", v.block.File)
	// recovery from super block
	if err = v.block.Recovery(v.needles, v.indexer, BlockOffset(offset)); err != nil {
		log.Info("recovery from super block: %s error(%v)", v.block.File, err)
		return
	}
	log.Info("finish recovery from block: %s", v.indexer.File)
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
	log.Infof("get needle, key: %d, cookie: %d", key, cookie)
	// get a needle
	v.lock.Lock()
	needleCache, ok = v.needles[key]
	v.lock.Unlock()
	if !ok {
		err = ErrNeedleNotExists
		return
	}
	size, offset = needleCache.Value()
	if offset == NeedleCacheDelOffset {
		err = ErrNeedleDeleted
		return
	}
	buf = make([]byte, size)
	// WARN atomic read superblock, pread syscall is atomic
	if err = v.block.Read(offset, buf); err != nil {
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
		err = ErrNeedleKeyNotMatch
		return
	}
	if needle.Cookie != cookie {
		err = ErrNeedleCookieNotMatch
		return
	}
	// if delete
	if needle.Flag == NeedleStatusDel {
		v.lock.Lock()
		v.needles[key] = NewNeedleCache(size, NeedleCacheDelOffset)
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
	// superblock append
	if size, offset, err = v.block.Append(key, cookie, data); err != nil {
		v.lock.Unlock()
		return
	}
	// update needle map
	v.needles[key] = NewNeedleCache(size, offset)
	// update index
	if err = v.indexer.Add(key, offset, size); err != nil {
		v.lock.Unlock()
		return
	}
	v.lock.Unlock()
	if ok {
		osize, ooffset = needleCache.Value()
		log.Warningf("same key: %d add a new needle, old offset: %d, old size: %d, new offset: %d, new size: %d", key, ooffset, osize, offset, size)
		// TODO set old file delete?
	}
	return
}

func (v *Volume) MultiAdd(keys, cookies []int64, data [][]byte) (offsets []int64, err error) {
	// TODO
	// superblock append
	// update needle map
	// aync update index
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
		err = ErrNeedleNotExists
	} else {
		size, offset = needleCache.Value()
		v.needles[key] = NewNeedleCache(size, NeedleCacheDelOffset)
		// update super block flag
		v.block.Del(offset)
	}
	v.lock.Unlock()
	return
}

// Compress copy the super block to another space, and drop the "delete"
// needle, so this can reduce disk space cost.
func (v *Volume) Compress(bfile, ifile string) (err error) {
	// TODO
	// scan the whole super block, skip the del needle.
	// copy to bfile
	// write index
	// update needles
	// update needles pointer
	// update block
	// update indexer
	return
}

func (v *Volume) Close() {
	v.lock.Lock()
	v.block.Close()
	v.indexer.Close()
	v.lock.Unlock()
	return
}
