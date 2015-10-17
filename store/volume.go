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
	wlock   sync.Mutex
	rlock   sync.Mutex
	block   *SuperBlock
	indexer *Indexer
	needles map[int64]NeedleCache
	// TODO status
}

func NewVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	v = &Volume{}
	v.Id = id
	if v.block, err = NewSuperBlock(bfile); err != nil {
		return
	}
	if v.indexer, err = NewIndexer(ifile, 10240, 4*1024); err != nil {
		return
	}
	v.needles = make(map[int64]NeedleCache)
	if err = v.init(); err != nil {
		return
	}
	return
}

func (v *Volume) init() (err error) {
	var offset uint32
	// recovery from index
	if offset, err = v.indexer.Recovery(v.needles); err != nil {
		log.Infof("%v\n", err)
	}
	log.Infof("recovery offset: %d\n", offset)
	// recovery from super block
	if err = v.block.Recovery(v.needles, v.indexer, BlockOffset(offset)); err != nil {
		return
	}
	return
}

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
	v.wlock.Lock()
	if needleCache, ok = v.needles[key]; !ok {
		err = ErrNeedleNotExists
		v.wlock.Unlock()
		return
	}
	v.wlock.Unlock()
	size, offset = needleCache.Value()
	if offset == NeedleCacheDelOffset {
		err = ErrNeedleDeleted
		return
	}
	buf = make([]byte, size)
	// read superblock
	v.rlock.Lock()
	if err = v.block.Read(offset, buf); err != nil {
		v.rlock.Unlock()
		return
	}
	v.rlock.Unlock()
	// parse needle
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
		v.wlock.Lock()
		v.needles[key] = NewNeedleCache(size, NeedleCacheDelOffset)
		v.wlock.Unlock()
		err = ErrNeedleDeleted
		return
	}
	data = needle.Data
	return
}

func (v *Volume) Add(key, cookie int64, data []byte) (err error) {
	var (
		ok     bool
		size   int32
		offset uint32
	)
	v.wlock.Lock()
	// TODO update needlecache
	if _, ok = v.needles[key]; ok {
		err = ErrNeedleAlreadyExists
		v.wlock.Unlock()
		return
	}
	// superblock append
	if size, offset, err = v.block.Append(key, cookie, data); err != nil {
		v.wlock.Unlock()
		return
	}
	// update needle map
	v.needles[key] = NewNeedleCache(size, offset)
	// update index
	if err = v.indexer.Add(key, offset, size); err != nil {
		v.wlock.Unlock()
		return
	}
	v.wlock.Unlock()
	return
}

func (v *Volume) MultiAdd(keys, cookies []int64, data [][]byte) (offsets []int64, err error) {
	// TODO
	// superblock append
	// update needle map
	// aync update index
	return
}

func (v *Volume) Del(key int64) (err error) {
	var (
		ok          bool
		size        int32
		offset      uint32
		needleCache NeedleCache
	)
	// get a needle, update the offset to del
	v.wlock.Lock()
	if needleCache, ok = v.needles[key]; !ok {
		err = ErrNeedleNotExists
		v.wlock.Unlock()
		return
	}
	size, offset = needleCache.Value()
	v.needles[key] = NewNeedleCache(size, NeedleCacheDelOffset)
	// update super block flag
	v.block.Del(offset)
	v.wlock.Unlock()
	return
}

func (v *Volume) Compress() (err error) {
	// scan the whole super block, skip the del needle.
	// copy to dst
	// update needles
	// update needles pointer
	return
}
