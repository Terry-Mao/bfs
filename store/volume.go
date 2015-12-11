package main

import (
	"fmt"
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/libs/stat"
	"github.com/Terry-Mao/bfs/store/block"
	"github.com/Terry-Mao/bfs/store/index"
	"github.com/Terry-Mao/bfs/store/needle"
	log "github.com/golang/glog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// signal command
	volumeFinish = 0
	volumeReady  = 1
)

// Uint32Slice deleted offset sort.
type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type CheckNeedle struct {
	Key    int64 `json:"key"`
	Cookie int32 `json:"cookie"`
}

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
	ch      chan uint32
	// compact
	Compact       bool   `json:"compact"`
	CompactOffset uint32 `json:"compact_offset"`
	CompactTime   int64  `json:"compact_time"`
	compactKeys   []int64
	// status
	closed bool
	Debug  bool
	// check
	CheckNeedles  []CheckNeedle `json:"check_needles"`
	checkMaxIdx   int
	checkCurIdx   int
	check         int
	checkInterval int
}

// NewVolume new a volume and init it.
func NewVolume(id int32, bfile, ifile string, c *Config) (v *Volume, err error) {
	v = &Volume{}
	v.Id = id
	v.conf = c
	v.Debug = c.DebugVolume
	v.closed = false
	v.Stats = &stat.Stats{}
	v.needles = make(map[int64]int64, c.VolumeNeedleCache)
	v.ch = make(chan uint32, c.VolumeDelChan)
	v.compactKeys = []int64{}
	v.checkCurIdx = 0
	v.checkMaxIdx = c.VolumeCheckSize - 1
	v.check = 0
	v.checkInterval = c.VolumeCheckInterval
	v.CheckNeedles = make([]CheckNeedle, c.VolumeCheckSize)
	if v.Block, err = block.NewSuperBlock(bfile, block.Options{
		BufferSize:    c.NeedleMaxSize * c.BatchMaxNum,
		SyncAtWrite:   c.SuperBlockSync,
		Syncfilerange: c.SuperBlockSyncfilerange,
	}); err != nil {
		return nil, err
	}
	if v.Indexer, err = index.NewIndexer(ifile, index.Options{
		MergeAtTime:   c.IndexMergeTime,
		MergeAtWrite:  c.IndexMerge,
		RingBuffer:    c.IndexRingBuffer,
		BufferSize:    c.IndexBufferio,
		SyncAtWrite:   c.IndexSync,
		Syncfilerange: c.IndexSyncfilerange,
	}); err != nil {
		v.Close()
		return nil, err
	}
	if err = v.init(); err != nil {
		v.Close()
		return nil, err
	}
	v.wg.Add(1)
	go v.del()
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

// Meta get index meta data.
func (v *Volume) Meta() []byte {
	return []byte(fmt.Sprintf("%s,%s,%d", v.Block.File, v.Indexer.File, v.Id))
}

// ParseMeta parse index meta data.
func (v *Volume) ParseMeta(line string) (bfile, ifile string, id int32, err error) {
	var (
		vid  int64
		seps []string
	)
	if seps = strings.Split(line, ","); len(seps) != 3 {
		log.Errorf("volume index: \"%s\" format error", line)
		err = errors.ErrStoreVolumeIndex
		return
	}
	bfile = seps[0]
	ifile = seps[1]
	if vid, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
		log.Errorf("volume index: \"%s\" format error", line)
	} else {
		id = int32(vid)
	}
	return
}

// IsClosed reports whether the volume is closed.
func (v *Volume) IsClosed() bool {
	return v.closed
}

// Get get a needle by key and cookie.
func (v *Volume) Get(key int64, cookie int32, buf []byte, n *needle.Needle) (err error) {
	var (
		now    = time.Now().UnixNano()
		ok     bool
		nc     int64
		size   int32
		offset uint32
	)
	// WARN pread syscall is atomic, so use rlock
	v.lock.RLock()
	if nc, ok = v.needles[key]; ok {
		offset, size = needle.Cache(nc)
		if offset != needle.CacheDelOffset {
			err = v.Block.Get(offset, buf[:size])
		} else {
			err = errors.ErrNeedleDeleted
		}
	} else {
		err = errors.ErrNeedleNotExist
	}
	v.lock.RUnlock()
	if err != nil {
		return
	}
	if log.V(1) {
		log.Infof("get needle key: %d, cookie: %d, offset: %d, size: %d", key, cookie, offset, size)
	}
	if err = n.ParseHeader(buf[:needle.HeaderSize]); err != nil {
		return
	}
	if n.TotalSize != size {
		err = errors.ErrNeedleSize
		return
	}
	if err = n.ParseFooter(buf[needle.HeaderSize:size]); err != nil {
		return
	}
	if log.V(1) {
		log.Infof("%v\n", n)
	}
	if n.Key != key {
		err = errors.ErrNeedleKey
		return
	}
	if n.Cookie != cookie {
		err = errors.ErrNeedleCookie
		return
	}
	// needles map may be out-dated, recheck
	if n.Flag == needle.FlagDel {
		v.lock.Lock()
		v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
		v.lock.Unlock()
		err = errors.ErrNeedleDeleted
	} else {
		atomic.AddUint64(&v.Stats.TotalGetProcessed, 1)
		atomic.AddUint64(&v.Stats.TotalReadBytes, uint64(size))
		atomic.AddUint64(&v.Stats.TotalGetDelay, uint64(time.Now().UnixNano()-now))
	}
	return
}

// addCheck add a check key for pitchfork check block health.
func (v *Volume) addCheck(key int64, cookie int32) {
	if v.check++; v.check >= v.checkInterval {
		v.check = 0
		if v.checkCurIdx > v.checkMaxIdx {
			v.checkCurIdx = 0
		}
		v.CheckNeedles[v.checkCurIdx] = CheckNeedle{Key: key, Cookie: cookie}
		v.checkCurIdx++
	}
	return
}

// Add add a needles, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Add(n *needle.Needle, buf []byte) (err error) {
	var (
		now             = time.Now().UnixNano()
		ok              bool
		nc              int64
		offset, ooffset uint32
	)
	if v.Debug {
		if n.TotalSize != int32(len(buf)) {
			err = errors.ErrNeedleSize
			return
		}
	}
	v.lock.Lock()
	offset = v.Block.Offset
	if err = v.Block.Write(buf); err == nil {
		if err = v.Indexer.Add(n.Key, offset, n.TotalSize); err == nil {
			nc, ok = v.needles[n.Key]
			v.needles[n.Key] = needle.NewCache(offset, n.TotalSize)
			v.addCheck(n.Key, n.Cookie)
		}
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
		err = v.asyncDel(ooffset)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", n.Key, ooffset, offset)
	}
	atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
	atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
	return
}

// Write write needles, if key exists append to super block, then update
// needle cache offset to new offset.
func (v *Volume) Write(ns []needle.Needle, buf []byte) (err error) {
	var (
		now             = time.Now().UnixNano()
		i               int
		ok              bool
		nc              int64
		ncs             []int64
		offset, ooffset uint32
		ts              int32
		n               *needle.Needle
	)
	if v.Debug {
		for i = 0; i < len(ns); i++ {
			n = &ns[i]
			ts += n.TotalSize
		}
		if int(ts) != len(buf) {
			err = errors.ErrNeedleSize
			return
		}
	}
	v.lock.Lock()
	offset = v.Block.Offset
	if err = v.Block.Write(buf); err == nil {
		for i = 0; i < len(ns); i++ {
			n = &ns[i]
			if err = v.Indexer.Add(n.Key, offset, n.TotalSize); err != nil {
				break
			}
			if nc, ok = v.needles[n.Key]; ok {
				ncs = append(ncs, nc)
			}
			v.needles[n.Key] = needle.NewCache(offset, n.TotalSize)
			v.addCheck(n.Key, n.Cookie)
			offset += n.IncrOffset
			if log.V(1) {
				log.Infof("add needle, offset: %d, size: %d", offset, n.TotalSize)
			}
		}
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	for _, nc = range ncs {
		ooffset, _ = needle.Cache(nc)
		err = v.asyncDel(ooffset)
		log.Warningf("same key: %d, old offset: %d, new offset: %d", n.Key, ooffset, offset)
	}
	atomic.AddUint64(&v.Stats.TotalWriteProcessed, 1)
	atomic.AddUint64(&v.Stats.TotalWriteBytes, uint64(n.TotalSize))
	atomic.AddUint64(&v.Stats.TotalWriteDelay, uint64(time.Now().UnixNano()-now))
	return
}

// asyncDel signal the godel goroutine aync merge all offsets and del.
func (v *Volume) asyncDel(offset uint32) (err error) {
	if offset == needle.CacheDelOffset {
		return
	}
	select {
	case v.ch <- offset:
	default:
		log.Errorf("volume: %d send signal failed", v.Id)
		err = errors.ErrVolumeDel
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
		if offset, size = needle.Cache(nc); offset != needle.CacheDelOffset {
			v.needles[key] = needle.NewCache(needle.CacheDelOffset, size)
			// when in compact, must save all del operations.
			if v.Compact {
				v.compactKeys = append(v.compactKeys, key)
			}
		} else {
			err = errors.ErrNeedleDeleted
		}
	} else {
		err = errors.ErrNeedleNotExist
	}
	v.lock.Unlock()
	if err == nil {
		err = v.asyncDel(offset)
	}
	return
}

// del merge from volume signal, then update block needles flag.
func (v *Volume) del() {
	var (
		err     error
		now     int64
		exit    bool
		offset  uint32
		offsets []uint32
	)
	log.Infof("volume: %d del job start", v.Id)
	for {
		select {
		case offset = <-v.ch:
			if exit = (offset == volumeFinish); !exit {
				if offsets = append(offsets, offset); len(offsets) < v.conf.VolumeSigCnt {
					continue
				}
			}
		case <-time.After(v.conf.VolumeSigTime):
			exit = false
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
		if exit {
			log.Warningf("signal volume: %d del job exit", v.Id)
			break
		}
	}
	v.wg.Done()
	log.Warningf("volume: %d del job exit", v.Id)
	return
}

// compact compact v to new v.
func (v *Volume) compact(nv *Volume) (err error) {
	var buf = make([]byte, v.conf.NeedleMaxSize)
	err = v.Block.Compact(v.CompactOffset, func(n *needle.Needle, so, eo uint32) (err1 error) {
		if n.TotalSize > int32(v.conf.NeedleMaxSize) || n.TotalSize < 0 {
			err1 = errors.ErrNeedleSize
			return
		}
		if n.Flag != needle.FlagDel {
			n.Write(buf)
			if err1 = nv.Add(n, buf); err1 != nil {
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
	if v.Compact {
		err = errors.ErrVolumeInCompact
	} else {
		v.Compact = true
	}
	v.lock.Unlock()
	if err != nil {
		return
	}
	v.CompactTime = time.Now().UnixNano()
	if err = v.compact(nv); err != nil {
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
		if err = v.compact(nv); err != nil {
			goto free
		}
		for _, key = range v.compactKeys {
			if err = nv.Del(key); err != nil {
				goto free
			}
		}
		atomic.AddUint64(&v.Stats.TotalCompactDelay, uint64(time.Now().UnixNano()-now))
	}
free:
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
	defer v.lock.Unlock()
	if !v.closed {
		return
	}
	if err = v.Block.Open(); err != nil {
		v.Close()
		return
	}
	if err = v.Indexer.Open(); err != nil {
		v.Close()
		return
	}
	if err = v.init(); err != nil {
		v.Close()
		return
	}
	v.closed = false
	v.wg.Add(1)
	go v.del()
	return
}

// Close close the volume.
func (v *Volume) Close() {
	v.lock.Lock()
	defer v.lock.Unlock()
	if v.ch != nil {
		v.ch <- volumeFinish
		v.wg.Wait()
	}
	if v.Block != nil {
		v.Block.Close()
	}
	if v.Indexer != nil {
		v.Indexer.Close()
	}
	v.closed = true
}

// Destroy remove block and index file, must called after Close().
func (v *Volume) Destroy() {
	v.lock.Lock()
	defer v.lock.Unlock()
	if !v.closed {
		v.Close()
	}
	if v.Block != nil {
		v.Block.Destroy()
	}
	if v.Indexer != nil {
		v.Indexer.Destroy()
	}
}
