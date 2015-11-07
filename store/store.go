package main

import (
	"fmt"
	"github.com/Terry-Mao/bfs/store/errors"
	log "github.com/golang/glog"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Store get all volume meta data from a index file. index contains volume id,
// volume file path, the super block file index ends with ".idx" if the super
// block is /bfs/super_block_1, then the super block index file is
// /bfs/super_block_1.idx.
//
// volume index file format:
//  ---------------------------------
// | block_path,index_path,volume_id |
// | /bfs/block_1,/bfs/block_1.idx\r |
// | /bfs/block_2,/bfs/block_2.idx\r |
//  ---------------------------------
//
// store -> N volumes
//		 -> volume index -> volume info
//
// volume -> super block -> needle -> photo info
//        -> block index -> needle -> photo info without raw data

const (
	volumeIndexComma      = ","
	volumeIndexSpliter    = "\n"
	storeFreeVolumePrefix = "block_"
)

// Int32Slice sort volumes.
type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Store save volumes.
type Store struct {
	f           *os.File
	VolumeId    int32
	Volumes     map[int32]*Volume // TODO split volumes lock
	FreeVolumes []*Volume
	zk          *Zookeeper
	conf        *Config
	lock        sync.Mutex
	vlock       sync.RWMutex
}

// NewStore
func NewStore(zk *Zookeeper, c *Config) (s *Store, err error) {
	s = &Store{}
	s.zk = zk
	s.conf = c
	s.VolumeId = 0
	s.Volumes = make(map[int32]*Volume, c.StoreVolumeCache)
	if s.f, err = os.OpenFile(c.StoreIndex, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.StoreIndex, err)
		return
	}
	if err = s.init(c); err != nil {
		return
	}
	log.Infof("current max volume id: %d", s.VolumeId)
	return
}

// init parse index from local config and zookeeper.
func (s *Store) init(c *Config) (err error) {
	var (
		i                int
		ok               bool
		bfiles, ifiles   []string
		bfiles1, ifiles1 []string
		lines            []string
		data             []byte
		volume           *Volume
		vids, vids1      []int32
		vMap, vMap1      map[int32]struct{}
	)
	if data, err = ioutil.ReadAll(s.f); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), volumeIndexSpliter)
	if vMap, vids, bfiles, ifiles, err = s.parseIndex(lines); err != nil {
		return
	}
	if lines, err = s.zk.Volumes(); err != nil {
		return
	}
	if vMap1, vids1, bfiles1, ifiles1, err = s.parseIndex(lines); err != nil {
		return
	}
	for i = 0; i < len(bfiles); i++ {
		if _, ok = s.Volumes[vids[i]]; ok {
			continue
		}
		// local index
		if volume, err = NewVolume(vids[i], bfiles[i], ifiles[i], s.conf); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s, index: %s",
				vids[i], bfiles[i], ifiles[i])
			continue
		}
		if volume.Id == VolumeEmptyId {
			volume.Close()
			s.FreeVolumes = append(s.FreeVolumes, volume)
		} else {
			s.Volumes[vids[i]] = volume
			if _, ok = vMap1[vids[i]]; !ok {
				// if not exists in zk, must readd to zk
				log.Infof("volume_id: %d not exist in zk", vids[i])
				if err = s.zk.AddVolume(volume); err != nil {
					return
				}
			}
			log.Infof("load volume: %d", vids[i])
		}
	}
	for i = 0; i < len(bfiles1); i++ {
		if _, ok = s.Volumes[vids1[i]]; ok {
			continue
		}
		// zk index
		if _, ok = vMap[vids1[i]]; !ok {
			// if not exists in local
			if volume, err = NewVolume(vids1[i], bfiles1[i], ifiles1[i], s.conf); err != nil {
				log.Warningf("fail recovery volume_id: %d, file: %s, index: %s",
					vids1[i], bfiles1[i], ifiles1[i])
				continue
			}
			s.Volumes[vids1[i]] = volume
		}
	}
	err = s.saveIndex()
	return
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex(lines []string) (vMap map[int32]struct{},
	vids []int32, bfiles []string, ifiles []string, err error) {
	var (
		bfile, ifile, line string
		seps               []string
		volumeId           int64
	)
	vMap = make(map[int32]struct{})
	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		seps = strings.Split(line, volumeIndexComma)
		if len(seps) != 3 {
			err = errors.ErrStoreVolumeIndex
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		bfile = seps[0]
		ifile = seps[1]
		if volumeId, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		vids = append(vids, int32(volumeId))
		bfiles = append(bfiles, bfile)
		ifiles = append(ifiles, ifile)
		vMap[int32(volumeId)] = struct{}{}
		if int32(volumeId) > s.VolumeId {
			// reset max volume id
			s.VolumeId = int32(volumeId)
		}
		log.V(1).Infof("parse volume index, vid: %d, file: %s, index: %s",
			volumeId, bfile, ifile)
	}
	return
}

// saveIndex save volumes index info to disk.
func (s *Store) saveIndex() (err error) {
	var (
		tn, n        int
		v            *Volume
		ok           bool
		vid          int32
		bfile, ifile string
		data         []byte
		vids         = make([]int32, 0, len(s.Volumes))
	)
	if _, err = s.f.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	for vid, v = range s.Volumes {
		vids = append(vids, vid)
	}
	sort.Sort(Int32Slice(vids))
	// volumes
	for _, vid = range vids {
		if v, ok = s.Volumes[vid]; ok {
			bfile, ifile = v.Block.File, v.Indexer.File
			data = []byte(fmt.Sprintf("%s,%s,%d\n", bfile, ifile, vid))
			if n, err = s.f.Write(data); err != nil {
				return
			}
		}
		tn += n
	}
	// free volumes
	for _, v = range s.FreeVolumes {
		bfile, ifile = v.Block.File, v.Indexer.File
		data = []byte(fmt.Sprintf("%s,%s,%d\n", bfile, ifile, v.Id))
		if n, err = s.f.Write(data); err != nil {
			return
		}
		tn += n
	}
	if err = s.f.Sync(); err != nil {
		return
	}
	err = os.Truncate(s.conf.StoreIndex, int64(tn))
	return
}

// AddFreeVolume add free volumes.
func (s *Store) AddFreeVolume(n int, bdir, idir string) (sn int, err error) {
	var (
		i            int
		bfile, ifile string
		v            *Volume
	)
	s.lock.Lock()
	for i = 0; i < n; i++ {
		s.VolumeId++
		bfile = path.Join(bdir, fmt.Sprintf("%s%d", storeFreeVolumePrefix, s.VolumeId))
		ifile = path.Join(bdir, fmt.Sprintf("%s%d.idx", storeFreeVolumePrefix, s.VolumeId))
		if v, err = NewVolume(VolumeEmptyId, bfile, ifile, s.conf); err != nil {
			break
		}
		v.Close()
		s.FreeVolumes = append(s.FreeVolumes, v)
		sn++
	}
	err = s.saveIndex()
	s.lock.Unlock()
	return
}

// freeVolume get a free volume.
func (s *Store) freeVolume() (v *Volume, err error) {
	if len(s.FreeVolumes) == 0 {
		err = errors.ErrStoreNoFreeVolume
	} else {
		v = s.FreeVolumes[0]
		s.FreeVolumes = s.FreeVolumes[1:]
		err = v.Open()
	}
	return
}

// AddVolume add a new volume.
func (s *Store) AddVolume(id int32) (v *Volume, err error) {
	s.vlock.Lock()
	if v = s.Volumes[id]; v == nil {
		if v, err = s.freeVolume(); err == nil {
			v.Id = id
			if err = s.zk.AddVolume(v); err == nil {
				s.Volumes[id] = v
			}
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	if err != nil {
		s.lock.Lock()
		if id > s.VolumeId {
			s.VolumeId = id
		}
		err = s.saveIndex()
		s.lock.Unlock()
	}
	return
}

// DelVolume del the volume by volume id.
func (s *Store) DelVolume(id int32) (err error) {
	var v *Volume
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		if !v.Compact {
			if err = s.zk.DelVolume(id); err == nil {
				delete(s.Volumes, id)
				v.Close()
			}
		} else {
			err = errors.ErrVolumeInCompact
		}
	} else {
		err = errors.ErrVolumeNotExist
	}
	s.vlock.Unlock()
	if err != nil {
		s.lock.Lock()
		err = s.saveIndex()
		s.lock.Unlock()
	}
	return
}

// BulkVolume copy a super block from another store server replace this server.
func (s *Store) BulkVolume(id int32, bfile, ifile string) (err error) {
	var v, nv *Volume
	if nv, err = NewVolume(id, bfile, ifile, s.conf); err != nil {
		return
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v == nil {
		if err = s.zk.AddVolume(nv); err == nil {
			s.Volumes[id] = nv
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	if err != nil {
		s.lock.Lock()
		err = s.saveIndex()
		s.lock.Unlock()
	}
	return
}

// CompactVolume compact a super block to another file.
func (s *Store) CompactVolume(id int32) (err error) {
	var v, nv *Volume
	s.vlock.RLock()
	if v = s.Volumes[id]; v != nil {
		if v.Compact {
			err = errors.ErrVolumeExist
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.RUnlock()
	if err != nil {
		return
	}
	s.lock.Lock()
	nv, err = s.freeVolume()
	s.lock.Unlock()
	if err != nil {
		return
	}
	// no lock here, Compact is no side-effect
	nv.Id = id
	if err = v.StartCompact(nv); err != nil {
		v.StopCompact(nil)
		return
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		if err = v.StopCompact(nv); err == nil {
			if err = s.zk.SetVolume(nv); err == nil {
				s.Volumes[id] = nv
				v.Close()
			}
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	if err != nil {
		s.lock.Lock()
		err = s.saveIndex()
		s.lock.Unlock()
	}
	return
}

// Close close the store.
// WARN the global variable store must first set nil and reject any other
// requests then safty close.
func (s *Store) Close() {
	var v *Volume
	s.saveIndex()
	if s.f != nil {
		s.f.Close()
	}
	for _, v = range s.Volumes {
		v.Close()
	}
	s.zk.Close()
	return
}
