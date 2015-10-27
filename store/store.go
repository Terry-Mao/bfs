package main

import (
	"fmt"
	log "github.com/golang/glog"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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
	volumeIndexComma   = ","
	volumeIndexSpliter = "\n"
	storeMap           = 10

	// store map flag
	storeAdd     = 1
	storeUpdate  = 2
	storeDel     = 3
	storeCompact = 4
	// stat
	storeStatDuration = 1 * time.Second
)

// Int32Slice sort volumes.
type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Store save volumes.
type Store struct {
	f        *os.File
	ch       chan *Volume
	file     string
	VolumeId int32
	volumes  map[int32]*Volume
	zk       *Zookeeper
}

// NewStore
func NewStore(zk *Zookeeper, file string) (s *Store, err error) {
	s = &Store{}
	s.zk = zk
	s.VolumeId = 1
	s.volumes = make(map[int32]*Volume)
	s.file = file
	s.ch = make(chan *Volume, storeMap)
	go s.command()
	if s.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDWR|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	if err = s.init(); err != nil {
		return
	}
	go s.stat()
	log.Infof("current max volume id: %d", s.VolumeId)
	return
}

func (s *Store) init() (err error) {
	var (
		i                                int
		ok                               bool
		bfiles, ifiles, bfiles1, ifiles1 []string
		volume                           *Volume
		volumeIds, volumeIds1            []int32
		volumeIdMap, volumeIdMap1        map[int32]struct{}
		data                             []byte
		lines                            []string
	)
	if data, err = ioutil.ReadAll(s.f); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), volumeIndexSpliter)
	if volumeIdMap, volumeIds, bfiles, ifiles, err = s.parseIndex(lines); err != nil {
		return
	}
	if lines, err = s.zk.Volumes(); err != nil {
		return
	}
	if volumeIdMap1, volumeIds1, bfiles1, ifiles1, err = s.parseIndex(lines); err != nil {
		return
	}
	for i = 0; i < len(bfiles); i++ {
		if _, ok = s.volumes[volumeIds[i]]; ok {
			continue
		}
		// local index
		if volume, err = NewVolume(volumeIds[i], bfiles[i], ifiles[i]); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", volumeIds[i], bfiles[i], ifiles[i])
			continue
		}
		s.volumes[volumeIds[i]] = volume
		if _, ok = volumeIdMap1[volumeIds[i]]; !ok {
			// if not exists in zk, must readd to zk
			log.Infof("volume_id: %d not exist in zk", volumeIds[i])
			if err = s.zk.AddVolume(volumeIds[i], bfiles[i], ifiles[i]); err != nil {
				return
			}
		}
	}
	for i = 0; i < len(bfiles1); i++ {
		if _, ok = s.volumes[volumeIds1[i]]; ok {
			continue
		}
		// zk index
		if _, ok = volumeIdMap[volumeIds1[i]]; !ok {
			// if not exists in local
			if volume, err = NewVolume(volumeIds1[i], bfiles1[i], ifiles1[i]); err != nil {
				log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", volumeIds1[i], bfiles1[i], ifiles1[i])
				continue
			}
			s.volumes[volumeIds1[i]] = volume
		}
	}
	err = s.saveIndex()
	return
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex(lines []string) (volumeIdMap map[int32]struct{}, volumeIds []int32, bfiles []string, ifiles []string, err error) {
	var (
		bfile, ifile, line string
		seps               []string
		volumeId           int64
	)
	volumeIdMap = make(map[int32]struct{})
	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		seps = strings.Split(line, volumeIndexComma)
		if len(seps) != 3 {
			err = ErrStoreVolumeIndex
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		bfile = seps[0]
		ifile = seps[1]
		if volumeId, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		volumeIds = append(volumeIds, int32(volumeId))
		bfiles = append(bfiles, bfile)
		ifiles = append(ifiles, ifile)
		volumeIdMap[int32(volumeId)] = struct{}{}
		if int32(volumeId) > s.VolumeId {
			// reset max volume id
			s.VolumeId = int32(volumeId)
		}
		log.V(1).Infof("parse volume index, volume_id: %d, file: %s, index: %s", volumeId, bfile, ifile)
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
		vids         = make([]int32, 0, len(s.volumes))
	)
	if _, err = s.f.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	for vid, v = range s.volumes {
		vids = append(vids, vid)
	}
	sort.Sort(Int32Slice(vids))
	for _, vid = range vids {
		if v, ok = s.volumes[vid]; ok {
			bfile, ifile = v.Block.File, v.Indexer.File
			if n, err = s.f.Write([]byte(fmt.Sprintf("%s,%s,%d\n", bfile, ifile, vid))); err != nil {
				return
			}
		}
		tn += n
	}
	if err = s.f.Sync(); err != nil {
		return
	}
	err = os.Truncate(s.file, int64(tn))
	return
}

// command do volume command.
func (s *Store) command() {
	var (
		err       error
		volumeId  int32
		v, vt, vc *Volume
		volumes   map[int32]*Volume
	)
	for {
		v = <-s.ch
		if v == nil {
			break
		}
		// copy-on-write
		volumes = make(map[int32]*Volume, len(s.volumes))
		for volumeId, vt = range s.volumes {
			volumes[volumeId] = vt
		}
		vc = volumes[v.Id]
		if v.Command == storeAdd {
			volumes[v.Id] = v
		} else if v.Command == storeUpdate {
			volumes[v.Id] = v
		} else if v.Command == storeDel {
			delete(volumes, v.Id)
		} else if v.Command == storeCompact {
			if err = vc.StopCompact(v); err != nil {
				continue
			}
			volumes[v.Id] = v
		} else {
			panic("unknow store flag")
		}
		// close volume
		if vc != nil {
			vc.Close()
		}
		// atomic update ptr
		s.volumes = volumes
		if err = s.saveIndex(); err != nil {
			log.Errorf("store save index: %s error(%v)", s.file, err)
		}
	}
}

// stat stat the store.
func (s *Store) stat() {
	var (
		v     *Volume
		stat  = new(Stats)
		stat1 *Stats
	)
	for {
		*stat = *(StoreInfo.Stats)
		stat1 = StoreInfo.Stats
		StoreInfo.Stats = stat
		stat1.Reset()
		for _, v = range s.volumes {
			v.Stats.Calc()
			stat1.Merge(v.Stats)
		}
		stat1.Calc()
		StoreInfo.Stats = stat1
		time.Sleep(storeStatDuration)
	}
}

// AddVolume add a new volume.
func (s *Store) AddVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	// test
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	v.Command = storeAdd
	s.ch <- v
	return
}

// DelVolume del the volume by volume id.
func (s *Store) DelVolume(id int32) {
	var v = s.Volume(id)
	v.Command = storeDel
	s.ch <- v
	return
}

// Volume get a volume by volume id.
func (s *Store) Volume(id int32) *Volume {
	return s.volumes[id]
}

// Bulk copy a super block from another store server replace this server.
func (s *Store) Bulk(id int32, bfile, ifile string) (err error) {
	var v *Volume
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	v.Command = storeUpdate
	s.ch <- v
	return
}

// Compact compact a super block to another file.
func (s *Store) Compact(id int32, bfile, ifile string) (err error) {
	var (
		nv *Volume
		v  = s.Volume(id)
	)
	if v == nil {
		err = ErrVolumeNotExist
		return
	}
	if nv, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	// set volume compact flag
	// copy to new volume
	if err = v.StartCompact(nv); err != nil {
		v.StopCompact(nil)
		return
	}
	nv.Command = storeCompact
	s.ch <- nv
	return
}

// Close close the store.
// WARN the global variable store must first set nil and reject any other
// requests then safty close.
func (s *Store) Close() {
	var v *Volume
	if s.f != nil {
		s.f.Close()
	}
	close(s.ch)
	for _, v = range s.volumes {
		v.Close()
	}
	return
}
