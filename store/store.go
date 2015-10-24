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
	storeAdd      = 1
	storeUpdate   = 2
	storeDel      = 3
	storeCompress = 4
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
}

// NewStore
func NewStore(file string) (s *Store, err error) {
	var (
		i              int
		bfiles, ifiles []string
		volume         *Volume
		volumeIds      []int32
	)
	s = &Store{}
	s.VolumeId = 1
	s.volumes = make(map[int32]*Volume)
	s.file = file
	s.ch = make(chan *Volume, storeMap)
	go s.command()
	if s.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDWR|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	if volumeIds, bfiles, ifiles, err = s.parseIndex(); err != nil {
		log.Errorf("parse volume index failed, check the volume index file format")
		return
	}
	for i = 0; i < len(bfiles); i++ {
		if volume, err = NewVolume(volumeIds[i], bfiles[i], ifiles[i]); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", volumeIds[i], bfiles[i], ifiles[i])
			continue
		}
		s.volumes[volumeIds[i]] = volume
	}
	log.Infof("current max volume id: %d", s.VolumeId)
	return
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex() (volumeIds []int32, bfiles []string, ifiles []string, err error) {
	var (
		data               []byte
		bfile, ifile, line string
		seps               []string
		volumeId           int64
	)
	if data, err = ioutil.ReadAll(s.f); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	for _, line = range strings.Split(string(data), volumeIndexSpliter) {
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
		v            *Volume
		ok           bool
		vid          int32
		bfile, ifile string
		vids         = make([]int32, 0, len(s.volumes))
	)
	for vid, v = range s.volumes {
		vids = append(vids, vid)
	}
	sort.Sort(Int32Slice(vids))
	for _, vid = range vids {
		if v, ok = s.volumes[vid]; ok {
			bfile, ifile = v.Block.File, v.Indexer.File
			if _, err = s.f.Write([]byte(fmt.Sprintf("%s,%s,%d\n", bfile, ifile, vid))); err != nil {
				return
			}
		}
	}
	err = s.f.Sync()
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
			log.Errorf("signal store command goroutine exit")
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
		} else if v.Command == storeCompress {
			if err = vc.StopCompress(v); err != nil {
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
	log.Errorf("store command goroutine exit")
}

func (s *Store) stat() {
	var v *Volume
	for {
		for _, v = range s.volumes {
			v.Stats.Calc()
			StoreInfo.Stats.Merge(v.Stats)
		}
		StoreInfo.Stats.Calc()
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

// Compress compress a super block to another file.
func (s *Store) Compress(id int32, bfile, ifile string) (err error) {
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
	// set volume compress flag
	// copy to new volume
	if err = v.StartCompress(nv); err != nil {
		v.StopCompress(nil)
		return
	}
	nv.Command = storeCompress
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
