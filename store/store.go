package main

import (
	log "github.com/golang/glog"
	"io/ioutil"
	"os"
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
//  ----------------------------
// | super_block_path,volume_id |
// | /bfs/super_block_1,1\r     |
// | /bfs/super_block_2,2\r     |
//  ----------------------------
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
	storeAdd    = 0
	storeUpdate = 1
	storeDel    = 2
)

// Store save volumes.
type Store struct {
	f        *os.File
	ch       chan *Volume
	file     string
	bp       *sync.Pool
	VolumeId int32
	volumes  map[int32]*Volume
}

// NewStore
func NewStore(file string) (s *Store, err error) {
	var (
		i         int
		files     []string
		volume    *Volume
		volumeIds []int32
	)
	s = &Store{}
	s.VolumeId = 1
	s.volumes = make(map[int32]*Volume)
	s.file = file
	s.ch = make(chan *Volume, storeMap)
	go s.update()
	if s.f, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDWR|os.O_CREATE, 0664) error(%v)", file, err)
		return
	}
	if volumeIds, files, err = s.parseIndex(); err != nil {
		log.Errorf("parse volume index failed, check the volume index file format")
		return
	}
	for i = 0; i < len(files); i++ {
		log.Infof("start recovery volume_id: %d, file: %s", volumeIds[i], files[i])
		if volume, err = NewVolume(volumeIds[i], files[i], files[i]+".idx"); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s", volumeIds[i], files[i])
			continue
		}
		s.volumes[volumeIds[i]] = volume
		log.Infof("finish recovery volume_id: %d, file: %s", volumeIds[i], files[i])
	}
	s.bp = &sync.Pool{}
	log.Infof("current max volume id: %d", s.VolumeId)
	return
}

// update atomic update volumes.
func (s *Store) update() {
	var (
		volumeId  int32
		v, vt, vc *Volume
		volumes   map[int32]*Volume
	)
	for {
		v = <-s.ch
		// copy-on-write
		volumes = make(map[int32]*Volume, len(s.volumes))
		for volumeId, vt = range s.volumes {
			volumes[volumeId] = vt
		}
		vc = volumes[v.Id]
		if v.Store == storeAdd {
			volumes[v.Id] = v
		} else if v.Store == storeUpdate {
			volumes[v.Id] = v
		} else if v.Store == storeDel {
			delete(volumes, v.Id)
		} else {
			panic("unknow store flag")
		}
		// close volume
		if vc != nil {
			vc.Close()
		}
		// atomic update ptr
		s.volumes = volumes
	}
}

// AddVolume add a new volume.
func (s *Store) AddVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	// test
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	v.Store = storeAdd
	s.ch <- v
	return
}

// DelVolume del the volume by volume id.
func (s *Store) DelVolume(id int32) {
	var v = s.Volume(id)
	v.Store = storeDel
	s.ch <- v
	return
}

// Volume get a volume by volume id.
func (s *Store) Volume(id int32) *Volume {
	return s.volumes[id]
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex() (volumeIds []int32, files []string, err error) {
	var (
		data        []byte
		bfile, line string
		idx         int
		volumeId    int64
	)
	if data, err = ioutil.ReadAll(s.f); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	for _, line = range strings.Split(string(data), volumeIndexSpliter) {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		if idx = strings.Index(line, volumeIndexComma); idx == -1 {
			log.Errorf("volume index: \"%s\" format error", line)
			err = ErrStoreVolumeIndex
			return
		}
		bfile = line[:idx]
		if volumeId, err = strconv.ParseInt(line[idx+1:], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		volumeIds = append(volumeIds, int32(volumeId))
		files = append(files, bfile)
		if int32(volumeId) > s.VolumeId {
			// reset max volume id
			s.VolumeId = int32(volumeId)
		}
		log.V(1).Infof("get volume index, volume_id: %d, file: %s", volumeId, bfile)
	}
	return
}

// saveIndex save volumes index info to disk.
func (s *Store) saveIndex() {
	// sort the volumes map
	// write file
	// flush
}

// Bulk copy a super block from another store server replace this server.
func (s *Store) Bulk(id int32, bfile, ifile string) (err error) {
	var v *Volume
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	v.Store = storeUpdate
	s.ch <- v
	return
}

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
	if err = v.Compress(nv); err != nil {
		return
	}
	// v.Lock
	// copy new add needles
	// set del flag
	// set readonly?
	// v.Unlock
	v.Store = storeUpdate
	s.ch <- nv
	return
}

// Buffer get a buffer from sync.Pool
func (s *Store) Buffer() (d []byte) {
	var v interface{}
	if v = s.bp.Get(); v != nil {
		d = v.([]byte)
		return
	}
	return make([]byte, NeedleMaxSize)
}

// Free free the buffer to pool.
func (s *Store) Free(d []byte) {
	s.bp.Put(d)
}
