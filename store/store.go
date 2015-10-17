package main

import (
	log "github.com/golang/glog"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
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

const (
	volumeIndexComma   = ","
	volumeIndexSpliter = "\n"
)

// Store save volumes.
type Store struct {
	VolumeId int32
	volumes  map[int32]*Volume
	file     string
	f        *os.File
}

// NewStore
func NewStore(file string) (s *Store, err error) {
	var (
		i         int
		volume    *Volume
		files     []string
		volumeIds []int32
	)
	s = &Store{}
	s.VolumeId = 1
	s.volumes = make(map[int32]*Volume)
	s.file = file
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
		if volume, err = s.AddVolume(volumeIds[i], files[i], files[i]+".idx"); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s", volumeIds[i], files[i])
			continue
		}
		s.volumes[volumeIds[i]] = volume
		log.Infof("finish recovery volume_id: %d, file: %s", volumeIds[i], files[i])
	}
	log.Infof("current max volume id: %d", s.VolumeId)
	return
}

func (s *Store) AddVolume(id int32, bfile, ifile string) (v *Volume, err error) {
	// test
	if v, err = NewVolume(id, bfile, ifile); err != nil {
		return
	}
	s.volumes[id] = v
	return
}

// Volume get a volume by a volume id.
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
