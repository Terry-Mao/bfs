package main

import (
	"fmt"
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	myos "github.com/Terry-Mao/bfs/store/os"
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
// | /bfs/block_1,/bfs/block_1.idx\n |
// | /bfs/block_2,/bfs/block_2.idx\n |
//  ---------------------------------
//
// store -> N volumes
//		 -> volume index -> volume info
//
// volume -> super block -> needle -> photo info
//        -> block index -> needle -> photo info without raw data

const (
	freeVolumePrefix  = "_free_block_"
	volumeIndexPrefix = ".idx"
	volumeFreeId      = -1
)

// Int32Slice sort volumes.
type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Store save volumes.
type Store struct {
	vf          *os.File
	fvf         *os.File
	FreeId      int32
	bp          []*sync.Pool      // buffer pool
	np          *sync.Pool        // needle pool
	Volumes     map[int32]*Volume // TODO split volumes lock
	FreeVolumes []*Volume
	zk          *Zookeeper
	conf        *Config
	lock        sync.Mutex   // protect FreeId & saveIndex
	vlock       sync.RWMutex // protect Volumes map
}

// NewStore
func NewStore(zk *Zookeeper, c *Config) (s *Store, err error) {
	var i int
	s = &Store{}
	s.zk = zk
	s.conf = c
	s.FreeId = 0
	s.Volumes = make(map[int32]*Volume, c.StoreVolumeCache)
	s.bp = make([]*sync.Pool, c.BatchMaxNum)
	s.bp[0] = nil
	for i = 1; i < c.BatchMaxNum; i++ {
		s.bp[i] = &sync.Pool{}
	}
	s.np = &sync.Pool{}
	if s.vf, err = os.OpenFile(c.VolumeIndex, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.VolumeIndex, err)
		s.Close()
		return
	}
	if s.fvf, err = os.OpenFile(c.FreeVolumeIndex, os.O_RDWR|os.O_CREATE, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", c.FreeVolumeIndex, err)
		s.Close()
		return
	}
	if err = s.init(); err != nil {
		s.Close()
		return
	}
	return
}

// init init the store.
func (s *Store) init() (err error) {
	if err = s.parseVolumeIndex(); err != nil {
		return
	}
	err = s.parseFreeVolumeIndex()
	return
}

// parseFreeVolumeIndex parse free index from local.
func (s *Store) parseFreeVolumeIndex() (err error) {
	var (
		i     int
		id    int32
		bfile string
		ifile string
		v     *Volume
		data  []byte
		ids   []int32
		lines []string
		bfs   []string
		ifs   []string
	)
	if data, err = ioutil.ReadAll(s.fvf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), "\n")
	if _, ids, bfs, ifs, err = s.parseIndex(lines); err != nil {
		return
	}
	for i = 0; i < len(bfs); i++ {
		id, bfile, ifile = ids[i], bfs[i], ifs[i]
		if v, err = NewVolume(id, bfile, ifile, s.conf); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", id, bfile, ifile)
			continue
		}
		v.Close()
		s.FreeVolumes = append(s.FreeVolumes, v)
		if id = s.fileFreeId(bfile); id > s.FreeId {
			s.FreeId = id
		}
	}
	log.V(1).Infof("current max free volume id: %d", s.FreeId)
	err = s.saveFreeVolumeIndex()
	return
}

// parseVolumeIndex parse index from local config and zookeeper.
func (s *Store) parseVolumeIndex() (err error) {
	var (
		i          int
		ok         bool
		id         int32
		bfile      string
		ifile      string
		v          *Volume
		data       []byte
		lids, zids []int32
		lines      []string
		lbfs, lifs []string
		zbfs, zifs []string
		lim, zim   map[int32]struct{}
	)
	if data, err = ioutil.ReadAll(s.vf); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	lines = strings.Split(string(data), "\n")
	if lim, lids, lbfs, lifs, err = s.parseIndex(lines); err != nil {
		return
	}
	if lines, err = s.zk.Volumes(); err != nil {
		return
	}
	if zim, zids, zbfs, zifs, err = s.parseIndex(lines); err != nil {
		return
	}
	// local index
	for i = 0; i < len(lbfs); i++ {
		id, bfile, ifile = lids[i], lbfs[i], lifs[i]
		if _, ok = s.Volumes[id]; ok {
			continue
		}
		if v, err = NewVolume(id, bfile, ifile, s.conf); err != nil {
			log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", id, bfile, ifile)
			continue
		}
		s.Volumes[id] = v
		if _, ok = zim[id]; !ok {
			if err = s.zk.AddVolume(v); err != nil {
				continue
			}
		} else {
			if err = s.zk.SetVolume(v); err != nil {
				continue
			}
		}
	}
	// zk index
	for i = 0; i < len(zbfs); i++ {
		id, bfile, ifile = zids[i], zbfs[i], zifs[i]
		if _, ok = s.Volumes[id]; ok {
			continue
		}
		if _, ok = lim[id]; !ok {
			// if not exists in local
			if v, err = NewVolume(id, bfile, ifile, s.conf); err != nil {
				log.Warningf("fail recovery volume_id: %d, file: %s, index: %s", id, bfile, ifile)
				continue
			}
			s.Volumes[id] = v
		}
	}
	err = s.saveVolumeIndex()
	return
}

// parseIndex parse volume info from a index file.
func (s *Store) parseIndex(lines []string) (im map[int32]struct{}, ids []int32, bfs, ifs []string, err error) {
	var (
		id    int64
		vid   int32
		line  string
		bfile string
		ifile string
		seps  []string
	)
	im = make(map[int32]struct{})
	for _, line = range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}
		if seps = strings.Split(line, ","); len(seps) != 3 {
			log.Errorf("volume index: \"%s\" format error", line)
			err = errors.ErrStoreVolumeIndex
			return
		}
		bfile = seps[0]
		ifile = seps[1]
		if id, err = strconv.ParseInt(seps[2], 10, 32); err != nil {
			log.Errorf("volume index: \"%s\" format error", line)
			return
		}
		vid = int32(id)
		ids = append(ids, vid)
		bfs = append(bfs, bfile)
		ifs = append(ifs, ifile)
		im[vid] = struct{}{}
		log.V(1).Infof("parse volume index, id: %d, block: %s, index: %s", id, bfile, ifile)
	}
	return
}

// saveFreeVolumeIndex save free volumes index info to disk.
func (s *Store) saveFreeVolumeIndex() (err error) {
	var (
		tn, n int
		v     *Volume
	)
	if _, err = s.fvf.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	for _, v = range s.FreeVolumes {
		if n, err = s.fvf.Write(v.Meta()); err != nil {
			return
		}
		tn += n
	}
	if err = s.fvf.Sync(); err != nil {
		return
	}
	err = os.Truncate(s.conf.FreeVolumeIndex, int64(tn))
	return
}

// saveVolumeIndex save volumes index info to disk.
func (s *Store) saveVolumeIndex() (err error) {
	var (
		tn, n int
		ok    bool
		id    int32
		ids   []int32
		v     *Volume
	)
	if _, err = s.vf.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	ids = make([]int32, 0, len(s.Volumes))
	for id, v = range s.Volumes {
		ids = append(ids, id)
	}
	sort.Sort(Int32Slice(ids))
	for _, id = range ids {
		if v, ok = s.Volumes[id]; !ok {
			continue
		}
		if n, err = s.vf.Write(v.Meta()); err != nil {
			return
		}
		tn += n
	}
	if err = s.vf.Sync(); err != nil {
		return
	}
	err = os.Truncate(s.conf.VolumeIndex, int64(tn))
	return
}

// RLockVolume read lock
func (s *Store) RLockVolume() {
	s.vlock.RLock()
}

// RUnlockVolume read unlock
func (s *Store) RUnlockVolume() {
	s.vlock.RUnlock()
}

// Needle get a needle from sync.Pool.
func (s *Store) Needle() (n *needle.Needle) {
	var i interface{}
	if i = s.np.Get(); i != nil {
		n = i.(*needle.Needle)
		return
	}
	return new(needle.Needle)
}

// FreeNeedle free the needle to pool.
func (s *Store) FreeNeedle(n *needle.Needle) {
	s.np.Put(n)
}

// Buffer get a buffer from sync.Pool.
func (s *Store) Buffer(n int) (d []byte) {
	var di interface{}
	if di = s.bp[n].Get(); di != nil {
		d = di.([]byte)
		return
	}
	d = make([]byte, n*s.conf.NeedleMaxSize)
	return
}

// FreeBuffer free the buffer to pool.
func (s *Store) FreeBuffer(n int, d []byte) {
	s.bp[n].Put(d)
}

// freeFile get volume block & index free file name.
func (s *Store) freeFile(id int32, bdir, idir string) (bfile, ifile string) {
	var file = fmt.Sprintf("%s%d", freeVolumePrefix, id)
	bfile = path.Join(bdir, file)
	file = fmt.Sprintf("%s%d%s", freeVolumePrefix, id, volumeIndexPrefix)
	ifile = path.Join(idir, file)
	return
}

// file get volume block & index file name.
func (s *Store) file(id int32, bdir, idir string, i int) (bfile, ifile string) {
	var file = fmt.Sprintf("%d_%d", id, i)
	bfile = path.Join(bdir, file)
	file = fmt.Sprintf("%d_%d%s", id, i, volumeIndexPrefix)
	ifile = path.Join(idir, file)
	return
}

// fileFreeId get a file free id.
func (s *Store) fileFreeId(file string) (id int32) {
	var (
		fid    int64
		fidStr = strings.Replace(path.Base(file), freeVolumePrefix, "", -1)
	)
	fid, _ = strconv.ParseInt(fidStr, 10, 32)
	id = int32(fid)
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
		s.FreeId++
		bfile, ifile = s.freeFile(s.FreeId, bdir, idir)
		if v, err = NewVolume(volumeFreeId, bfile, ifile, s.conf); err != nil {
			break
		}
		v.Close()
		s.FreeVolumes = append(s.FreeVolumes, v)
		sn++
	}
	err = s.saveFreeVolumeIndex()
	s.lock.Unlock()
	return
}

// freeVolume get a free volume.
func (s *Store) freeVolume(id int32) (v *Volume, err error) {
	var (
		i                                        int
		bfile, nbfile, ifile, nifile, bdir, idir string
	)
	if len(s.FreeVolumes) == 0 {
		err = errors.ErrStoreNoFreeVolume
		return
	}
	v = s.FreeVolumes[0]
	s.FreeVolumes = s.FreeVolumes[1:]
	v.Id = id
	bfile, ifile = v.Block.File, v.Indexer.File
	bdir, idir = path.Dir(bfile), path.Dir(ifile)
	for {
		nbfile, nifile = s.file(id, bdir, idir, i)
		if !myos.Exist(nbfile) && !myos.Exist(nifile) {
			break
		}
		i++
	}
	log.Infof("rename block: %s to %s", bfile, nbfile)
	log.Infof("rename index: %s to %s", ifile, nifile)
	if err = os.Rename(ifile, nifile); err != nil {
		log.Errorf("os.Rename(\"%s\", \"%s\") error(%v)", ifile, nifile, err)
		v.Destroy()
		return
	}
	if err = os.Rename(bfile, nbfile); err != nil {
		log.Errorf("os.Rename(\"%s\", \"%s\") error(%v)", bfile, nbfile, err)
		v.Destroy()
		return
	}
	v.Block.File = nbfile
	v.Indexer.File = nifile
	if err = v.Open(); err != nil {
		v.Destroy()
		return
	}
	err = s.saveFreeVolumeIndex()
	return
}

// AddVolume add a new volume.
func (s *Store) AddVolume(id int32) (v *Volume, err error) {
	var nv *Volume
	s.vlock.RLock()
	if v = s.Volumes[id]; v != nil {
		err = errors.ErrVolumeExist
	}
	s.vlock.RUnlock()
	if err != nil {
		return
	}
	s.lock.Lock()
	nv, err = s.freeVolume(id)
	s.lock.Unlock()
	if err != nil {
		return
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v == nil {
		s.Volumes[id] = nv
		if err = s.saveVolumeIndex(); err == nil {
			err = s.zk.AddVolume(nv)
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	return
}

// DelVolume del the volume by volume id.
func (s *Store) DelVolume(id int32) (err error) {
	var v *Volume
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		if !v.Compact {
			delete(s.Volumes, id)
			if err = s.saveVolumeIndex(); err == nil {
				err = s.zk.DelVolume(id)
			}
		} else {
			err = errors.ErrVolumeInCompact
		}
	} else {
		err = errors.ErrVolumeNotExist
	}
	s.vlock.Unlock()
	if err == nil {
		v.Close()
		v.Destroy()
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
		s.Volumes[id] = nv
		if err = s.saveVolumeIndex(); err == nil {
			err = s.zk.AddVolume(nv)
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	return
}

// CompactVolume compact a super block to another file.
func (s *Store) CompactVolume(id int32) (err error) {
	var v, nv *Volume
	s.vlock.RLock()
	if v = s.Volumes[id]; v != nil {
		if v.Compact {
			err = errors.ErrVolumeInCompact
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.RUnlock()
	if err != nil {
		return
	}
	s.lock.Lock()
	nv, err = s.freeVolume(id)
	s.lock.Unlock()
	if err != nil {
		return
	}
	// no lock here, Compact is no side-effect
	if err = v.StartCompact(nv); err != nil {
		v.StopCompact(nil)
		return
	}
	s.vlock.Lock()
	if v = s.Volumes[id]; v != nil {
		if err = v.StopCompact(nv); err == nil {
			s.Volumes[id] = nv
			if err = s.saveVolumeIndex(); err == nil {
				err = s.zk.SetVolume(nv)
			}
		}
	} else {
		err = errors.ErrVolumeExist
	}
	s.vlock.Unlock()
	if err == nil {
		v.Close()
		v.Destroy()
	}
	return
}

// Close close the store.
// WARN the global variable store must first set nil and reject any other
// requests then safty close.
func (s *Store) Close() {
	var v *Volume
	if s.vf != nil {
		s.vf.Close()
	}
	if s.fvf != nil {
		s.fvf.Close()
	}
	for _, v = range s.Volumes {
		v.Close()
	}
	s.zk.Close()
	return
}
