package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/directory/hbase"
	"github.com/Terry-Mao/bfs/directory/hbase/filemeta"
	"github.com/Terry-Mao/bfs/directory/snowflake"
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"net/http"
	"strconv"
	"time"
)

const (
	retrySleep = time.Second * 1
)

// Directory
// id means store serverid; vid means volume id; gid means group id
type Directory struct {
	idStore   map[string]*meta.Store // store status
	idVolumes map[string][]int32     // init from getStoreVolume
	idGroup   map[string]int         // for http read

	vidVolume map[int32]*meta.VolumeState
	vidStores map[int32][]string // init from getStoreVolume    for  http Read
	gidStores map[int][]string

	genkey     *snowflake.Genkey  // snowflake client for gen key
	hbase      *hbase.HBaseClient // hbase client
	dispatcher *Dispatcher        // dispatch for write or read reqs

	config *Config
	zk     *Zookeeper
}

// NewDirectory
func NewDirectory(config *Config, zk *Zookeeper) (d *Directory, err error) {
	d = &Directory{}
	d.config = config
	d.zk = zk
	if d.genkey, err = snowflake.NewGenkey(config.SnowflakeZkAddrs, config.SnowflakeZkPath, config.SnowflakeZkTimeout, config.SnowflakeWorkId); err != nil {
		return
	}
	if err = hbase.Init(config.HbaseAddr, config.HbaseTimeout, config.HbaseMaxIdle, config.HbaseMaxActive); err != nil {
		return
	}
	d.hbase = hbase.NewHBaseClient()
	d.dispatcher = NewDispatcher(d)
	go d.SyncZookeeper()
	return
}

// Stores get all the store nodes and set a watcher
func (d *Directory) syncStores() (ev <-chan zk.Event, err error) {
	var (
		storeMeta              *meta.Store
		idStore                map[string]*meta.Store
		idVolumes              map[string][]int32
		rack, store, volume    string
		racks, stores, volumes []string
		data                   []byte
		vid                    int
	)

	if racks, ev, err = d.zk.WatchRacks(); err != nil {
		return
	}
	idStore = make(map[string]*meta.Store)
	idVolumes = make(map[string][]int32)
	for _, rack = range racks {
		if stores, err = d.zk.Stores(rack); err != nil {
			return
		}
		for _, store = range stores {
			if data, err = d.zk.Store(rack, store); err != nil {
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}
			if volumes, err = d.zk.StoreVolumes(rack, store); err != nil {
				return
			}
			idVolumes[storeMeta.Id] = []int32{}
			for _, volume = range volumes {
				if vid, err = strconv.Atoi(volume); err != nil {
					log.Errorf("wrong volume:%s", volume)
					continue
				}
				idVolumes[storeMeta.Id] = append(idVolumes[storeMeta.Id], int32(vid))
			}
			idStore[storeMeta.Id] = storeMeta
		}
	}
	d.idStore = idStore
	d.idVolumes = idVolumes
	return
}

// Volumes get all volumes in zk
func (d *Directory) syncVolumes() (err error) {
	var (
		volumeState     *meta.VolumeState
		vidVolume       map[int32]*meta.VolumeState
		vidStores       map[int32][]string
		volume          string
		vid             int
		volumes, stores []string
		data            []byte
	)
	if volumes, err = d.zk.Volumes(); err != nil {
		return
	}
	vidVolume = make(map[int32]*meta.VolumeState)
	vidStores = make(map[int32][]string)
	for _, volume = range volumes {
		if data, err = d.zk.Volume(volume); err != nil {
			return
		}
		volumeState = new(meta.VolumeState)
		if err = json.Unmarshal(data, volumeState); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
		if vid, err = strconv.Atoi(volume); err != nil {
			log.Errorf("wrong volume:%s", volume)
			continue
		}
		vidVolume[int32(vid)] = volumeState
		if stores, err = d.zk.VolumeStores(volume); err != nil {
			return
		}
		vidStores[int32(vid)] = stores
	}
	d.vidVolume = vidVolume
	d.vidStores = vidStores
	return
}

// Groups get all groups and set a watcher
func (d *Directory) syncGroups() (ev <-chan zk.Event, err error) {
	var (
		gidStores      map[int][]string
		idGroup        map[string]int
		group, store   string
		gid            int
		groups, stores []string
	)
	if groups, ev, err = d.zk.WatchGroups(); err != nil {
		return
	}
	gidStores = make(map[int][]string)
	idGroup = make(map[string]int)
	for _, group = range groups {
		if stores, err = d.zk.GroupStores(group); err != nil {
			return
		}
		if gid, err = strconv.Atoi(group); err != nil {
			log.Errorf("wrong group:%s", group)
			continue
		}
		gidStores[gid] = stores
		for _, store = range stores {
			idGroup[store] = gid
		}
	}
	d.gidStores = gidStores
	d.idGroup = idGroup
	return
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		sev <-chan zk.Event
		gev <-chan zk.Event
		err error
	)
	for {
		if sev, err = d.syncStores(); err != nil {
			log.Errorf("syncStores() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if gev, err = d.syncGroups(); err != nil {
			log.Errorf("syncGroups() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.syncVolumes(); err != nil {
			log.Errorf("syncVolumes() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.dispatcher.Update(); err != nil {
			log.Errorf("Update() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
	selectBack:
		select {
		case <-sev:
			log.Infof("stores status change or new store")
			break
		case <-gev:
			log.Infof("new group")
			break
		case <-time.After(d.config.PullInterval):
			if err = d.syncVolumes(); err != nil {
				log.Errorf("syncVolumes() called error(%v)", err)
			} else {
				if err = d.dispatcher.Update(); err != nil {
					log.Errorf("Update() called error(%v)", err)
				}
			}
			goto selectBack
		}
	}
}

// cookie  rand uint16
func (d *Directory) cookie() (cookie int32) {
	return int32(uint16(time.Now().UnixNano()))
}

// Rstores get readable stores for http get
func (d *Directory) Rstores(key int64, cookie int32) (res Response, ret int, err error) {
	var (
		f *filemeta.File
	)
	ret = http.StatusOK
	if f, err = d.hbase.Get(key); err != nil {
		return
	}
	if f == nil {
		ret = http.StatusNotFound
		return
	}
	if f.Cookie != cookie {
		ret = http.StatusBadRequest
		return
	}
	res.Vid = f.Vid
	if res.Stores, err = d.dispatcher.RStores(res.Vid); err != nil {
		return
	}
	if len(res.Stores) == 0 {
		ret = http.StatusInternalServerError
	}
	return
}

// Wstores get writable stores for http upload
func (d *Directory) Wstores(numKeys int) (res Response, ret int, err error) {
	var (
		i    int
		key  int64
		keys []int64
		f    filemeta.File
	)
	ret = http.StatusOK
	if numKeys > d.config.MaxNum {
		ret = http.StatusBadRequest
		return
	}
	if res.Stores, res.Vid, err = d.dispatcher.WStores(); err != nil {
		return
	}
	if len(res.Stores) == 0 {
		ret = http.StatusInternalServerError
		return
	}
	keys = make([]int64, numKeys)
	for i = 0; i < numKeys; i++ {
		if key, err = d.genkey.Getkey(); err != nil {
			return
		}
		keys[i] = key
	}
	res.Keys = keys
	res.Cookie = d.cookie()
	for _, key = range keys {
		f.Key = key
		f.Vid = res.Vid
		f.Cookie = res.Cookie
		if err = d.hbase.Put(&f); err != nil {
			return //puted keys will be ignored
		}
	}
	return
}

// Dstores get delable stores for http del
func (d *Directory) Dstores(key int64, cookie int32) (res Response, ret int, err error) {
	var (
		f *filemeta.File
	)
	ret = http.StatusOK
	if f, err = d.hbase.Get(key); err != nil {
		return
	}
	if f == nil {
		ret = http.StatusNotFound
		return
	}
	if f.Cookie != cookie {
		ret = http.StatusBadRequest
		return
	}
	if err = d.hbase.Del(key); err != nil {
		return
	}
	res.Vid = f.Vid
	if res.Stores, err = d.dispatcher.DStores(res.Vid); err != nil {
		return
	}
	if len(res.Stores) == 0 {
		ret = http.StatusInternalServerError
	}
	return
}
