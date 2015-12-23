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
	// STORE
	store       map[string]*meta.Store // store_server_id:store_info
	storeVolume map[string][]int32     // store_server_id:volume_ids

	// GROUP
	storeGroup map[string]int   // store_server_id:group
	group      map[int][]string // group_id:store_servers

	// VOLUME
	volume      map[int32]*meta.VolumeState // volume_id:volume_state
	volumeStore map[int32][]string          // volume_id:store_server_id

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
	d.dispatcher = NewDispatcher()
	go d.SyncZookeeper()
	return
}

// Stores get all the store nodes and set a watcher
func (d *Directory) syncStores() (ev <-chan zk.Event, err error) {
	var (
		storeMeta              *meta.Store
		store                  map[string]*meta.Store
		storeVolume            map[string][]int32
		rack, store, volume    string
		racks, stores, volumes []string
		data                   []byte
		vid                    int
	)
	// get all rack
	if racks, ev, err = d.zk.WatchRacks(); err != nil {
		return
	}
	store = make(map[string]*meta.Store)
	storeVolume = make(map[string][]int32)
	for _, rack = range racks {
		// get all stores in the rack
		if stores, err = d.zk.Stores(rack); err != nil {
			return
		}
		for _, store = range stores {
			// get store
			if data, err = d.zk.Store(rack, store); err != nil {
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}
			// get all volumes in the store
			if volumes, err = d.zk.StoreVolumes(rack, store); err != nil {
				return
			}
			storeVolume[storeMeta.Id] = []int32{}
			for _, volume = range volumes {
				if vid, err = strconv.Atoi(volume); err != nil {
					log.Errorf("wrong volume:%s", volume)
					continue
				}
				storeVolume[storeMeta.Id] = append(storeVolume[storeMeta.Id], int32(vid))
			}
			store[storeMeta.Id] = storeMeta
		}
	}
	d.store = store
	d.storeVolume = storeVolume
	return
}

// Volumes get all volumes in zk
func (d *Directory) syncVolumes() (err error) {
	var (
		vid             int
		str             string
		volumes, stores []string
		data            []byte
		volumeState     *meta.VolumeState
		volume          map[int32]*meta.VolumeState
		volumeStore     map[int32][]string
	)
	// get all volumes
	if volumes, err = d.zk.Volumes(); err != nil {
		return
	}
	volume = make(map[int32]*meta.VolumeState)
	volumeStore = make(map[int32][]string)
	for _, str = range volumes {
		// get the volume
		if data, err = d.zk.Volume(str); err != nil {
			return
		}
		volumeState = new(meta.VolumeState)
		if err = json.Unmarshal(data, volumeState); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
		if vid, err = strconv.Atoi(str); err != nil {
			log.Errorf("wrong volume:%s", str)
			continue
		}
		volume[int32(vid)] = volumeState
		// get the stores by the volume
		if stores, err = d.zk.VolumeStores(str); err != nil {
			return
		}
		volumeStore[int32(vid)] = stores
	}
	d.volume = volume
	d.volumeStore = volumeStore
	return
}

// syncGroups get all groups and set a watcher.
func (d *Directory) syncGroups() (err error) {
	var (
		gid            int
		str            string
		groups, stores []string
		group          map[int][]string
		storeGroup     map[string]int
	)
	// get all groups
	if groups, err = d.zk.Groups(); err != nil {
		return
	}
	group = make(map[int][]string)
	storeGroup = make(map[string]int)
	for _, str = range groups {
		// get all stores by the group
		if stores, err = d.zk.GroupStores(str); err != nil {
			return
		}
		if gid, err = strconv.Atoi(str); err != nil {
			log.Errorf("wrong group:%s", str)
			continue
		}
		group[gid] = stores
		for _, str = range stores {
			storeGroup[str] = gid
		}
	}
	d.group = group
	d.storeGroup = storeGroup
	return
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		sev <-chan zk.Event
		err error
	)
	for {
		if sev, err = d.syncStores(); err != nil {
			log.Errorf("syncStores() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if err = d.syncGroups(); err != nil {
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
		select {
		case <-sev:
			log.Infof("stores status change or new store")
			break
		case <-time.After(d.config.PullInterval):
			log.Infof("pull from zk")
			break
		}
	}
}

// TODO move cookie  rand uint16
func (d *Directory) cookie() (cookie int32) {
	return int32(uint16(time.Now().UnixNano())) + 1
}

// ReadStores get readable stores for http get
func (d *Directory) ReadStores(key int64, cookie int32) (res Response, ret int, err error) {
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

// Writestores get writable stores for http upload
func (d *Directory) WriteStores(numKeys int) (res Response, ret int, err error) {
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

// DelStores get delable stores for http del
func (d *Directory) DelStores(key int64, cookie int32) (res Response, ret int, err error) {
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

/*
// WStores get suitable stores for writing
func (d *Dispatcher) WStores() (hosts []string, vid int32, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		gid       int
		index     int
		r         *rand.Rand
		index     int
		ok        bool
	)
	if len(d.gids) == 0 {
		return nil, 0, errors.New(fmt.Sprintf("no available gid"))
	}
	r = d.rp.Get().(*rand.Rand)
	defer d.rp.Put(r)
	gid = d.gids[r.Intn(len(d.gids))]
	stores = d.dr.gidStores[gid]
	if len(stores) > 0 {
		store = stores[0]
		index = d.r.Intn(len(d.dr.idVolumes[store]))
		vid = int32(d.dr.idVolumes[store][index])
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			return nil, 0, errors.New(fmt.Sprintf("bad store : %s", store))
		}
		hosts = append(hosts, storeMeta.Api)
	}
	return
}

// RStores get suitable stores for reading
func (d *Dispatcher) RStores(vid int32) (hosts []string, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		ok        bool
	)
	hosts = []string{}
	if stores, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New(fmt.Sprintf("vidStores cannot match vid: %s", vid))
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			continue
		}
		if storeMeta.Status != meta.StoreStatusFail {
			hosts = append(hosts, storeMeta.Api)
		}
	}
	return
}

// DStores get suitable stores for delete
func (d *Dispatcher) DStores(vid int32) (hosts []string, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		ok        bool
	)
	hosts = []string{}
	if stores, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New(fmt.Sprintf("vidStores cannot match vid: %s", vid))
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			continue
		}
		if storeMeta.Status == meta.StoreStatusFail {
			return nil, errors.New(fmt.Sprintf("bad store : %s", store))
		}
		hosts = append(hosts, storeMeta.Api)
	}
	return
}
*/
