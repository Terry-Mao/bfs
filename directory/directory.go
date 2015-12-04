package main
import (
	"github.com/Terry-Mao/bfs/directory/hbase"
	"github.com/Terry-Mao/bfs/directory/snowflake"
	log "github.com/golang/glog"
)

// Directory
// id means store serverid; vid means volume id; gid means group id
type Directory struct {
	idStore          map[string]*meta.Store // store status
	idVolumes        map[string][]string    // init from getStoreVolume
	idGroup          map[string]string      // for http read

	vidVolume        map[string]*meta.Volume
	vidStores        map[string][]string    // init from getStoreVolume    for  http Read
	gidStores        map[string][]string

	genkey           *Genkey                // snowflake client for gen key
	hbase            *HBaseClient           // hbase client
	dispatcher       *Dispatcher            // dispatch for write or read reqs

	config           *Config
	zk               *Zookeeper
}

// 

// NewDirectory
func NewDirectory(config *Config, zk *Zookeeper) (d *Directory, err error) {
	d = &Directory{}
	d.config = config
	d.zk = zk
	if d.genkey, err = NewGenkey(config.SfZookeeperAddrs, config.SfZookeeperPath, config.SfZookeeperTimeout, config.SfWorkId); err != nil {
		return nil, err
	}
	if err = hbase.Init(config.HbaseAddr, config.HbaseTimeout, config.HbaseMaxIdle, config.HbaseMaxActive); err != nil {
		return nil, err
	}
	d.hbase = hbase.NewHBaseClient()
	dispatcher = NewDispatcher(d)
	return
}

// Stores get all the store nodes and set a watcher
func (d *Directory) syncStores() (ev <-chan zk.Event, err error) {
	var (
		storeMeta              *meta.Store
		idStore                map[string]*meta.Store
		idVolumes              map[string][]string
		rack, store            string
		racks, stores, volumes []string
		data                   []byte
	)

	if racks, ev, err = d.zk.WatchRacks(); err != nil {
		return
	}
	idVolumes = make(map[string][]string)
	idStore = make(map[string]*meta.Store)
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
			idVolumes[storeMeta.Id] = volumes
			idStore[storeMeta.Id] = storeMeta
		}
	}
	d.idVolumes = idVolumes
	d.idStore = idStore
	return
}

// Volumes get all volumes in zk
func (d *Directory) syncVolumes() (err error) {
	var (
		volumeState    *meta.VolumeState
		vidVolume      map[string]*meta.VolumeState
		volume,store   string
		volumes,stores []string
		vidStores      map[string][]string
		data           []byte
	)
	if volumes, err = d.zk.Volumes(); err != nil {
		return
	}
	vidVolume = make(map[string]*meta.VolumeState)
	vidStores = make(map[string][]string)
	for _, volume = range volumes {
		if data, err = d.zk.Volume(volume); err != nil {
			return
		}
		volumeState = new(meta.VolumeState)
		if err = json.Unmarshal(data, volumeState); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
		vidVolume[volume] = volumeState
		if stores, err = d.zk.VolumeStores(volume); err != nil {
			return
		}
		vidStores[volume] = stores
	}
	d.vidVolume = vidVolume
	d.vidStores = vidStores
	return
}

// Groups get all groups and set a watcher
func (d *Directory) syncGroups() (ev <-chan zk.Event, err error) {
	var (
		gidStores,idGroup map[int][]string
		group,store       string
		groups,stores     []string
		data              []byte
	)
	if groups, ev, err = d.zk.WatchGroups()(); err != nil {
		return
	}
	groupsMeta = make(map[int][]string)
	for _, group = range groups {
		if stores, err = d.zk.GroupStores(group); err != nil {
			return
		}
		gidStores[group] = stores
		for _,store = range stores {
			idGroup[store] = group
		}
	}
	d.gidStores = gidStores
	d.idGroup = idGroup
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		storeChanges     <-chan zk.Event
		groupChanges     <-chan zk.Event
		err              error
	)
	for {
		if storeChanges, err = d.syncStores(); err != nil {
			log.Errorf("Stores() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if groupChanges, err = d.syncGroups(); err != nil {
			log.Errorf("Groups() called error(%v)", err)
			time.Sleep(! * time.Second)
			continue
		}
		d.dispatcher.Update()
	selectBack:
		select {
		case <-storeChanges:
			log.Infof("stores status change or new store")
			break
		case <-groupChanges:
			log.Infof("new group")
			break
		case <-time.After(d.config.PullInterval):
			if err = d.syncVolumes(); err != nil {
				log.Errorf("syncVolumes() called error(%v)", err)
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
func (d *Directory) Rstores(key int64, cookie int32) (hosts []string, vid int32, ret int, err error) {
	var (
		m   *meta.Meta
	)
	ret = http.StatusOK
	if m ,err = d.hbase.Get(key); err != nil {
		return
	}
	if m == nil {
		ret = http.StatusNotFound
		return
	}
	if m.Cookie != cookie {
		ret = http.StatusBadRequest
		return
	}
	vid = m.Vid
	if hosts, err = d.dispatcher.RStores(vid); err != nil {
		return
	}
	if len(hosts) == 0 {
		ret = http.StatusInternalServerError
	}
	return
}

// Wstores get writable stores for http upload
func (d *Directory) Wstores(numKeys int) (keys []int64, vid, cookie int32, hosts []string, ret int, err error) {
	var (
		m    *meta.Meta
		i    int
		key  int64
	)
	ret = http.StatusOK
	if numKeys > d.config.configBatchMaxNum {
		ret = http.StatusBadRequest
		return
	}
	if hosts, vid, err = d.dispatcher.WStores(); err != nil {
		return
	}
	keys = make([]int64, numKeys)
	for i=0; i<numKeys; i++ {
		if key, err = d.genkey.Getkey(); err != nil {
			return
		}
		keys[i] = key
	}
	cookie = d.cookie()
	return
}

// Dstores get delable stores for http del
func (d *Directory) Dstores(key int64, cookie int32) (hosts []string, vid int32, ret int, err error) {
	var (
		m    *meta.Meta
	)
	ret = http.StatusOK
	if m, err = d.hbase.Get(key); err != nil {
		return
	}
	if m == nil {
		ret = http.StatusNotFound
		return
	}
	if m.Cookie != cookie {
		ret = http.StatusBadRequest
		return
	}
	if err = d.hbase.Del(key); err != nil {
		return
	}
	vid = m.Vid
	if hosts, err = d.dispatcher.DStores(vid); err != nil {
		return
	}
	return
}
