package main
import (
	"github.com/Terry-Mao/bfs/directory/hbase"
	"github.com/Terry-Mao/bfs/directory/snowflake"
	log "github.com/golang/glog"
	"strconv"
)

const (
	retrySleep = time.Second * 1
)

// Directory
// id means store serverid; vid means volume id; gid means group id
type Directory struct {
	idStore          map[string]*meta.Store // store status
	idVolumes        map[string][]int32    // init from getStoreVolume
	idGroup          map[string]int32      // for http read

	vidVolume        map[int32]*meta.Volume
	vidStores        map[int32][]string    // init from getStoreVolume    for  http Read
	gidStores        map[int32][]string

	genkey           *Genkey                // snowflake client for gen key
	hbase            *HBaseClient           // hbase client
	dispatcher       *Dispatcher            // dispatch for write or read reqs

	config           *Config
	zk               *Zookeeper
}

// NewDirectory
func NewDirectory(config *Config, zk *Zookeeper) (d *Directory, err error) {
	d = &Directory{}
	d.config = config
	d.zk = zk
	if d.genkey, err = NewGenkey(config.SnowflakeZkAddrs, config.SnowflakeZkPath, config.SnowflakeZkTimeout, config.SnowflakeWorkId); err != nil {
		return
	}
	if err = hbase.Init(config.HbaseAddr, config.HbaseTimeout, config.HbaseMaxIdle, config.HbaseMaxActive); err != nil {
		return
	}
	d.hbase = hbase.NewHBaseClient()
	d.dispatcher = NewDispatcher(d)
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
			idVolumes[storeMeta.Id] = make([]int32)
			for _, volume = range volumes {
				idVolumes[storeMeta.Id] = append(idVolumes[storeMeta.Id], int32(strconv.Atoi(volume)))
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
		volumeState    *meta.VolumeState
		vidVolume      map[int32]*meta.VolumeState
		vidStores      map[int32][]string
		volume,store   string
		vid            int32
		volumes,stores []string
		data           []byte
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
		vid = int32(strconv.Atoi(volume))
		vidVolume[vid] = volumeState
		if stores, err = d.zk.VolumeStores(volume); err != nil {
			return
		}
		vidStores[vid] = stores
	}
	d.vidVolume = vidVolume
	d.vidStores = vidStores
	return
}

// Groups get all groups and set a watcher
func (d *Directory) syncGroups() (ev <-chan zk.Event, err error) {
	var (
		gidStores         map[int32][]string
		idGroup           map[string][]int32
		group,store       string
		gid               int32
		groups,stores     []string
		data              []byte
	)
	if groups, ev, err = d.zk.WatchGroups()(); err != nil {
		return
	}
	gidStores = make(map[int32][]string)
	idGroup = make(map[string][]int32)
	for _, group = range groups {
		if stores, err = d.zk.GroupStores(group); err != nil {
			return
		}
		gid = int32(strconv.Atoi(group))
		gidStores[gid] = stores
		for _,store = range stores {
			idGroup[store] = gid
		}
	}
	d.gidStores = gidStores
	d.idGroup = idGroup
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		sev     <-chan zk.Event
		gev     <-chan zk.Event
		err              error
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
		d.dispatcher.Update()
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
				d.dispatcher.Update()
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
		f   *meta.File
	)
	ret = http.StatusOK
	if f ,err = d.hbase.Get(key); err != nil {
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
	vid = f.Vid
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
		f    *meta.File
		i    int
		key  int64
	)
	ret = http.StatusOK
	if numKeys > d.config.maxnum {
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
		f    *meta.File
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
	vid = f.Vid
	if hosts, err = d.dispatcher.DStores(vid); err != nil {
		return
	}
	return
}
