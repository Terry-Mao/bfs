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

	genkey           *Genkey
	hbase            *HBaseClient

	config           *Config
	zk               *Zookeeper
}

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
	return
}

// Stores get all the store nodes and set a watcher
func (d *Directory) stores() (ev <-chan zk.Event, err error) {
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
func (d *Directory) volumes() (err error) {
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
func (d *Directory) groups() (ev <-chan zk.Event, err error) {
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
		if storeChanges, err = d.stores(); err != nil {
			log.Errorf("Stores() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if groupChanges, err = d.groups(); err != nil {
			log.Errorf("Groups() called error(%v)", err)
			time.Sleep(! * time.Second)
			continue
		}
	selectBack:
		select {
		case <-storeChanges:
			log.Infof("stores status change or new store")
			break
		case <-groupChanges:
			log.Infof("new group")
			break
		case <-time.After(d.config.PullInterval):
			if err = d.volumes(); err != nil {
				log.Errorf("syncVolumes() called error(%v)", err)
			}
			goto selectBack
		}
	}
}
