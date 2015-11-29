package main

// Directory
// id means store serverid; vid means volume id; gid means group id
type Directory struct {
	idStore          map[string]*meta.Store // store status
	idVolumes        map[string][]string    // init from getStoreVolume

	vidVolume        map[string]*meta.Volume
	vidStores        map[string][]string    // init from getStoreVolume    for  http Read

	gidStores        map[string][]string

	genkey           *Genkey
	//hbase client

	config           *Config
	zk               *Zookeeper
}

// NewDirectory
func NewDirectory(config *Config, zk *Zookeeper) d *Directory {
	d = &Directory{}
	d.config = config
	d.zk = zk
}

// watchStores get all the store nodes and set up the watcher in the zookeeper
func (d *Directory) watchStores() (ev <-chan zk.Event, err error) {
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
func (d *Directory) syncVolumes() err error {
	var (
		volumeMeta     *meta.Volume
		vidVolume      map[string]*meta.Volume
		volume,store   string
		volumes,stores []string
		vidStores      map[string][]string
		data           []byte
	)
	if volumes, _, err = d.zk.Volumes(); err != nil {
		return
	}
	vidVolume = make(map[string]*meta.Volume)
	vidStores = make(map[string][]string)
	for _, volume = range volumes {
		if data, _, err = d.zk.Volume(volume); err != nil {
			return
		}
		volumeMeta = new(meta.Volume)
		if err = json.Unmarshal(data, volumeMeta); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
		vidVolume[volume] = volumeMeta
		if stores, err = d.zk.VolumeStores(volume); err != nil {
			return
		}
		vidStores[volume] = stores
	}
	d.vidVolume = vidVolume
	d.vidStores = vidStores
	return
}

// Groups get all groups
func (d *Directory) watchGroups() (ev <-chan zk.Event, err error) {
	var (
		gidStores     map[int][]string
		group         string
		groups,stores []string
		data          []byte
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
	}
	d.gidStores = gidStores
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		storeChanges     <-chan zk.Event
		groupChanges     <-chan zk.Event
		err              error
	)
	for {
		if storeChanges, err = d.watchStores(); err != nil {
			log.Errorf("watchStores() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if groupChanges, err = d.watchGroups(); err != nil {
			log.Errorf("watchGroups() called error(%v)", err)
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
			if err = d.syncVolumes(); err != nil {
				log.Errorf("syncVolumes() called error(%v)", err)
			}
			goto selectBack
		}
	}
}

//
