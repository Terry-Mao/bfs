package main

// Directory
type Directory struct {
	storesWritable   StoreStateList        //可读可写store列表 init from updateStoreState
	storesReadable   StoreStateList        //只读stores列表 init from updateStoreState
	storesMeta       StoreList             //所有store节点 init from watchGetStores and updateStoreStatus

	idStore          map[string]*meta.Store //init from updateStoreStatus
	volumeStore      map[int]*meta.Store    //init from getStoreVolume
	storeVolumes     map[string][]int       //init from getStoreVolume

	genkey           *Genkey
	//hbase client

	config           *Config
	zk               *Zookeeper
}

// NewDirectory
func NewDirectory() d *Directory {
	d = &Directory{}

}

func (d *Directory) init() {

}

// watchGetStores get all the store nodes and set up the watcher in the zookeeper
func (d *Directory) watchGetStores() (storeChanges <-chan zk.Event, err error) {
	var (
		storeMeta           StoreList
		storeRootPath       string
		children, children1 []string
		data                []byte
		store               = &meta.Store{}
	)

	storeRootPath = d.config.ZookeeperStoreRoot
	if _, _, storeChanges, err = d.zk.c.GetW(storeRootPath); err != nil {
		log.Errorf("zk.GetW(\"%s\") error(%v)", storeRootPath, err)
		return
	}
	if children, _, err = d.zk.c.Children(storeRootPath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", storeRootPath, err)
		return
	}
	storeMeta = make(StoreList, 0)
	for _, child := range children {
		pathRack := fmt.Sprintf("%s/%s", storeRootPath, child)
		if children1, _, err = d.zk.c.Children(pathRack); err != nil {
			log.Errorf("zk.Children(\"%s\") error(%v)", pathRack, err)
			return
		}
		for _, child1 := range children1 {
			pathStore := fmt.Sprintf("%s/%s", pathRack, child1)
			if data, _, err = d.zk.c.Get(pathStore); err != nil {
				log.Errorf("zk.Get(\"%s\") error(%v)", pathStore, err)
				return
			}
			if err = json.Unmarshal(data, store); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}

			storeMeta = append(storeMeta, store)
		}
	}
	d.storesMeta = storeMeta
	return
}

// updateStoreStatus timer get store statement from zookeeper 
func (d *Directory) updateStoreStatus() err error {
	var (
		store          *meta.Store
		storeState     =&StoreState{}
		idStore        =make(map[string]*meta.Store)
		storesWritable =make(StoreStateList, 0)
		storesReadable =make(StoreStateList, 0)
	)
	for _, store = range d.storesMeta {
		idStore[store.Id] = store
		storeState.Id = store.Id
		switch store.Status {
		case meta.StoreStatusHealth:
			storesWritable = append(storesWritable, storeState)
		case meta.StoreStatusRead:
			storesReadable = append(storesReadable, storeState)
		case meta.StoreStatusEnable:
			log.Warnf("store not online %s status:%d", store.Stat, store.Status)
		default:
			log.Errorf("unknonw status  %s  status:%d", store.Stat, store.Status)
		}
	}
	d.idStore = idStore
	d.storesWritable = storesWritable
	d.storesReadable = storesReadable
}

// getStoreVolumes get all volumes in zk
func (d *Directory) getStoreVolumes() err error {

}

// updateStoreState watch zk and update store state
func (d *Directory) updateStoreState() err error {
	//update  storesWritable  storesReadable  storesAll
}

// SyncZookeeper Synchronous zookeeper data to memory
func (d *Directory) SyncZookeeper() {
	var (
		storeChanges     <-chan zk.Event
		err              error
	)
	for {
		if storeChanges, err = d.WatchGetStores(); err != nil {
			log.Errorf("watchGetStores() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if err = d.updateStoreStatus(); err != nil {
			log.Errorf("updateStoreStatus() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		select {
		case <-storeChanges:
			log.Infof("stores status change or new store")
			break
		case <-time.After(d.config.PullInterval):
			log.Infof("stores state need sync")
			if err = d.getStoreVolumes(); err != nil {
				log.Errorf("getStoreVolumes() called error(%v)", err)
			}
			if err = d.updateStoreState(); err != nil {
				log.Errorf("updateStoreState() called error(%v)", err)
			}
		}
	}
}