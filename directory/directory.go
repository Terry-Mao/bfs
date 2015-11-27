package main

// Directory
type Directory struct {
	storesWritable   StoreStateList        //可读可写store列表
	storesReadable   StoreStateList        //只读stores列表
	storesMeta       StoreList             //所有store节点
	
	idStore          map[string]*meta.Store
	volumeStore      map[int]*meta.Store
	storeVolumes     map[string][]int

	config           *Config
	zk               *Zookeeper
}

// updateStoreState watch zk and update store state
func (d *Directory)updateStoreState() err error {
	//update  storesWritable  storesReadable  storesAll
}

// updateStoreStatus timer get store statement from zookeeper 
func (d *Directory)updateStoreStatus() err error {
	//
}