package main

import (
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"time"
)

type Zookeeper struct {
	c              *zk.Conn
	storeRootPath  string
	volumeRootPath string
	groupRootPath  string
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(addrs []string, timeout time.Duration, storeRootPath, volumeRootPath, groupRootPath string) (z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	if z.c, s, err = zk.Connect(addrs, timeout); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", addrs, err)
		return
	}
	z.storeRootPath = storeRootPath
	z.volumeRootPath = volumeRootPath
	z.groupRootPath = groupRootPath
	go func() {
		var e zk.Event
		for {
			if e = <-s; e.Type == 0 {
				return
			}
			log.Infof("zookeeper get a event: %s", e.State.String())
		}
	}()
	return
}

// WatchRacks get all racks and watch
func (z *Zookeeper) WatchRacks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.storeRootPath); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.storeRootPath, err)
	}
	return
}

// Stores get all stores
func (z *Zookeeper) Stores(rack string) (nodes []string, err error) {
	var spath = path.Join(z.storeRootPath, rack)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get store node data
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.storeRootPath, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// StoreVolumes get volumes of store
func (z *Zookeeper) StoreVolumes(rack, store string) (nodes []string, err error) {
	var spath = path.Join(z.storeRootPath, rack, store)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Volumes get all volumes
func (z *Zookeeper) Volumes() (nodes []string, err error) {
	if nodes, _, err = z.c.Children(z.volumeRootPath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.volumeRootPath, err)
	}
	return
}

// Volume get volume node data
func (z *Zookeeper) Volume(volume string) (data []byte, err error) {
	var spath = path.Join(z.volumeRootPath, volume)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// VolumeStores get stores of volume
func (z *Zookeeper) VolumeStores(volume string) (nodes []string, err error) {
	var spath = path.Join(z.volumeRootPath, volume)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// Groups get all groups and watch
func (z *Zookeeper) WatchGroups() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.groupRootPath); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.groupRootPath, err)
	}
	return
}

// GroupStores get stores of group
func (z *Zookeeper) GroupStores(group string) (nodes []string, err error) {
	var spath = path.Join(z.groupRootPath, group)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
