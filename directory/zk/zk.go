package zk

import (
	"bfs/directory/conf"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"path"
)

type Zookeeper struct {
	c      *zk.Conn
	config *conf.Config
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(config *conf.Config) (z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	z.config = config
	if z.c, s, err = zk.Connect(config.Zookeeper.Addrs, config.Zookeeper.Timeout.Duration); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", config.Zookeeper.Addrs, err)
		return
	}
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
	if _, _, ev, err = z.c.GetW(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.GetW(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
		return
	}
	if nodes, _, err = z.c.Children(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// Stores get all stores
func (z *Zookeeper) Stores(rack string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get store node data
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// StoreVolumes get volumes of store
func (z *Zookeeper) StoreVolumes(rack, store string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Volumes get all volumes
func (z *Zookeeper) Volumes() (nodes []string, err error) {
	if nodes, _, err = z.c.Children(z.config.Zookeeper.VolumeRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.VolumeRoot, err)
	}
	return
}

// Volume get volume node data
func (z *Zookeeper) Volume(volume string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.VolumeRoot, volume)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// VolumeStores get stores of volume
func (z *Zookeeper) VolumeStores(volume string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.VolumeRoot, volume)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// Groups get all groups and watch
func (z *Zookeeper) Groups() (nodes []string, err error) {
	if nodes, _, err = z.c.Children(z.config.Zookeeper.GroupRoot); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.config.Zookeeper.GroupRoot, err)
	}
	return
}

// GroupStores get stores of group
func (z *Zookeeper) GroupStores(group string) (nodes []string, err error) {
	var spath = path.Join(z.config.Zookeeper.GroupRoot, group)
	if nodes, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
