package zk

import (
	"bfs/libs/meta"
	"bfs/pitchfork/conf"
	"encoding/json"
	"fmt"
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
	if z.c, s, err = zk.Connect(config.Zookeeper.Addrs, config.Zookeeper.Timeout.Duration); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", config.Zookeeper.Addrs, err)
		return
	}
	z.config = config
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

// NewNode create pitchfork node in zk.
func (z *Zookeeper) NewNode(fpath string) (node string, err error) {
	if node, err = z.c.Create(path.Join(fpath, "")+"/", []byte(""), int32(zk.FlagEphemeral|zk.FlagSequence), zk.WorldACL(zk.PermAll)); err != nil {
		log.Errorf("zk.Create error(%v)", err)
	} else {
		node = path.Base(node)
	}
	return
}

// setRoot update root.
func (z *Zookeeper) setRoot() (err error) {
	if _, err = z.c.Set(z.config.Zookeeper.StoreRoot, []byte(""), -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// SetStore update store status.
func (z *Zookeeper) SetStore(s *meta.Store) (err error) {
	var (
		data  []byte
		store = &meta.Store{}
		spath = path.Join(z.config.Zookeeper.StoreRoot, s.Rack, s.Id)
	)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
		return
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, store); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
	}
	store.Status = s.Status
	if data, err = json.Marshal(store); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return err
	}
	if _, err = z.c.Set(spath, data, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
		return
	}
	err = z.setRoot()
	return
}

// WatchPitchforks watch pitchfork nodes.
func (z *Zookeeper) WatchPitchforks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.config.Zookeeper.PitchforkRoot); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.config.Zookeeper.PitchforkRoot, err)
	}
	return
}

// WatchRacks watch the rack nodes.
func (z *Zookeeper) WatchRacks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.config.Zookeeper.StoreRoot); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.config.Zookeeper.StoreRoot, err)
	}
	return
}

// Stores get all stores from a rack.
func (z *Zookeeper) Stores(rack string) (stores []string, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack)
	if stores, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get a store node data.
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.config.Zookeeper.StoreRoot, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// SetVolumeStat set volume stat
func (z *Zookeeper) SetVolumeState(volume *meta.Volume) (err error) {
	var (
		d      []byte
		spath  string
		vstate = &meta.VolumeState{
			TotalWriteProcessed: volume.Stats.TotalWriteProcessed,
			TotalWriteDelay:     volume.Stats.TotalWriteDelay,
		}
	)
	vstate.FreeSpace = volume.Block.FreeSpace()
	spath = path.Join(z.config.Zookeeper.VolumeRoot, fmt.Sprintf("%d", volume.Id))
	if d, err = json.Marshal(vstate); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(spath, d, -1); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
