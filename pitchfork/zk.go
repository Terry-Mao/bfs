package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/libs/meta"
	"github.com/Terry-Mao/bfs/libs/stat"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"time"
	"fmt"
)

type Zookeeper struct {
	c                 *zk.Conn
	storeRootPath     string
	pitchforkRootPath string
	volumeRootPath    string
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(addrs []string, timeout time.Duration, pitchforkRootPath string, storeRootPath string, volumeRootPath string) (
	z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	if z.c, s, err = zk.Connect(addrs, timeout); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", addrs, err)
		return
	}
	z.storeRootPath = storeRootPath
	z.pitchforkRootPath = pitchforkRootPath
	z.volumeRootPath = volumeRootPath
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
	var stat *zk.Stat
	if _, stat, err = z.c.Get(z.storeRootPath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.storeRootPath, err)
		return
	}
	if _, err = z.c.Set(z.storeRootPath, []byte(""), stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.storeRootPath, err)
	}
	return
}

// SetStore update store status.
func (z *Zookeeper) SetStore(s *meta.Store) (err error) {
	var (
		data  []byte
		stat  *zk.Stat
		store = &meta.Store{}
		spath = path.Join(z.storeRootPath, s.Rack, s.Id)
	)
	if data, stat, err = z.c.Get(spath); err != nil {
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
	if _, err = z.c.Set(spath, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
		return
	}
	err = z.setRoot()
	return
}

// WatchPitchforks watch pitchfork nodes.
func (z *Zookeeper) WatchPitchforks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.pitchforkRootPath); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.pitchforkRootPath, err)
	}
	return
}

// WatchRacks watch the rack nodes.
func (z *Zookeeper) WatchRacks() (nodes []string, ev <-chan zk.Event, err error) {
	if nodes, _, ev, err = z.c.ChildrenW(z.storeRootPath); err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", z.storeRootPath, err)
	}
	return
}

// Stores get all stores from a rack.
func (z *Zookeeper) Stores(rack string) (stores []string, err error) {
	var spath = path.Join(z.storeRootPath, rack)
	if stores, _, err = z.c.Children(spath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", spath, err)
	}
	return
}

// Store get a store node data.
func (z *Zookeeper) Store(rack, store string) (data []byte, err error) {
	var spath = path.Join(z.storeRootPath, rack, store)
	if data, _, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
	}
	return
}

// SetVolumeStat set volume stat 
func (z *Zookeeper) SetVolumeStat(vid int32, stats *stat.Stats) (err error) {
	var (
		d       []byte
		s       *zk.Stat
		spath   string
		vstat = &stat.StatsVolume{
			TotalAddProcessed: stats.TotalAddProcessed,
			TotalAddDelay: stats.TotalAddDelay,
		}
	)
	spath = path.Join(z.volumeRootPath, fmt.Sprintf("%d", vid))
	if _, s, err = z.c.Get(spath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", spath, err)
		return
	}
	if d, err = json.Marshal(vstat); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(spath, d, s.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", spath, err)
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
