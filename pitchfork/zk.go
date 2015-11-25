package main

import (
	"encoding/json"
	"path"
	"strings"
	"time"

	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

type Zookeeper struct {
	c *zk.Conn
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(addrs []string, timeout time.Duration) (
	z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	if z.c, s, err = zk.Connect(addrs, timeout); err != nil {
		log.Errorf("zk.Connect(\"%v\") error(%v)", addrs, err)
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

// createPath create a zookeeper path.
func (z *Zookeeper) createPath(fpath string, flags int32) (err error) {
	var (
		str   string
		tpath = ""
	)
	for _, str = range strings.Split(fpath, "/")[1:] {
		tpath = path.Join(tpath, "/", str)
		log.V(1).Infof("create zookeeper path: \"%s\"", tpath)
		if _, err = z.c.Create(tpath, []byte(""), flags, zk.WorldACL(zk.PermAll)); err != nil {
			if err != zk.ErrNodeExists {
				log.Errorf("zk.create(\"%s\") error(%v)", tpath, err)
				return
			} else {
				err = nil
			}
		}
	}
	return
}

// setStoreStatus update store status
func (z *Zookeeper) setStoreStatus(pathStore string, status int) error {
	var (
		data  []byte
		stat  *zk.Stat
		store = &meta.Store{}
		err   error
	)
	if data, stat, err = z.c.Get(pathStore); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", pathStore, err)
		return err
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, store); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return err
		}
	}
	store.Status = status
	if data, err = json.Marshal(store); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return err
	}
	if _, err = z.c.Set(pathStore, data, stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", pathStore, err)
		return err
	}
	return nil
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
