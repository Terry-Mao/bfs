package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strconv"
	"strings"
	"time"
)

// zookeeper save the store meta data.
//
//                                 /rack -- rack root path
//									 |
//                   /rack-a -------- --------- /rack-b -- rack node path
//                     |
//         /store-a -------- /store-b -- store node path (data: {"stat":"localhost:6061","admin":"localhost:6063","api":"localhost:6062","status":0})
//            |                 |
// /volume-1 -                   - /volume-4 volume node path (data: /tmp/block_1,/tmp/block_1.idx,1)
// /volume-2 -                   - /volume-5
// /volume-3 -                   - /volume-6

const (
	storeStatusInit = 0
)

type storeZK struct {
	Stat   string `json:"stat"`
	Admin  string `json:"admin"`
	Api    string `json:"api"`
	Status int    `json:"status"`
}

type Zookeeper struct {
	c     *zk.Conn
	fpath string
	root  string
}

// NewZookeeper new a connection to zookeeper.
func NewZookeeper(addrs []string, timeout time.Duration, root, rack, sid string) (z *Zookeeper, err error) {
	var (
		s <-chan zk.Event
	)
	z = &Zookeeper{}
	z.root = root
	z.fpath = strings.TrimRight(path.Join(root, rack, sid), "/")
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
	err = z.init()
	return
}

// createPath create a zookeeper path.
func (z *Zookeeper) createPath(fpath string) (err error) {
	var (
		str   string
		tpath = ""
	)
	for _, str = range strings.Split(fpath, "/")[1:] {
		tpath = path.Join(tpath, "/", str)
		log.V(1).Infof("create zookeeper path: \"%s\"", tpath)
		if _, err = z.c.Create(tpath, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
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

// init create /rack/store zk path.
func (z *Zookeeper) init() (err error) {
	err = z.createPath(z.fpath)
	return
}

// Volumes get all zk path volumes data.
func (z *Zookeeper) Volumes() (lines []string, err error) {
	var (
		d     []byte
		paths []string
		dpath string
	)
	if paths, _, err = z.c.Children(z.fpath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", z.fpath, err)
		return
	}
	for _, dpath = range paths {
		if d, _, err = z.c.Get(path.Join(z.fpath, dpath)); err != nil {
			log.Errorf("zk.Get(\"%s\") error(%v)", path.Join(z.fpath, dpath),
				err)
			return
		}
		lines = append(lines, string(d))
	}
	return
}

func (z *Zookeeper) volumePath(id int32) string {
	return path.Join(z.fpath, strconv.Itoa(int(id)))
}

// AddVolume add a volume data in zk.
func (z *Zookeeper) AddVolume(v *Volume) (err error) {
	var vpath = z.volumePath(v.Id)
	if _, err = z.c.Create(vpath, v.Meta(), 0, zk.WorldACL(zk.PermAll)); err != nil {
		log.Errorf("zk.Create(\"%s\") error(%v)", vpath, err)
	}
	return
}

// DelVolume delete a volume from zk.
func (z *Zookeeper) DelVolume(id int32) (err error) {
	var (
		stat  *zk.Stat
		vpath = z.volumePath(id)
	)
	if _, stat, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}
	if err = z.c.Delete(vpath, stat.Version); err != nil {
		log.Errorf("zk.Delete(\"%s\") error(%v)", vpath, err)
	}
	return
}

// SetVolume set the data into fpath.
func (z *Zookeeper) SetVolume(v *Volume) (err error) {
	var (
		stat  *zk.Stat
		vpath = z.volumePath(v.Id)
	)
	if _, stat, err = z.c.Get(vpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", vpath, err)
		return
	}
	if _, err = z.c.Set(vpath, v.Meta(), stat.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", vpath, err)
	}
	return
}

// SetStore set the data into fpath.
func (z *Zookeeper) SetStore(stat, admin, api string) (err error) {
	var (
		d   []byte
		s   *zk.Stat
		szk = &storeZK{
			Status: storeStatusInit,
		}
	)
	if d, s, err = z.c.Get(z.fpath); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.fpath, err)
		return
	}
	if len(d) > 0 {
		if err = json.Unmarshal(d, szk); err != nil {
			log.Errorf("json.Unmarshal() error(%v)", err)
			return
		}
	}
	szk.Stat = stat
	szk.Admin = admin
	szk.Api = api
	// zsk.Status not modifify, may update by pitchfork
	if d, err = json.Marshal(szk); err != nil {
		log.Errorf("json.Marshal() error(%v)", err)
		return
	}
	if _, err = z.c.Set(z.fpath, d, s.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.fpath, err)
	}
	return
}

// SetRoot update root.
func (z *Zookeeper) SetRoot() (err error) {
	var s *zk.Stat
	if _, s, err = z.c.Get(z.root); err != nil {
		log.Errorf("zk.Get(\"%s\") error(%v)", z.root, err)
		return
	}
	if _, err = z.c.Set(z.root, []byte(""), s.Version); err != nil {
		log.Errorf("zk.Set(\"%s\") error(%v)", z.root, err)
	}
	return
}

// Close close the zookeeper connection.
func (z *Zookeeper) Close() {
	z.c.Close()
}
