package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"time"
)

const (
	retrySleep = time.Second * 1
	retryCount = 3
)

type Pitchfork struct {
	Id     string
	config *Config
	zk     *Zookeeper
}

// NewPitchfork
func NewPitchfork(zk *Zookeeper, config *Config) (p *Pitchfork, err error) {
	var id string
	p = &Pitchfork{}
	p.config = config
	p.zk = zk
	if id, err = p.init(); err != nil {
		log.Errorf("NewPitchfork failed error(%v)", err)
		return
	}
	p.Id = id
	return
}

// init register temporary pitchfork node in the zookeeper.
func (p *Pitchfork) init() (node string, err error) {
	node, err = p.zk.NewNode(p.config.ZookeeperPitchforkRoot)
	return
}

// watchPitchforks get all the pitchfork nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watchPitchforks() (res []string, ev <-chan zk.Event, err error) {
	if res, ev, err = p.zk.WatchPitchforks(); err == nil {
		sort.Strings(res)
	}
	return
}

// watchStores get all the store nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watchStores() (res []*meta.Store, ev <-chan zk.Event, err error) {
	var (
		rack, store   string
		racks, stores []string
		data          []byte
		storeMeta     *meta.Store
	)
	if racks, ev, err = p.zk.WatchRacks(); err != nil {
		log.Errorf("zk.WatchGetStore() error(%v)", err)
		return
	}
	for _, rack = range racks {
		if stores, err = p.zk.Stores(rack); err != nil {
			return
		}
		for _, store = range stores {
			if data, err = p.zk.Store(rack, store); err != nil {
				return
			}
			storeMeta = new(meta.Store)
			if err = json.Unmarshal(data, storeMeta); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return
			}
			res = append(res, storeMeta)
		}
	}
	sort.Sort(meta.StoreList(res))
	return
}

// Probe main flow of pitchfork server.
func (p *Pitchfork) Probe() {
	var (
		stores     []*meta.Store
		pitchforks []string
		sev        <-chan zk.Event
		pev        <-chan zk.Event
		stop       chan struct{}
		store      *meta.Store
		err        error
	)
	for {
		if stores, sev, err = p.watchStores(); err != nil {
			log.Errorf("watchGetStores() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if pitchforks, pev, err = p.watchPitchforks(); err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			time.Sleep(retrySleep)
			continue
		}
		if stores = p.divide(pitchforks, stores); err != nil || len(stores) == 0 {
			time.Sleep(retrySleep)
			continue
		}
		stop = make(chan struct{})
		for _, store = range stores {
			go p.checkHealth(store, stop)
			go p.checkNeedles(store, stop)
		}
		select {
		case <-sev:
			log.Infof("store nodes change, rebalance")
			close(stop)
		case <-pev:
			log.Infof("pitchfork nodes change, rebalance")
			close(stop)
		}
	}

	return
}

// divide a set of stores between a set of pitchforks.
func (p *Pitchfork) divide(pitchforks []string, stores []*meta.Store) []*meta.Store {
	var (
		n, m        int
		ss, ps      int
		first, last int
		node        string
		store       *meta.Store
		sm          = make(map[string][]*meta.Store)
	)
	ss = len(stores)
	ps = len(pitchforks)
	if ss == 0 || ps == 0 || ss < ps {
		return nil
	}
	n = ss / ps
	m = ss % ps
	first = 0
	for _, node = range pitchforks {
		last = first + n
		if m > 0 {
			// let front node add one more
			last++
			m--
		}
		if last > ss {
			last = ss
		}
		for _, store = range stores[first:last] {
			sm[node] = append(sm[node], store)
		}
		first = last
	}
	return sm[p.Id]
}

// checkHealth check the store health.
func (p *Pitchfork) checkHealth(store *meta.Store, stop chan struct{}) (err error) {
	var (
		status,i  int
		volume    *meta.Volume
		volumes   []*meta.Volume
	)
	log.Infof("check_health job start")
	for {
		select {
		case <-stop:
			log.Infof("check_health job stop")
			return
		case <-time.After(p.config.GetInterval):
			break
		}
		status = store.Status
		for i = 0; i < retryCount; i++ {
			if volumes, err = store.Info(); err == nil {
				break
			}
		}
		if err == nil {
			for _, volume = range volumes {
				if volume.Block.LastErr != nil {
					log.Infof("get store block.lastErr:%s   host:%s", volume.Block.LastErr, store.Stat)
					store.Status = meta.StoreStatusFail
					break
				} else if volume.Block.Full() {
						log.Infof("block: %s, offset: %d", volume.Block.File, volume.Block.Offset)
						store.Status = meta.StoreStatusRead
				}
			}
		} else {
			log.Errorf("get store info failed, retry host:%s", store.Stat)
			store.Status = meta.StoreStatusFail
		}

		if status != store.Status {
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}
			if err = p.zk.setRoot(); err != nil {
				log.Errorf("setRoot zk failed")
			}
		}
	}
	return
}

// checkNeedles check the store health.
func (p *Pitchfork) checkNeedles(store *meta.Store, stop chan struct{}) (err error) {
	var (
		status,i  int
		needle    *meta.Needle
		volume    *meta.Volume
		volumes   []*meta.Volume
	)
	log.Infof("checkNeedles job start")
	for {
		select {
		case <-stop:
			log.Infof("checkNeedles job stop")
			return
		case <-time.After(p.config.HeadInterval):
			break
		}
		if volumes, err = store.Info(); err != nil {
			log.Errorf("get store info failed, retry host:%s", store.Stat)
			continue
		}
		status = store.Status
		for _, volume = range volumes {
			if volume.Block.LastErr != nil {
				break
			} else {
				for _, needle = range volume.CheckNeedles {
					for i = 0; i < retryCount; i++ {
						if err = store.Head(needle, volume.Id); err == nil {
							break
						}
					}
					if err != nil {
						log.Errorf("head store failed, needle:%d host:%s", needle.Key, store.Stat)
						store.Status = meta.StoreStatusFail
						goto feedback
					}
				}
			}
		}
	feedback:
		if status != store.Status {
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}
			if err = p.zk.setRoot(); err != nil {
				log.Errorf("setRoot zk failed")
			}
		}
	}
	return
}