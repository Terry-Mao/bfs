package main

import (
	"bfs/libs/meta"
	"bfs/pitchfork/conf"
	myzk "bfs/pitchfork/zk"
	"encoding/json"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"time"
)

const (
	_retrySleep = time.Second * 1
	_retryCount = 3
)

type Pitchfork struct {
	Id     string
	config *conf.Config
	zk     *myzk.Zookeeper
}

// NewPitchfork
func NewPitchfork(config *conf.Config) (p *Pitchfork, err error) {
	var id string
	p = &Pitchfork{}
	p.config = config
	if p.zk, err = myzk.NewZookeeper(config); err != nil {
		log.Errorf("NewZookeeper() failed, Quit now")
		return
	}
	if id, err = p.init(); err != nil {
		log.Errorf("NewPitchfork failed error(%v)", err)
		return
	}
	p.Id = id
	return
}

// init register temporary pitchfork node in the zookeeper.
func (p *Pitchfork) init() (node string, err error) {
	node, err = p.zk.NewNode(p.config.Zookeeper.PitchforkRoot)
	return
}

// watchPitchforks get all the pitchfork nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watch() (res []string, ev <-chan zk.Event, err error) {
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
			time.Sleep(_retrySleep)
			continue
		}
		if pitchforks, pev, err = p.watch(); err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if stores = p.divide(pitchforks, stores); err != nil || len(stores) == 0 {
			time.Sleep(_retrySleep)
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
		case <-pev:
			log.Infof("pitchfork nodes change, rebalance")
		case <-time.After(p.config.Store.RackCheckInterval.Duration):
			log.Infof("pitchfork poll zk")
		}
		close(stop)
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
		status, i int
		volume    *meta.Volume
		volumes   []*meta.Volume
	)
	log.Infof("check_health job start")
	for {
		select {
		case <-stop:
			log.Infof("check_health job stop")
			return
		case <-time.After(p.config.Store.StoreCheckInterval.Duration):
			break
		}
		status = store.Status
		store.Status = meta.StoreStatusHealth
		for i = 0; i < _retryCount; i++ {
			if volumes, err = store.Info(); err == nil {
				break
			}
			time.Sleep(_retrySleep)
		}
		if err == nil {
			for _, volume = range volumes {
				if volume.Block.LastErr != nil {
					log.Infof("get store block.lastErr:%s host:%s", volume.Block.LastErr, store.Stat)
					store.Status = meta.StoreStatusFail
					break
				} else if volume.Block.Full() {
					log.Infof("block: %s, offset: %d", volume.Block.File, volume.Block.Offset)
					store.Status = meta.StoreStatusRead
				}
				if err = p.zk.SetVolumeState(volume); err != nil {
					log.Errorf("zk.SetVolumeState() error(%v)", err)
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
		}
	}
	return
}

// checkNeedles check the store health.
func (p *Pitchfork) checkNeedles(store *meta.Store, stop chan struct{}) (err error) {
	var (
		status  int
		volume  *meta.Volume
		volumes []*meta.Volume
	)
	log.Infof("checkNeedles job start")
	for {
		select {
		case <-stop:
			log.Infof("checkNeedles job stop")
			return
		case <-time.After(p.config.Store.NeedleCheckInterval.Duration):
			break
		}
		if volumes, err = store.Info(); err != nil {
			log.Errorf("get store info failed, retry host:%s", store.Stat)
			continue
		}
		status = store.Status
		for _, volume = range volumes {
			if err = volume.Block.LastErr; err != nil {
				break
			}
			if err = store.Head(volume.Id); err != nil {
				store.Status = meta.StoreStatusFail
				goto failed
			}
		}
	failed:
		if status != store.Status {
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("update store zk status failed, retry")
				continue
			}
		}
	}
	return
}
