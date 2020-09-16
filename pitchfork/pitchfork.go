package main

import (
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
	"time"

	"bfs/libs/errors"
	"bfs/libs/meta"

	"bfs/pitchfork/conf"
	myzk "bfs/pitchfork/zk"

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	_retrySleep = time.Second * 1
	_retryCount = 3
)

// Pitchfork struct
type Pitchfork struct {
	ID     string
	config *conf.Config
	zk     *myzk.Zookeeper
}

// NewPitchfork new pitchfork.
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
	p.ID = id
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

func (p *Pitchfork) groups() (groups []string, err error) {
	if groups, err = p.zk.Groups(); err != nil {
		log.Errorf("zk.Groups() error(%v)", err)
		return
	}
	sort.Sort(sort.StringSlice(groups))
	return
}

// watchStores get all the store nodes and set up the watcher in the zookeeper.
func (p *Pitchfork) watchStores() (res map[string]*meta.Store, ev <-chan zk.Event, err error) {
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
	res = make(map[string]*meta.Store)
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
			res[storeMeta.Id] = storeMeta
		}
	}
	return
}

// Probe main flow of pitchfork server.
func (p *Pitchfork) Probe() {
	var (
		groups     []string
		pitchforks []string
		storeMetas map[string]*meta.Store
		sev        <-chan zk.Event
		pev        <-chan zk.Event
		err        error
		wg         sync.WaitGroup
	)
	for {
		if storeMetas, sev, err = p.watchStores(); err != nil {
			log.Errorf("watchGetStores() called error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if pitchforks, pev, err = p.watch(); err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if groups, err = p.groups(); err != nil {
			log.Errorf("get groups() error(%v)", err)
			time.Sleep(_retrySleep)
			continue
		}
		if groups = p.divide(pitchforks, groups); err != nil || len(groups) == 0 {
			time.Sleep(_retrySleep)
			continue
		}
		wg.Add(len(groups))
		for _, group := range groups {
			go p.healthCheck(group, storeMetas, &wg)
		}
		wg.Wait()
		select {
		case <-sev:
			log.Infof("store nodes change, rebalance")
		case <-pev:
			log.Infof("pitchfork nodes change, rebalance")

		case <-time.After(p.config.Store.RackCheckInterval.Duration):
			log.Infof("pitchfork poll zk")
		}
	}
}

// divide a set of stores between a set of pitchforks.
func (p *Pitchfork) divide(pitchforks []string, groups []string) []string {
	var (
		n, m        int
		ss, ps      int
		first, last int
		node        string
		sm          = make(map[string][]string)
	)
	ss = len(groups)
	ps = len(pitchforks)
	if ss == 0 || ps == 0 {
		return nil
	}
	if ss < ps {
		// rand get a group
		return []string{groups[rand.Intn(ss)]}
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
		sm[node] = append(sm[node], groups[first:last]...)
		first = last
	}
	return sm[p.ID]
}

// check the group health.
func (p *Pitchfork) healthCheck(group string, sm map[string]*meta.Store, wg *sync.WaitGroup) {
	var (
		err           error
		nodes         []string
		groupReadOnly = false
	)
	defer func() {
		wg.Done()
		log.Infof("health check job stop")
	}()
	log.Infof("health check job start")
	if nodes, err = p.zk.GroupStores(group); err != nil {
		log.Errorf("zk.GroupStores(%s) error(%v)", group, err)
		return
	}
	stores := make([]*meta.Store, 0, len(nodes))
	for _, sid := range nodes {
		if store, ok := sm[sid]; ok {
			stores = append(stores, store)
		}
	}
	for _, store := range stores {
		p.syncStore(store)
		if store.Status == meta.StoreStatusRead {
			groupReadOnly = true
		}
		log.Infof("after check, group:%s,store(%+v)", group, store)
	}
	if groupReadOnly {
		for _, store := range stores {
			store.Status = meta.StoreStatusRead
			if err = p.zk.SetStore(store); err != nil {
				log.Errorf("zk.SetStore(%+v) error(%v)", store, err)
			}
		}
	}
}

func (p *Pitchfork) syncStore(store *meta.Store) {
	var (
		err     error
		status  int
		volume  *meta.Volume
		volumes []*meta.Volume
	)
	if store.Status == meta.StoreStatusSync {
		log.Infof("store status is sync, health check will be ignored")
		return
	}
	storeReadOnly := true
	status = store.Status
	store.Status = meta.StoreStatusHealth
	if volumes, err = store.Info(); err == errors.ErrServiceTimeout {
		// ignore timeout and no retry.
		log.Errorf("store.Info() err:%v", err)
		return
	}
	if err == errors.ErrServiceUnavailable {
		log.Errorf("store.Info() err:%v", err)
		store.Status = meta.StoreStatusFail
		goto failed
	} else if err != nil {
		log.Errorf("get store info failed, retry host:%s", store.Stat)
		store.Status = meta.StoreStatusFail
		goto failed
	}
	for _, volume = range volumes {
		if volume.Block.LastErr != nil {
			log.Infof("get store block.lastErr:%s host:%s", volume.Block.LastErr, store.Stat)
			store.Status = meta.StoreStatusFail
			goto failed
		} else if !volume.Block.Full() {
			storeReadOnly = false
		}
		if err = p.zk.UpdateVolumeState(volume); err != nil {
			log.Errorf("zk.UpdateVolumeState() error(%v)", err)
		}
	}
	if storeReadOnly {
		store.Status = meta.StoreStatusRead
	}
	if len(volumes) > 0 {
		volume = volumes[rand.Intn(len(volumes))]
		if err = store.Head(volume.Id); err == errors.ErrInternal {
			store.Status = meta.StoreStatusFail
		}
	}
failed:
	if status != store.Status {
		if err = p.zk.SetStore(store); err != nil {
			log.Errorf("update store zk status failed, retry")
		}
	}
}
