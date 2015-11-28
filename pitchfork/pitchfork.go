package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"net/http"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	retrySleep   = 15 * time.Second //zookeeper FlagEphemeral
	retryCount   = 3
	getStoreInfo = "http://%s/info"
	getStoreFile = "http://%s/get?key=%d&cookie=%d&vid=%d"
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
		stores           meta.StoreList
		pitchforks       []string
		storeChanges     <-chan zk.Event
		pitchforkChanges <-chan zk.Event
		allStores        map[string]meta.StoreList
		stopper          chan struct{}
		store            *meta.Store
		err              error
	)
	for {
		if stores, storeChanges, err = p.watchStores(); err != nil {
			log.Errorf("watchGetStores() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if pitchforks, pitchforkChanges, err = p.watchPitchforks(); err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			time.Sleep(1 * time.Second)
			continue
		}
		for i := 0; i < retryCount; i++ {
			if allStores, err = divideStoreBetweenPitchfork(pitchforks, stores); err == nil {
				break
			}
			if i == 2 {
				log.Errorf("divideStoreBetweenPitchfork() called error(%v)", err)
				return
			}
			time.Sleep(retrySleep)
		}
		stopper = make(chan struct{})
		for _, store = range allStores[p.Id] {
			go func(store *meta.Store) {
				for {
					if err = p.getStore(store); err != nil {
						log.Errorf("probeStore() called error(%v)", err)
					}
					select {
					case <-stopper:
						return
					case <-time.After(p.config.GetInterval):
					}
				}
			}(store)
			go func(store *meta.Store) {
				for {
					if err = p.headStore(store); err != nil {
						log.Errorf("headStore() called error(%v)", err)
					}
					select {
					case <-stopper:
						return
					case <-time.After(p.config.HeadInterval):
					}
				}
			}(store)
		}
		select {
		case <-storeChanges:
			log.Infof("Triggering rebalance due to store list change")
			close(stopper)
		case <-pitchforkChanges:
			log.Infof("Triggering rebalance due to pitchfork list change")
			close(stopper)
		}
	}
}

// Divides a set of stores between a set of pitchforks.
func divideStoreBetweenPitchfork(pitchforks []string, stores meta.StoreList) (result map[string]meta.StoreList, err error) {
	result = make(map[string]meta.StoreList)
	slen := len(stores)
	plen := len(pitchforks)
	if slen == 0 || plen == 0 || slen < plen {
		return nil, errors.New("divideStoreBetweenPitchfork error")
	}
	n := slen / plen
	m := slen % plen
	p := 0
	for i, node := range pitchforks {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > slen {
			last = slen
		}

		for _, store := range stores[first:last] {
			result[node] = append(result[node], store)
		}
		p = last
	}
	return
}

// getStore get store node and feed back to directory
func (p *Pitchfork) getStore(s *meta.Store) (err error) {
	var (
		status   = meta.StoreStatusHealth
		url      string
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		volumes  []interface{}
	)
	if s.Status == 0 {
		log.Warningf("getStore() store not online host:%s", s.Stat)
		return
	}
	url = fmt.Sprintf(getStoreInfo, s.Stat)
	if resp, err = http.Get(url); err != nil || resp.StatusCode == 500 {
		status = meta.StoreStatusEnable
		log.Errorf("http.Get() called error(%v)  url:%s", err, url)
		goto feedbackZk
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("getStore() ioutil.ReadAll() error(%v)", err)
		return
	}
	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("getStore() json.Unmarshal() error(%v)", err)
		return
	}
	volumes = dataJson["volumes"].([]interface{})
	for _, volume := range volumes {
		volumeValue := volume.(map[string]interface{})
		block := volumeValue["block"].(map[string]interface{})
		offset := int64(block["offset"].(float64))
		if int64(meta.MaxVolumeOffset*p.config.MaxUsedSpacePercent) < offset {
			log.Warningf("getStore() store block has no enough space, host:%s", s.Stat)
			status = meta.StoreStatusRead
		}
		lastErr := block["last_err"]
		if lastErr != nil {
			status = meta.StoreStatusEnable
			log.Errorf("getStore() store last_err error(%v) host:%s", lastErr, s.Stat)
			goto feedbackZk
		}
		//update /volume state
	}

feedbackZk:
	if s.Status == status {
		return nil
	}
	pathStore := path.Join(p.config.ZookeeperStoreRoot, s.Rack, s.Id)
	if err = p.zk.SetStoreStatus(pathStore, status); err != nil {
		log.Errorf("setStoreStatus() called error(%v) path:%s", err, pathStore)
		return
	}
	if err = p.zk.SetRoot(); err != nil {
		log.Errorf("SetRoot() called error(%v)", err)
		return
	}
	s.Status = status
	log.Infof("getStore() called success host:%s status: %d %d", s.Stat, s.Status, status)
	return
}

// headStore head store node and feed back to directory
func (p *Pitchfork) headStore(s *meta.Store) (err error) {
	var (
		status   = meta.StoreStatusHealth
		url      string
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		volumes  []interface{}
		wg       sync.WaitGroup
	)

	if s.Status == 0 {
		log.Warningf("headStore() store not online host:%s", s.Stat)
		return
	}
	url = fmt.Sprintf(getStoreInfo, s.Stat)
	if resp, err = http.Get(url); err != nil || resp.StatusCode != 200 {
		log.Warningf("headStore Store http.Head error")
		return nil
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("headStore() ioutil.ReadAll() error(%v)", err)
		return
	}
	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("headStore() json.Unmarshal() error(%v)", err)
		return
	}
	volumes = dataJson["volumes"].([]interface{})
	for _, volume := range volumes {
		volumeValue := volume.(map[string]interface{})
		vid := int64(volumeValue["id"].(float64))
		headResult := make([]int, 0) //mabay be change logic

		checkNeedles := volumeValue["check_needles"].([]interface{})
		for _, needle := range checkNeedles {
			needleValue := needle.(map[string]interface{})
			key := int64(needleValue["key"].(float64))
			cookie := int64(needleValue["cookie"].(float64))
			if key == 0 {
				continue
			}
			wg.Add(1)
			go func(key int64, cookie int64) {
				defer wg.Done()
				url := fmt.Sprintf(getStoreFile, s.Api, key, cookie, vid)
				if resp, err = http.Head(url); err == nil {
					if resp.StatusCode == 500 {
						headResult = append(headResult, resp.StatusCode)
					}
				}
			}(key, cookie)
		}
		wg.Wait()
		if len(headResult) != 0 {
			status = meta.StoreStatusEnable
			log.Errorf("headStore result : io error   host:%s", s.Api)
			goto feedbackZk
		}
	}
	return nil

feedbackZk:
	pathStore := path.Join(p.config.ZookeeperStoreRoot, s.Rack, s.Id)
	if err = p.zk.SetStoreStatus(pathStore, status); err != nil {
		log.Errorf("setStoreStatus() called error(%v) path:%s", err, pathStore)
		return
	}
	if err = p.zk.SetRoot(); err != nil {
		log.Errorf("SetRoot() called error(%v)", err)
		return
	}
	s.Status = status
	log.Infof("headStore() called success host:%s status: %d %d", s.Stat, s.Status, status)
	return nil
}
