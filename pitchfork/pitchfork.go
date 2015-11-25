package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Terry-Mao/bfs/libs/meta"
	"github.com/Terry-Mao/bfs/libs/uuid"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type Pitchfork struct {
	Id     string
	config *Config
	zk     *Zookeeper
}

type PitchforkList []*Pitchfork

//Len
func (pl PitchforkList) Len() int {
	return len(pl)
}

//Less
func (pl PitchforkList) Less(i, j int) bool {
	return pl[i].Id < pl[j].Id
}

//Swap
func (pl PitchforkList) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}

// NewPitchfork
func NewPitchfork(zk *Zookeeper, config *Config) (p *Pitchfork, err error) {
	p = &Pitchfork{}
	p.config = config
	p.zk = zk
	if p.Id, err = p.id(); err != nil {
		return
	}
	err = p.init()
	return
}

// init register temporary pitchfork node in the zookeeper.
func (p *Pitchfork) init() (err error) {
	// TODO
	return p.zk.createPath(fmt.Sprintf("%s/%s", p.config.ZookeeperPitchforkRoot, p.Id), int32(zk.FlagEphemeral))
}

//WatchGetPitchforks get all the pitchfork nodes and set up the watcher in the zookeeper
func (p *Pitchfork) WatchGetPitchforks() (PitchforkList, <-chan zk.Event, error) {
	var (
		pitchforkRootPath string
		children          []string
		pitchforkChanges  <-chan zk.Event
		result            PitchforkList
		err               error
	)

	pitchforkRootPath = p.config.ZookeeperPitchforkRoot
	children, _, pitchforkChanges, err = p.zk.c.ChildrenW(pitchforkRootPath)
	if err != nil {
		log.Errorf("zk.ChildrenW(\"%s\") error(%v)", pitchforkRootPath, err)
		return nil, nil, err
	}

	result = make(PitchforkList, 0, len(children))
	for _, child := range children {
		pitchforkId := child
		result = append(result, &Pitchfork{Id: pitchforkId, config: p.config, zk: p.zk})
	}

	return result, pitchforkChanges, nil
}

//WatchGetStores get all the store nodes and set up the watcher in the zookeeper
func (p *Pitchfork) WatchGetStores() (StoreList, <-chan zk.Event, error) {
	var (
		storeRootPath       string
		children, children1 []string
		storeChanges        <-chan zk.Event
		result              StoreList
		data                []byte
		store               = &meta.Store{}
		err                 error
	)

	storeRootPath = p.config.ZookeeperStoreRoot
	if _, _, storeChanges, err = p.zk.c.GetW(storeRootPath); err != nil {
		log.Errorf("zk.GetW(\"%s\") error(%v)", storeRootPath, err)
		return nil, nil, err
	}

	if children, _, err = p.zk.c.Children(storeRootPath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", storeRootPath, err)
		return nil, nil, err
	}

	result = make(StoreList, 0, len(children))
	for _, child := range children {
		pathRack := fmt.Sprintf("%s/%s", storeRootPath, child)
		if children1, _, err = p.zk.c.Children(pathRack); err != nil {
			log.Errorf("zk.Children(\"%s\") error(%v)", pathRack, err)
			return nil, nil, err
		}
		for _, child1 := range children1 {
			pathStore := fmt.Sprintf("%s/%s", pathRack, child1)
			if data, _, err = p.zk.c.Get(pathStore); err != nil {
				log.Errorf("zk.Get(\"%s\") error(%v)", pathStore, err)
				return nil, nil, err
			}
			if err = json.Unmarshal(data, store); err != nil {
				log.Errorf("json.Unmarshal() error(%v)", err)
				return nil, nil, err
			}

			result = append(result, store)
		}
	}

	return result, storeChanges, nil
}

//getStore get store node and feed back to directory
func (p *Pitchfork) getStore(s *meta.Store) error {
	var (
		status   = meta.StoreStatusHealth
		url      string
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		volumes  []interface{}
		err      error
	)
	if s.Status == 0 {
		log.Warningf("getStore() store not online host:%s", s.Stat)
		return nil
	}
	url = fmt.Sprintf("http://%s/info", s.Stat)
	if resp, err = http.Get(url); err != nil || resp.StatusCode == 500 {
		status = meta.StoreStatusEnable
		log.Errorf("http.Get() called error(%v)  url:%s", err, url)
		goto feedbackZk
	}

	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("getStore() ioutil.ReadAll() error(%v)", err)
		return err
	}

	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("getStore() json.Unmarshal() error(%v)", err)
		return err
	}

	volumes = dataJson["volumes"].([]interface{})
	for _, volume := range volumes {
		volumeValue := volume.(map[string]interface{})
		block := volumeValue["block"].(map[string]interface{})
		offset := int64(block["offset"].(float64))
		if int64(maxOffset*p.config.MaxUsedSpacePercent) < offset {
			log.Warningf("getStore() store block has no enough space, host:%s", s.Stat)
			status = meta.StoreStatusRead
		}
		lastErr := block["last_err"]
		if lastErr != nil {
			status = meta.StoreStatusEnable
			log.Errorf("getStore() store last_err error(%v) host:%s", lastErr, s.Stat)
			goto feedbackZk
		}
	}

feedbackZk:
	if s.Status == status {
		return nil
	}
	pathStore := fmt.Sprintf("%s/%s/%s", p.config.ZookeeperStoreRoot, s.Rack, s.Id)
	if err = p.zk.setStoreStatus(pathStore, status); err != nil {
		log.Errorf("setStoreStatus() called error(%v) path:%s", err, pathStore)
		return err
	}
	s.Status = status
	log.Infof("getStore() called success host:%s status: %d %d", s.Stat, s.Status, status)
	return nil
}

//headStore head store node and feed back to directory
func (p *Pitchfork) headStore(s *meta.Store) error {
	var (
		status   = meta.StoreStatusHealth
		url      string
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		volumes  []interface{}
		wg       sync.WaitGroup
		err      error
	)

	if s.Status == 0 {
		log.Warningf("headStore() store not online host:%s", s.Stat)
		return nil
	}
	url = fmt.Sprintf("http://%s/info", s.Stat)
	if resp, err = http.Get(url); err != nil || resp.StatusCode != 200 {
		log.Warningf("headStore Store http.Head error")
		return nil
	}

	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("headStore() ioutil.ReadAll() error(%v)", err)
		return err
	}

	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("headStore() json.Unmarshal() error(%v)", err)
		return err
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
				url := fmt.Sprintf("http://%s/get?key=%d&cookie=%d&vid=%d", s.Api, key, cookie, vid)
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
	pathStore := fmt.Sprintf("%s/%s/%s", p.config.ZookeeperStoreRoot, s.Rack, s.Id)
	if err = p.zk.setStoreStatus(pathStore, status); err != nil {
		log.Errorf("setStoreStatus() called error(%v) path:%s", err, pathStore)
		return err
	}
	s.Status = status
	log.Infof("headStore() called success host:%s status: %d %d", s.Stat, s.Status, status)
	return nil
}

//Probe main flow of pitchfork server
func (p *Pitchfork) Probe() {
	var (
		stores           StoreList
		pitchforks       PitchforkList
		storeChanges     <-chan zk.Event
		pitchforkChanges <-chan zk.Event
		allStores        map[string]StoreList
		stopper          chan struct{}
		store            *meta.Store
		err              error
	)
	for {
		stores, storeChanges, err = p.WatchGetStores()
		if err != nil {
			log.Errorf("WatchGetStores() called error(%v)", err)
			return
		}

		pitchforks, pitchforkChanges, err = p.WatchGetPitchforks()
		if err != nil {
			log.Errorf("WatchGetPitchforks() called error(%v)", err)
			return
		}

		if allStores, err = divideStoreBetweenPitchfork(pitchforks, stores); err != nil {
			log.Errorf("divideStoreBetweenPitchfork() called error(%v)", err)
			return
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

// id get a pitchfork node id.
func (p *Pitchfork) id() (str string, err error) {
	var uuidStr, host string
	if uuidStr, err = uuid.New(); err != nil {
		return
	}
	if host, err = os.Hostname(); err != nil {
		return
	}
	str = fmt.Sprintf("%s:%s", host, uuidStr)
	return
}

// Divides a set of stores between a set of pitchforks.
func divideStoreBetweenPitchfork(pitchforks PitchforkList, stores StoreList) (map[string]StoreList, error) {
	result := make(map[string]StoreList)

	slen := len(stores)
	plen := len(pitchforks)
	if slen == 0 || plen == 0 || slen < plen {
		return nil, errors.New("divideStoreBetweenPitchfork error")
	}

	sort.Sort(stores)
	sort.Sort(pitchforks)

	n := slen / plen
	m := slen % plen
	p := 0
	for i, pitchfork := range pitchforks {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > slen {
			last = slen
		}

		for _, store := range stores[first:last] {
			result[pitchfork.Id] = append(result[pitchfork.Id], store)
		}
		p = last
	}

	return result, nil
}
