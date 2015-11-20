package main

import (
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"fmt"
	"encoding/json"
	"net/http"
	"io/ioutil"
)


type Pitchfork struct {
	ID        string
	config    *Config
	zk        *Zookeeper
}

func NewPitchfork(zk *Zookeeper, config *Config) *Pitchfork {
	id, err := generateID()
	if err != nil {
		panic(err)
	}

	return &Pitchfork{ID: id, config: config, zk: zk}
}

func (p *Pitchfork) Register() error {
	node := fmt.Sprintf("%s/%s", p.config.ZookeeperPitchforkRoot, p.ID)
	flags := int32(zk.FlagEphemeral)

	return p.zk.createPath(node, flags)
}

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
		pitchforkID := child
		result = append(result, &Pitchfork{ID:pitchforkID, config:p.config, zk:p.zk})
	}

	return result, pitchforkChanges, nil
}

func (p *Pitchfork) WatchGetStores() (StoreList, <-chan zk.Event, error) {
	var (
		storeRootPath      string
		children,children1 []string
		storeChanges       <-chan zk.Event
		result             StoreList
		data               []byte
		dataJson           map[string]interface{}
		err                error
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
		rackName := child
		pathRack := fmt.Sprintf("%s/%s", storeRootPath, rackName)
		if children1, _, err = p.zk.c.Children(pathRack); err != nil {
			log.Errorf("zk.Children(\"%s\") error(%v)", pathRack, err)
			return nil, nil, err
		}
		for _, child1 := range children1 {
			storeId := child1
			pathStore := fmt.Sprintf("%s/%s", pathRack, storeId)
			if data, _, err = p.zk.c.Get(pathStore); err != nil {
				log.Errorf("zk.Get(\"%s\") error(%v)", pathStore, err)
				return nil, nil, err
			}
			if err = json.Unmarshal(data, &dataJson); err != nil {
				log.Errorf("json.Marshal() error(%v)", err)
				return nil, nil, err
			}

			status := int32(dataJson["status"].(float64))
			host := string(dataJson["stat"].(string))
			result = append(result, &Store{rack:rackName, ID:storeId, host:host, status:status})  //need to do
		}
	}

	return result, storeChanges, nil
}

func (p *Pitchfork)probeStore(s *Store) error {
	var (
		status = int32(0xff)
		url      string
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		volumes  []interface{}
		err      error
	)
	if s.status == 0 {
		log.Warningf("probeStore() store not online host:%s", s.host)
		return nil
	}
	url = fmt.Sprintf("http://%s/info", s.host)
	if resp, err = http.Get(url); err != nil {
		status = status & 0xfc
		log.Errorf("http.Get() called error(%v)  url:%s", err, url)
		goto feedbackZk
	}

	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("probeStore() ioutil.ReadAll() error(%v)", err)
		return err
	}

	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("probeStore() json.Unmarshal() error(%v)", err)
		return err
	}

	volumes =  dataJson["volumes"].([]interface{})
	for _, volume := range volumes {
		volumeValue := volume.(map[string]interface{})
		block := volumeValue["block"].(map[string]interface{})
		offset := int64(block["offset"].(float64))
		if int64(maxOffset * p.config.MaxUsedSpacePercent) < offset {
			log.Warningf("probeStore() store block has no enough space, host:%s", s.host)
			status = status & 0xfd
		}
		lastErr := block["last_err"]
		if lastErr != nil {
			status = status & 0xfc
			log.Errorf("probeStore() store error(%v) host:%s", lastErr, s.host)
			goto feedbackZk
		}
	}
	if s.status == status {
		return nil
	}

feedbackZk:
    pathStore := fmt.Sprintf("%s/%s/%s", p.config.ZookeeperStoreRoot, s.rack, s.ID)
    if err = p.zk.setStoreStatus(pathStore, status); err != nil {
    	log.Errorf("setStoreStatus() called error(%v) path:%s", err, pathStore)
    	return err
    }
    log.Infof("probeStore() called success host:%s status: %d %d", s.host, s.status, status)
    return nil
}

type PitchforkList []*Pitchfork

func (pl PitchforkList) Len() int {
	return len(pl)
}

func (pl PitchforkList) Less(i, j int) bool {
	return pl[i].ID < pl[j].ID
}

func (pl PitchforkList) Swap(i, j int) {
	pl[i], pl[j] = pl[j], pl[i]
}
