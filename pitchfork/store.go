package main

import (
	log "github.com/golang/glog"
	"net/http"
	"encoding/json"
	"io/ioutil"
)


type Store struct {
	rack      string
	ID        string
	host      string
	status    int
	config    *Config
	zk        *Zookeeper
}
type StoreList []*Store

func (sl StoreList) Len() int {
	return len(sl)
}

func (sl StoreList) Less(i, j int) bool {
	return sl[i].ID < sl[j].ID
}

func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

type ProbeResult struct {
	ID        string
	status    int
}


func (s *Store)probeStore() (*ProbeResult, error) {
	url = fmt.Sprintf("http://%s/info", s.host)
	resp, err := http.Get(url)
	if err != nil {
		//log 
		//result = xxxxxxxx
		return result, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//log
		return nil, err
	}

	// make ProbeResult
	var dataJson map[string]interface{}
	if err = json.Unmarshal(body, dataJson); err != nil {
		//log
		return nil, err
	}
	//dataJson['free_volumes']  dataJson['volumes']
}
