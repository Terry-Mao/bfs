package meta

import (
	"encoding/json"
	"fmt"
	log "github.com/golang/glog"
	"io/ioutil"
	"net/http"
)

const (
	// bit
	StoreStatusEnableBit = 31
	StoreStatusReadBit   = 0
	StoreStatusWriteBit  = 1
	// status
	StoreStatusInit   = 0
	StoreStatusEnable = (1 << StoreStatusEnableBit)
	StoreStatusRead   = StoreStatusEnable | (1 << StoreStatusReadBit)
	StoreStatusWrite  = StoreStatusEnable | (1 << StoreStatusWriteBit)
	StoreStatusHealth = StoreStatusRead | StoreStatusWrite
	StoreStatusFail   = StoreStatusEnable
	// api
	statAPI = "http://%s/info"
)

type StoreList []*Store

func (sl StoreList) Len() int {
	return len(sl)
}

func (sl StoreList) Less(i, j int) bool {
	return sl[i].Id < sl[j].Id
}

func (sl StoreList) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

// store zk meta data.
type Store struct {
	Stat   string `json:"stat"`
	Admin  string `json:"admin"`
	Api    string `json:"api"`
	Id     string `json:"id"`
	Rack   string `json:"rack"`
	Status int    `json:"status"`
}

// statAPI get stat http api.
func (s *Store) statAPI() string {
	return fmt.Sprintf(statAPI, s.Stat)
}

// Info get store volumes info.
func (s *Store) Info() (vs []*Volume, err error) {
	var (
		ok       bool
		body     []byte
		resp     *http.Response
		dataJson map[string]interface{}
		url      = s.statAPI()
	)
	if resp, err = http.Get(url); err != nil {
		log.Warningf("http.Get(\"%s\") error(%v)", url, err)
		return
	}
	defer resp.Body.Close()
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll() error(%v)", err)
		return
	}
	if err = json.Unmarshal(body, &dataJson); err != nil {
		log.Errorf("json.Unmarshal() error(%v)", err)
		return
	}
	if vs, ok = dataJson["volumes"].([]*Volume); !ok {
		log.Errorf("type assection failed")
		vs = nil
	}
	return
}

// Head send a head request to store.
func (s *Store) Head(n *Needle) (err error) {
	return
}
