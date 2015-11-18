package main

import (
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)


type Pitchfork struct {
	ID        string
	config    *Config
	zk        *Zookeeper
//	stopper   chan struct{}
}

func NewPitchfork(config *Config, zk *Zookeeper) *Pitchfork {
	id, err := generateID()
	if err != nil {
		panic(err)
	}
	return &Pitchfork{ID: id, config: config, zk: zk}
}

func (p *Pitchfork) Register() error {
	node := fmt.Sprintf("%s/%s", p.cg.ZookeeperPitchforkRoot, p.ID)
	flags := int32(zk.FlagEphemeral)
	return p.zk.createPath(node, flags)
}

func (p *Pitchfork) WatchGetPitchfork(pf *Pitchfork) (PitchforkList, <-chan zk.Event, error) {
	pitchforkRootPath = pf.config.ZookeeperPitchforkRoot
	children, _, pitchforkChanges, err := pf.zk.c.ChildrenW(pitchforkRootPath)
	if err != nil {
		return nil, nil, err
	}
	
	result := make(PitchforkList, 0, len(children))
	for _, child := range children {
		pitchforkID = child
		result = append(result, &Pitchfork{ID:pitchforkID, config:p.config, zk:p.zk}
	}
	return result, pitchforkChanges, nil
}

func (p *Pitchfork) WatchGetStores() (StoreList, <-chan zk.Event, error) {
	storeRootPath = p.config.ZookeeperStoreRoot
	if _, storeChanges, err = p.zk.c.GetW(storeRootPath); err != nil {
		log.Errorf("zk.GetW(\"%s\") error(%v)", vpath, err)
		return nil, nil, err
	}

	if children, _, err = p.zk.c.Children(storeRootPath); err != nil {
		log.Errorf("zk.Children(\"%s\") error(%v)", storeRootPath, err)
		return nil, nil, err
	}

	result := make(StoreList, 0, len(children))
	for _, child := range children {
		pathRack = fmt.Sprintf("%s/%s", storeRootPath, child)
		if children1, _, err = p.zk.c.Children(pathRack); err != nil {
			log.Errorf("zk.Children(\"%s\") error(%v)", pathRack, err)
			return nil, nil, err
		}
		for _, child1 := range children1 {
			storeId = child1
			// storeip  status ip need to do rackname
			result = append(result, &Store{rack:rackname, ID:storeId, ip:ip, status:status, config:p.config})  //need to do
		}
	}
	return result, storeChanges, nil
}


func (p *Pitchfork) feedbackDirectory() {
	//set zk store:status
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
