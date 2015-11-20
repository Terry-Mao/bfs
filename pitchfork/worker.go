package main

import (
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)


func Work(p *Pitchfork) {
	var (
		stores             StoreList
		pitchforks         PitchforkList
		storeChanges       <-chan zk.Event
		pitchforkChanges   <-chan zk.Event
		allStores          map[string]StoreList
		stopper            chan struct{}
		store              *Store
		err                error
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

		for _, store = range allStores[p.ID] {
			go func(stopper chan struct{}, store *Store) {
				for {
					if err = p.probeStore(store); err != nil {
						log.Errorf("probeStore() called error(%v)", err)
					}
					select {
						case <- stopper:
							break
						case <- time.After(p.config.ProbeInterval):
					}
				}
			}(stopper, store)
		}


		select {
//		case <-p.stopper:
//			close(stopper)
//			return

		case <-storeChanges:
			log.Infof("Triggering rebalance due to store list change")
			close(stopper)

		case <-pitchforkChanges:
			log.Infof("Triggering rebalance due to pitchfork list change")
			close(stopper)
		}
	}
}


