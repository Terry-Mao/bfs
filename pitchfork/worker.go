package main

import (
    "time"
    "sort"
    "errors"

    log "github.com/golang/glog"
    "github.com/samuel/go-zookeeper/zk"
)

//Work main flow of pitchfork server
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
							return
						case <- time.After(p.config.ProbeInterval):
					}
				}
			}(stopper, store)
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
			result[pitchfork.ID] = append(result[pitchfork.ID], store)
		}
		p = last
	}

	return result, nil
}