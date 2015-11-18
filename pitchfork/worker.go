package main

import (
	log "github.com/golang/glog"
	"net/http"
	"os"
	"time"
)


func Work(pf *Pitchfork) {
	for {
		stores, storeChanges, err := pf.WatchGetStores()
		if err != nil {
			//to do log
			return
		}

		pitchforks, pitchforkChanges, err := pf.WatchGetPitchfork()
		if err != nil {
			//to do log
			return
		}

		myStores, err = divideStoreBetweenPitchfork(pitchforks, stores)

		stopper := make(chan struct{})

		go func(stopper <-chan struct{}) {
			for _, store := range myStores {
				if result, err := store.probeStore(); err != nil {
					log.Errorf("")
					continue
				}
				pf.feedbackDirectory(result)
			}

			select {
			case <- stopper:
				return
			case <- time.After(pf.config.ProbeInterval * time.Second):
			}
		}(stopper)


		select {
		case <-pf.stopper:
			close(stopper)
			return

		case <-storeChanges:
			log.Infof("Triggering rebalance due to store list change")
			close(stopper)

		case <-pitchforkChanges:
			log.Infof("Triggering rebalance due to pitchfork list change")
			close(stopper)
//			pf.wg.Wait()
		}
	}
}


