package main

import (
	"errors"
	"fmt"
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"math/rand"
	"sync"
	"time"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gids []int // for write eg:  gid:1;2   gids: [1,1,2,2,2,2,2]
	dr   *Directory
	rp   *sync.Pool // rand pool
}

const (
	maxScore          = 1000
	nsToMs            = 1000000             // ns ->  us
	spaceBenchmark    = meta.MaxBlockOffset // 1 volume
	addDelayBenchmark = 100                 // 100ms   <100ms means no load, -Score==0
	baseAddDelay      = 100                 // 1s score:   -(1000/baseAddDelay)*addDelayBenchmark == -1000
)

// NewDispatcher
func NewDispatcher(dr *Directory) (d *Dispatcher) {
	d = new(Dispatcher)
	d.dr = dr
	d.rp = &sync.Pool{
		New: func() interface{} {
			return rand.New(rand.NewSource(time.Now().UnixNano()))
		},
	}
	return
}

// Update when zk updates
func (d *Dispatcher) Update() (err error) {
	var (
		store                      string
		stores                     []string
		storeMeta                  *meta.Store
		volumeState                *meta.VolumeState
		writable, ok               bool
		totalAdd, totalAddDelay    uint64
		restSpace, minScore, score int
		gid, i                     int
		vid                        int32
		gids                       []int
	)
	gids = []int{}
	for gid, stores = range d.dr.gidStores {
		writable = true
		for _, store = range stores {
			if storeMeta, ok = d.dr.idStore[store]; !ok {
				log.Errorf("idStore cannot match store: %s", store)
				break
			}
			if storeMeta.Status == meta.StoreStatusFail || storeMeta.Status == meta.StoreStatusRead {
				writable = false
			}
		}
		if writable {
			for _, store = range stores {
				totalAdd, totalAddDelay, restSpace, minScore = 0, 0, 0, 0
				for _, vid = range d.dr.idVolumes[store] {
					volumeState = d.dr.vidVolume[vid]
					totalAdd = totalAdd + volumeState.TotalWriteProcessed
					restSpace = restSpace + int(volumeState.FreeSpace)
					totalAddDelay = totalAddDelay + volumeState.TotalWriteDelay
				}
				score = d.calScore(int(totalAdd), int(totalAddDelay), restSpace)
				if score < minScore || minScore == 0 {
					minScore = score
				}
			}
			for i = 0; i < minScore; i++ {
				gids = append(gids, gid)
			}
		}
	}
	d.gids = gids
	return
}

// cal_score algorithm of calculating score
func (d *Dispatcher) calScore(totalAdd, totalAddDelay, restSpace int) (score int) {
	var (
		rsScore, adScore int
	)
	rsScore = (restSpace / int(spaceBenchmark))
	if rsScore > maxScore {
		rsScore = maxScore // more than 32T will be 32T and set score maxScore; less than 32G will be set 0 score;
	}
	if totalAdd != 0 {
		adScore = (((totalAddDelay / nsToMs) / totalAdd) / addDelayBenchmark) * baseAddDelay
		if adScore > maxScore {
			adScore = maxScore // more than 1s will be 1s and set score -maxScore; less than 100ms will be set -0 score;
		}
	}
	score = rsScore - adScore
	return
}

// WStores get suitable stores for writing
func (d *Dispatcher) WStores() (hosts []string, vid int32, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		gid       int
		index     int
		r         *rand.Rand
		ok        bool
	)
	if len(d.gids) == 0 {
		return nil, 0, errors.New(fmt.Sprintf("no available gid"))
	}
	r = d.rp.Get().(*rand.Rand)
	defer d.rp.Put(r)
	gid = d.gids[r.Intn(len(d.gids))]
	stores = d.dr.gidStores[gid]
	if len(stores) > 0 {
		store = stores[0]
		index = r.Intn(len(d.dr.idVolumes[store]))
		vid = int32(d.dr.idVolumes[store][index])
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			return nil, 0, errors.New(fmt.Sprintf("bad store : %s", store))
		}
		hosts = append(hosts, storeMeta.Api)
	}
	return
}

// RStores get suitable stores for reading
func (d *Dispatcher) RStores(vid int32) (hosts []string, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		ok        bool
	)
	hosts = []string{}
	if stores, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New(fmt.Sprintf("vidStores cannot match vid: %s", vid))
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			continue
		}
		if storeMeta.Status != meta.StoreStatusFail {
			hosts = append(hosts, storeMeta.Api)
		}
	}
	return
}

// DStores get suitable stores for delete
func (d *Dispatcher) DStores(vid int32) (hosts []string, err error) {
	var (
		store     string
		stores    []string
		storeMeta *meta.Store
		ok        bool
	)
	hosts = []string{}
	if stores, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New(fmt.Sprintf("vidStores cannot match vid: %s", vid))
	}
	for _, store = range stores {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			continue
		}
		if storeMeta.Status == meta.StoreStatusFail {
			return nil, errors.New(fmt.Sprintf("bad store : %s", store))
		}
		hosts = append(hosts, storeMeta.Api)
	}
	return
}
