package main
import (
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"sort"
	"math/rand"
	"errors"
	"time"
	"fmt"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gidScore  map[int]int   // for write  gid:score
	gidWIndex map[int]int      // volume index  directory:idVolumes[store][index] =>volume id
	gids      []int
	dr        *Directory
}

const (
	nsToMs = 1000000                     // ns ->  us
	spaceBenchmark = meta.MaxBlockOffset // 1 volume
	addDelayBenchmark = 1                // 1ms   <1ms means no load, adScore==0
)

// NewDispatcher
func NewDispatcher(dr *Directory) (d *Dispatcher) {
	d = new(Dispatcher)
	d.dr = dr
	d.gidScore = make(map[int]int)
	d.gidWIndex = make(map[int]int)
	return
}

// Update when zk updates
func (d *Dispatcher) Update() (err error) {
	var (
		store                    string
		stores                   []string
		storeMeta                *meta.Store
		volumeState              *meta.VolumeState
		writable,ok              bool
		totalAdd,totalAddDelay   uint64
		restSpace,minScore,score int
		gid,sum                  int
		vid                      int32
		gids                     []int
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
				totalAdd ,totalAddDelay ,restSpace ,minScore = 0, 0, 0, 0
				for _, vid = range d.dr.idVolumes[store] {
					volumeState = d.dr.vidVolume[vid]
					totalAdd = totalAdd + volumeState.TotalAddProcessed
					restSpace = restSpace + int(volumeState.FreeSpace)
					totalAddDelay = totalAddDelay + volumeState.TotalAddDelay
				}
				score = d.calScore(int(totalAdd), int(totalAddDelay), restSpace)
				if score < minScore {
					minScore = score
				}
			}
			d.gidScore[gid] = minScore
			gids = append(gids, gid)
		}
	}
	sort.Ints(gids)
	d.gids = gids
	for _, gid = range gids {
		sum += d.gidScore[gid]
		d.gidScore[gid] = sum
	}
	return
}

// cal_score algorithm of calculating score
func (d *Dispatcher) calScore(totalAdd, totalAddDelay ,restSpace int) int {
	//score = rsScore + (-adScore)   when adScore==0 means ignored
	var (
		rsScore, adScore   int
	)
	rsScore = (restSpace / int(spaceBenchmark))
	if totalAdd != 0 {
		adScore = ((totalAddDelay / nsToMs) / totalAdd) / addDelayBenchmark
	}
	//rsScore < adScore todo
	return rsScore - adScore
}

// WStores get suitable stores for writing
func (d *Dispatcher) WStores() (stores []string, vid int32, err error) {
	var (
		store                string
		gid                  int
		maxScore,randomScore,score int
		r                    *rand.Rand
	)
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	maxScore = d.gidScore[len(d.gids) - 1]
	randomScore = r.Intn(maxScore)
	for gid,score = range d.gidScore {
		if randomScore < score {
			break
		}
	} // need to do  cache

	stores = d.dr.gidStores[gid]
	if len(stores) > 0 {
		store = stores[0]
		vid = (int32(d.gidWIndex[gid]) + 1) % int32(len(d.dr.idVolumes[store]))
		d.gidWIndex[gid] = int(vid)
	}
	return
}

// RStores get suitable stores for reading
func (d *Dispatcher) RStores(vid int32) (stores []string, err error) {
	var (
		store        string
		s            []string
		storeMeta    *meta.Store
		ok           bool
	)
	stores = []string{}
	if s, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New(fmt.Sprintf("vidStores cannot match vid: %s", vid))
	}
	for _, store = range s {
		if storeMeta, ok = d.dr.idStore[store]; !ok {
			log.Errorf("idStore cannot match store: %s", store)
			continue
		}
		if storeMeta.Status != meta.StoreStatusFail {
			stores = append(stores, store)
		}
	}
	return
}

// DStores get suitable stores for delete 
func (d *Dispatcher) DStores(vid int32) (stores []string, err error) {
	var (
		store        string
		storeMeta    *meta.Store
		ok           bool
	)
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
	}
	return
}
