package main
import (
	"github.com/Terry-Mao/bfs/libs/meta"
	"errors"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gidW      map[string]int   // for write
	gidWIndex map[string]int   // volume index  directory:idVolumes[store][index] =>volume id
	dr        *Directory
}

const (
	nanoToMs = 1000000   // ns ->  ms
	maxRate = 10000
	restSpaceRate = 6000
	addDelayRate = maxRate - restSpaceRate
	spaceBenchmark = meta.MaxBlockOffset // 1 volume
	addDelayBenchmark = 1 // 1ms   <1ms means no load
)

// 
func NewDispatcher(dr *Directory) (d *Dispatcher) {
	d = new(Dispatcher)
	d.dr = dr
	d.gidW = make(map[string]int)
	d.gidWIndex = make(map[string]int)
	return
}

// Update when zk updates
func (d *Dispatcher) Update() (err error) {
	var (
		gid,store,volume         string
		stores                   []string
		storeMeta                *meta.Store
		volumeState              *meta.VolumeState
		writable,ok              bool
		totalAdd,totalAddDelay   uint64
		restSpace,minScore,score uint32
	)
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
				totalAdd = totalAddDelay = restSpace = minScore = 0
				for _, volume = range d.dr.idVolumes[store] {
					volumeState = d.dr.vidVolume[volume]
					totalAdd = totalAdd + volumeState.TotalAddProcessed
					restSpace = restSpace + volumeState.FreeSpace
					totalAddDelay = totalAddDelay + volumeState.TotalAddDelay
				}
				score = d.calScore(totalAdd, restSpace, totalAddDelay)
				if score < minScore {
					minScore = score
				}
			}
			d.gidW[gid] = minScore
		}
	}
	return
}

// cal_score algorithm of calculating score      bug: 0ms   todo
func (d *Dispatcher) calScore(totalAdd, totalAddDelay uint64, restSpace uint32) int {
	return int((restSpace / uint32(spaceBenchmark)) * restSpaceRate + ((addDelayMaxScore / (((uint32(totalAddDelay) / nanoToMs) / uint32(totalAdd)) / addDelayBenchmark)) * addDelayRate)
}

// WStores get suitable stores for writing
func (d *Dispatcher) WStores() (stores []string, vid string, err error) {
	var (
		store,gid     string
	)
	//get gid
	stores = d.dr.gidStores[gid]
	if len(stores) > 0 {
		store = stores[0]
		vid = (d.gidWIndex[gid] + 1) % len(d.dr.idVolumes[store])
		d.gidWIndex[gid] = vid
	}
	return
}

// RStores get suitable stores for reading
func (d *Dispatcher) RStores(vid string) (stores []string, err error) {
	var (
		store,gid    string
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
			delete(stores, store)
		}
	}
	return
}

// DStores get suitable stores for delete 
func (d *Dispatcher) DStores(vid string) (stores []string, err error) {
	var (
		store,gid    string
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
