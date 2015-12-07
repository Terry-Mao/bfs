package main
import (
	"github.com/Terry-Mao/bfs/libs/meta"
	"errors"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gidScore  map[int32]uint32   // for write  gid:score
	gidWIndex map[int32]int      // volume index  directory:idVolumes[store][index] =>volume id
	gids      []int32
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
	d.gidScore = make(map[int32]uint32)
	d.gidWIndex = make(map[int32]int)
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
		gid, vid                 int32
		gids                     []int32
		restSpace,minScore,score uint32
	)
	gids = make([]int32)
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
				for _, vid = range d.dr.idVolumes[store] {
					volumeState = d.dr.vidVolume[vid]
					totalAdd = totalAdd + volumeState.TotalAddProcessed
					restSpace = restSpace + volumeState.FreeSpace
					totalAddDelay = totalAddDelay + volumeState.TotalAddDelay
				}
				score = d.calScore(uint32(totalAdd), restSpace, uint32(totalAddDelay))
				if score < minScore {
					minScore = score
				}
			}
			d.gidScore[gid] = minScore
			gids = append(gids, gid)
		}
	}
	d.gids = gids
	return
}

// cal_score algorithm of calculating score
func (d *Dispatcher) calScore(totalAdd, totalAddDelay, restSpace uint32) uint32 {
	//score = rsScore + (-adScore)   when adScore==0 means ignored
	var (
		rsScore, adScore   uint32
	)
	rsScore = uint32(restSpace / spaceBenchmark)
	if totalAdd == 0 {
		adScore = 0 // ignored
	}
	adScore = uint32(((totalAddDelay / nsToMs) / totalAdd) / addDelayBenchmark)
	//rsScore < adScore todo
	return rsScore - adScore
}

// WStores get suitable stores for writing
func (d *Dispatcher) WStores() (stores []string, vid int32, err error) {
	var (
		store     string
		gid       int32
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
func (d *Dispatcher) RStores(vid int32) (stores []string, err error) {
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
			delete(stores, store)
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
