package main
import (
	"github.com/Terry-Mao/bfs/libs/meta"
	"errors"
)

// Dispatcher
// get raw data and processed into memory for http reqs
type Dispatcher struct {
	gidR      map[string]bool  // for read
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
func (d *Dispatcher) Init(dr *Directory) (err error) {
	var (
		gid,store,volume         string
		stores                   []string
		storeMeta                *meta.Store
		volumeState              *meta.VolumeState
		writable,readable        bool
		totalAdd,totalAddDelay   uint64
		restSpace,minScore,score uint32
	)
	for gid, stores = range dr.gidStores {
		writable = readable = true
		for _, store = range stores {
			storeMeta = dr.idStore[store]
			if storeMeta.Status == meta.StoreStatusEnable || storeMeta.Status == meta.StoreStatusRead {
				writable = false
			}
			if storeMeta.Status == meta.StoreStatusEnable {
				readable = false
			}
		}
		if writable {
			d.gidStatus[gid] = meta.StoreStatusHealth
			for _, store = range stores {
				totalAdd = totalAddDelay = restSpace = minScore = 0
				for _, volume = range dr.idVolumes[store] {
					volumeState = dr.vidVolume[volume]
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
		if readable {
			d.gidR[gid] = true
		}
	}
	return
}

// cal_score algorithm of calculating score      bug: 0ms   todo:
func (d *Dispatcher) calScore(totalAdd, totalAddDelay uint64, restSpace uint32) int {
	return int((restSpace / uint32(spaceBenchmark)) * restSpaceRate + ((addDelayMaxScore / (((uint32(totalAddDelay) / nanoToMs) / uint32(totalAdd)) / addDelayBenchmark)) * addDelayRate)
}

// WritableStores get suitable stores for writing
func (d *Dispatcher) WritableStores() (stores []string, vid string, err error) {
	var (
		store,gid     string
		ok            bool
	)
	//get gid
	if stores, ok = d.dr.gidStores[gid]; !ok {
		return nil, nil, errors.New("gid cannot mathc stores")
	}
	if len(stores) > 0 {
		store = stores[0]
		vid = (d.gidWIndex[gid] + 1) % len(d.dr.idVolumes[store])
		d.gidWIndex[gid] = vid
	}
	return
}

// ReadableStores get suitable stores for reading
func (d *Dispatcher) ReadableStores(vid string) (stores []string, err error) {
	var (
		store,gid    string
		ok           bool
	)
	if stores, ok = d.dr.vidStores[vid]; !ok {
		return nil, errors.New("vid cannot match stores")
	}
	if len(stores) > 0 {
		store = stores[0]
		if gid, ok = d.dr.idGroup[store]; !ok {
			return nil, errors.New("idGroup internal error")
		}
		if _, ok = d.gidR[gid]; !ok {
			return nil, errors.New("vid of group cannot read")
		}
	}
	return
}

