package main
import "github.com/Terry-Mao/bfs/libs/meta"

/*
// StoreState contains store state data
type StoreState struct {
	Id              string
	restSpace       int           //rest space of store
	avgResponseTime float         //average response time of write req
	numReqs         int           //num reqs of interval time
	score           int           //score effect probability of being chosen
}*/

// 
type Dispatcher struct {
	gidReadable   []string   // for read
	gidScore      map[string]int   // for write
	dr            *Directory
}

// 
func (d Dispatcher) Init(dr *Directory) err error {
	var (
		gid,store,volume  string
		stores            []string
		storeMata         *meta.Store
		volumeMeta        *meta.Volume
		writable,readable bool
		numReqs,restSpace,minScore,score int
		avgResponseTime   float
	)
	for gid, stores = range dr.gidStores {
		writable = readable = true
		for _, store = range stores {
			storeMata = dr.idStore[store]
			if storeMata.Status == meta.StoreStatusEnable || storeMeta.Status == meta.StoreStatusRead {
				writable = false
			}
			if storeMeta.Status != meta.StoreStatusEnable {
				readable = false
			}
		}
		if writable {
			d.gidStatus[gid] = meta.StoreStatusHealth
			for _, store = range stores {
				numReqs = restSpace = minScore = avgResponseTime = 0
				for _, volume = range dr.idVolumes[store] {
					volumeMeta = dr.vidVolume[volume]
					numReqs = numReqs + volumeMeta.NumReqs
					restSpace = restSpace + volumeMeta.RestSpace
					avgResponseTime = volumeMeta.AvgResponseTime * volumeMeta.NumReqs
				}
				avgResponseTime = avgResponseTime / numReqs
				score = d.calScore(numReqs, restSpace, avgResponseTime)
				if score < minScore {
					minScore = score
				}
			}
			d.gidScore[gid] = minScore
		}
		if readable {
			d.gidReadable = append(d.gidReadable, gid)
		}
	}
	return
}

// cal_score algorithm of calculating score
func (d *Dispatcher) calScore(numReqs, restSpace int, avgResponseTime float) score int {
	//
	return
}

// WritableStoreGroup get suitable stores for writing
func (d *Dispatcher) WritableStores() ([]*meta.Store, err error) {
	// gidScore
}

// ReadableStoreGroup get suitable stores for reading
func (d *Dispatcher) ReadableStores(vid int) ([]*meta.Store, err error) {
	// gidReadable
}

