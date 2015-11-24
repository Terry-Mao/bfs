package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/store/stat"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

const (
	statDuration = 1 * time.Second
)

func StartStat(addr string, s *Store) {
	var info = &stat.Info{
		Ver:       Ver,
		GitSHA1:   GitSHA1,
		StartTime: time.Now(),
		Stats:     &stat.Stats{},
	}
	go startStat(s, info)
	http.HandleFunc("/info", func(wr http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(wr, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var (
			v           *Volume
			err         error
			data        []byte
			volumes     []*Volume
			freeVolumes []*Volume
			res         = map[string]interface{}{"ret": errors.RetOK}
		)
		s.RLockVolume()
		volumes = make([]*Volume, 0, len(s.Volumes))
		for _, v = range s.Volumes {
			volumes = append(volumes, v)
		}
		s.RUnlockVolume()
		s.RLockFreeVolume()
		freeVolumes = make([]*Volume, 0, len(s.FreeVolumes))
		for _, v = range s.FreeVolumes {
			freeVolumes = append(freeVolumes, v)
		}
		s.RUnlockFreeVolume()
		res["server"] = info
		res["volumes"] = volumes
		res["free_volumes"] = freeVolumes
		if data, err = json.Marshal(res); err == nil {
			if _, err = wr.Write(data); err != nil {
				log.Errorf("wr.Write() error(%v)", err)
			}
		} else {
			log.Errorf("json.Marshal() error(%v)", err)
		}
		return
	})
	go func() {
		http.ListenAndServe(addr, nil)
	}()
	return
}

// startStat stat the store.
func startStat(s *Store, info *stat.Info) {
	var (
		v     *Volume
		stat1 *stat.Stats
		stat  = new(stat.Stats)
	)
	for {
		*stat = *(info.Stats)
		stat1 = info.Stats
		info.Stats = stat
		stat1.Reset()
		s.RLockVolume()
		for _, v = range s.Volumes {
			v.Stats.Calc()
			stat1.Merge(v.Stats)
		}
		s.RUnlockVolume()
		stat1.Calc()
		info.Stats = stat1
		time.Sleep(statDuration)
	}
}
