package main

import (
	"bfs/libs/errors"
	"bfs/libs/stat"
	"bfs/store/volume"
	"encoding/json"
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
			err     error
			data    []byte
			v       *volume.Volume
			volumes = make([]*volume.Volume, 0, len(s.Volumes))
			res     = map[string]interface{}{"ret": errors.RetOK}
		)
		for _, v = range s.Volumes {
			volumes = append(volumes, v)
		}
		res["server"] = info
		res["volumes"] = volumes
		res["free_volumes"] = s.FreeVolumes
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
		v     *volume.Volume
		stat1 *stat.Stats
		stat  = new(stat.Stats)
	)
	for {
		*stat = *(info.Stats)
		stat1 = info.Stats
		info.Stats = stat
		stat1.Reset()
		for _, v = range s.Volumes {
			v.Stats.Calc()
			stat1.Merge(v.Stats)
		}
		stat1.Calc()
		info.Stats = stat1
		time.Sleep(statDuration)
	}
}
