package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/stat"
	"net/http"
	"sort"
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
			v       *Volume
			err     error
			vid     int32
			ok      bool
			data    []byte
			res     = map[string]interface{}{"ret": errors.RetOK}
			vids    = make([]int32, 0, len(s.Volumes))
			volumes = make([]*Volume, 0, len(s.Volumes))
		)
		for vid, v = range s.Volumes {
			vids = append(vids, vid)
		}
		sort.Sort(Int32Slice(vids))
		for _, vid = range vids {
			if v, ok = s.Volumes[vid]; ok {
				volumes = append(volumes, v)
			}
		}
		res["server"] = info
		res["volumes"] = volumes
		res["free_volumes"] = s.FreeVolumes
		if data, err = json.Marshal(res); err != nil {
			wr.Write(data)
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
		for _, v = range s.Volumes {
			v.Stats.Calc()
			stat1.Merge(v.Stats)
		}
		stat1.Calc()
		info.Stats = stat1
		time.Sleep(statDuration)
	}
}
