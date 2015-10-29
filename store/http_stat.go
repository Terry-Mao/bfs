package main

import (
	"net/http"
	"sort"
	"time"
)

func StartStat(s *Store, addr string) {
	http.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var (
			v       *Volume
			vid     int32
			ok      bool
			res     = map[string]interface{}{"ret": RetOK}
			vids    = make([]int32, 0, len(s.Volumes))
			volumes = make([]*Volume, 0, len(s.Volumes))
		)
		defer HttpGetWriter(r, w, time.Now(), res)
		for vid, v = range s.Volumes {
			vids = append(vids, vid)
		}
		sort.Sort(Int32Slice(vids))
		for _, vid = range vids {
			if v, ok = s.Volumes[vid]; ok {
				volumes = append(volumes, v)
			}
		}
		res["server"] = s.Info
		res["volumes"] = volumes
		res["free_volumes"] = s.FreeVolumes
		return
	})
	go func() {
		http.ListenAndServe(addr, nil)
	}()
	return
}
