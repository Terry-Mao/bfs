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

func StartStat(addr string, s *Server) {
	var info = &stat.Info{
		Ver:       Ver,
		GitSHA1:   GitSHA1,
		StartTime: time.Now(),
		Stats:     &stat.Stats{},
	}
	s.info = info
	go s.startStat()
	http.HandleFunc("/info", s.stat)
	go func() {
		http.ListenAndServe(addr, nil)
	}()
	return
}

func (s *Server) stat(wr http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(wr, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var (
		err     error
		data    []byte
		v       *volume.Volume
		volumes = make([]*volume.Volume, 0, len(s.store.Volumes))
		res     = map[string]interface{}{"ret": errors.RetOK}
	)
	for _, v = range s.store.Volumes {
		volumes = append(volumes, v)
	}
	res["server"] = s.info
	res["volumes"] = volumes
	res["free_volumes"] = s.store.FreeVolumes
	if data, err = json.Marshal(res); err == nil {
		if _, err = wr.Write(data); err != nil {
			log.Errorf("wr.Write() error(%v)", err)
		}
	} else {
		log.Errorf("json.Marshal() error(%v)", err)
	}
	return
}

// startStat stat the store.
func (s *Server) startStat() {
	var (
		v     *volume.Volume
		stat1 *stat.Stats
		stat  = new(stat.Stats)
	)
	for {
		*stat = *(s.info.Stats)
		stat1 = s.info.Stats
		s.info.Stats = stat
		stat1.Reset()
		for _, v = range s.store.Volumes {
			v.Stats.Calc()
			stat1.Merge(v.Stats)
		}
		stat1.Calc()
		s.info.Stats = stat1
		time.Sleep(statDuration)
	}
}
