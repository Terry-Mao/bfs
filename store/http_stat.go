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

func (s *Server) startStat() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    s.conf.StatListen,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	s.info = &stat.Info{
		Ver:       Ver,
		GitSHA1:   GitSHA1,
		StartTime: time.Now(),
		Stats:     &stat.Stats{},
	}
	go s.statproc()
	serveMux.HandleFunc("/info", s.stat)
	if err = server.Serve(s.statSvr); err != nil {
		log.Errorf("server.Serve() error(%v)", err)
	}
	log.Info("http stat stop")
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

// statproc stat the store.
func (s *Server) statproc() {
	var (
		v    *volume.Volume
		olds *stat.Stats
		news = new(stat.Stats)
	)
	for {
		olds = s.info.Stats
		*news = *olds
		s.info.Stats = news // use news instead, for current display
		olds.Reset()
		for _, v = range s.store.Volumes {
			v.Stats.Calc()
			olds.Merge(v.Stats)
		}
		olds.Calc()
		s.info.Stats = olds
		time.Sleep(statDuration)
	}
}
