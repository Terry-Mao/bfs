package main

import (
	"bfs/libs/errors"
	"bfs/store/volume"
	log "github.com/golang/glog"
	"net/http"
	"strconv"
	"time"
)

// startAdmin start admin http listen.
func (s *Server) startAdmin() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    s.conf.AdminListen,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	serveMux.HandleFunc("/probe", s.probe)
	serveMux.HandleFunc("/bulk_volume", s.bulkVolume)
	serveMux.HandleFunc("/compact_volume", s.compactVolume)
	serveMux.HandleFunc("/add_volume", s.addVolume)
	serveMux.HandleFunc("/add_free_volume", s.addFreeVolume)
	if err = server.Serve(s.adminSvr); err != nil {
		log.Errorf("server.Serve() error(%v)", err)
	}
	log.Info("http admin stop")
	return
}

func (s *Server) probe(wr http.ResponseWriter, r *http.Request) {
	var (
		v      *volume.Volume
		err    error
		vid    int64
		ret    = http.StatusOK
		params = r.URL.Query()
		now    = time.Now()
	)
	if r.Method != "HEAD" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpGetWriter(r, wr, now, &err, &ret)
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		ret = http.StatusBadRequest
		return
	}
	if v = s.store.Volumes[int32(vid)]; v != nil {
		if err = v.Probe(); err != nil {
			if err == errors.ErrNeedleDeleted || err == errors.ErrNeedleNotExist {
				ret = http.StatusNotFound
			} else {
				ret = http.StatusInternalServerError
			}
		}
	} else {
		ret = http.StatusNotFound
		err = errors.ErrVolumeNotExist
	}
	return
}

func (s *Server) bulkVolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err          error
		vid          int64
		bfile, ifile string
		res          = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	bfile = r.FormValue("bfile")
	ifile = r.FormValue("ifile")
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}
	go func() {
		log.Infof("bulk volume: %d start", vid)
		err = s.store.BulkVolume(int32(vid), bfile, ifile)
		log.Infof("bulk volume: %d stop", vid)
	}()
	return
}

func (s *Server) compactVolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
		vid int64
		res = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}
	// long time processing, not block, we can from info stat api get status.
	go func() {
		log.Infof("compact volume: %d start", vid)
		if err = s.store.CompactVolume(int32(vid)); err != nil {
			log.Errorf("s.CompactVolume() error(%v)", err)
		}
		log.Infof("compact volume: %d stop", vid)
	}()
	return
}

func (s *Server) addVolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
		vid int64
		res = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}
	log.Infof("add volume: %d", vid)
	_, err = s.store.AddVolume(int32(vid))
	return
}

func (s *Server) addFreeVolume(wr http.ResponseWriter, r *http.Request) {
	var (
		err        error
		sn         int
		n          int64
		bdir, idir string
		res        = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	bdir, idir = r.FormValue("bdir"), r.FormValue("idir")
	if n, err = strconv.ParseInt(r.FormValue("n"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		err = errors.ErrParam
		return
	}
	log.Infof("add free volume: %d", n)
	sn, err = s.store.AddFreeVolume(int(n), bdir, idir)
	res["succeed"] = sn
	return
}
