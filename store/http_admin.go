package main

import (
	log "github.com/golang/glog"
	"net/http"
	"strconv"
	"time"
)

// StartAdmin start admin http listen.
func StartAdmin(s *Store, addr string) {
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.Handle("/bulk_volume", httpBulkVolumeHandler{s: s})
		serveMux.Handle("/compact_volume", httpCompactVolumeHandler{s: s})
		serveMux.Handle("/add_volume", httpAddVolumeHandler{s: s})
		serveMux.Handle("/add_free_volume", httpAddFreeVolumeHandler{s: s})
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

// httpBulkVolumeHandler http bulk block.
type httpBulkVolumeHandler struct {
	s *Store
}

func (h httpBulkVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err          error
		vid          int64
		bfile, ifile string
		res          = map[string]interface{}{"ret": RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	bfile = r.FormValue("bfile")
	ifile = r.FormValue("ifile")
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = RetParamErr
		return
	}
	if err = h.s.Bulk(int32(vid), bfile, ifile); err != nil {
		res["ret"] = RetBulkErr
	}
	return
}

// httpCompactVolumeHandler http compact block.
type httpCompactVolumeHandler struct {
	s *Store
}

func (h httpCompactVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
		vid int64
		res = map[string]interface{}{"ret": RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = RetParamErr
		return
	}
	// long time processing, not block, we can from info stat api get status.
	go func() {
		if err = h.s.Compact(int32(vid)); err != nil {
		}
	}()
	res["ret"] = RetOK
	return
}

// httpAddVolumeHandler http compact block.
type httpAddVolumeHandler struct {
	s *Store
}

func (h httpAddVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
		vid int64
		res = map[string]interface{}{"ret": RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = RetParamErr
		return
	}
	if _, err = h.s.AddVolume(int32(vid)); err != nil {
		res["ret"] = RetAddVolumeErr
	}
	return
}

// httpAddFreeVolumeHandler http compact block.
type httpAddFreeVolumeHandler struct {
	s *Store
}

func (h httpAddFreeVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err        error
		sn         int
		n          int64
		bdir, idir string
		res        = map[string]interface{}{"ret": RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	bdir, idir = r.FormValue("bdi"), r.FormValue("idir")
	if n, err = strconv.ParseInt(r.FormValue("n"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = RetParamErr
		return
	}
	if sn, err = h.s.AddFreeVolume(int(n), bdir, idir); err != nil {
		res["ret"] = RetAddVolumeErr
	}
	res["succeed"] = sn
	return
}
