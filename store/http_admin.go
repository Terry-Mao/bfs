package main

import (
	"github.com/Terry-Mao/bfs/store/errors"
	log "github.com/golang/glog"
	"net/http"
	"strconv"
	"time"
)

// StartAdmin start admin http listen.
func StartAdmin(addr string, s *Store) {
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
		ok           bool
		err          error
		storeErr     errors.StoreError
		vid          int64
		bfile, ifile string
		res          = map[string]interface{}{"ret": errors.RetOK}
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
		res["ret"] = errors.RetParamErr
		return
	}
	if err = h.s.BulkVolume(int32(vid), bfile, ifile); err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
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
		res = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	// long time processing, not block, we can from info stat api get status.
	go func() {
		if err = h.s.CompactVolume(int32(vid)); err != nil {
			log.Errorf("s.CompactVolume() error(%v)", err)
		}
	}()
	res["ret"] = errors.RetOK
	return
}

// httpAddVolumeHandler http compact block.
type httpAddVolumeHandler struct {
	s *Store
}

func (h httpAddVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		err      error
		storeErr errors.StoreError
		vid      int64
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	if _, err = h.s.AddVolume(int32(vid)); err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	return
}

// httpAddFreeVolumeHandler http compact block.
type httpAddFreeVolumeHandler struct {
	s *Store
}

func (h httpAddFreeVolumeHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok         bool
		storeErr   errors.StoreError
		err        error
		sn         int
		n          int64
		bdir, idir string
		res        = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	bdir, idir = r.FormValue("bdir"), r.FormValue("idir")
	if n, err = strconv.ParseInt(r.FormValue("n"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	if sn, err = h.s.AddFreeVolume(int(n), bdir, idir); err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	res["succeed"] = sn
	return
}
