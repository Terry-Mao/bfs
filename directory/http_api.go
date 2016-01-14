package main

import (
	"github.com/Terry-Mao/bfs/libs/errors"
	log "github.com/golang/glog"
	"net/http"
	"strconv"
	"time"
)

// StartApi start api http listen.
func StartApi(addr string, d *Directory) {
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.Handle("/get", httpGetHandler{d: d})
		serveMux.Handle("/upload", httpUploadHandler{d: d})
		serveMux.Handle("/del", httpDelHandler{d: d})
		serveMux.Handle("/ping", httpPingHandler{})
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

// httpGetHandler http upload a file.
type httpGetHandler struct {
	d *Directory
}

func (h httpGetHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		key    int64
		cookie int64
		res    GetResponse
		params = r.URL.Query()
		ok     bool
		uerr   errors.Error
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%d\") error(%v)", r.FormValue("key"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%d\") error(%v)", r.FormValue("cookie"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpGetWriter(r, wr, time.Now(), &res)
	if res.Vid, res.Stores, err = h.d.GetStores(key, int32(cookie)); err != nil {
		log.Errorf("GetStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
	}
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	d *Directory
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err  error
		num  int64
		res  UploadResponse
		ok   bool
		uerr errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if num, err = strconv.ParseInt(r.FormValue("num"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%d\") error(%v)", r.FormValue("num"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpUploadWriter(r, wr, time.Now(), &res)
	if res.Keys, res.Vid, res.Stores, err = h.d.UploadStores(int(num)); err != nil {
		log.Errorf("UploadStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
	}
	return
}

// httpDelHandler
type httpDelHandler struct {
	d *Directory
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		cookie int64
		key    int64
		res    DelResponse
		ok     bool
		uerr   errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if key, err = strconv.ParseInt(r.FormValue("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%d\") error(%v)", r.FormValue("key"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if cookie, err = strconv.ParseInt(r.FormValue("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%d\") error(%v)", r.FormValue("cookie"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpDelWriter(r, wr, time.Now(), &res)
	if res.Vid, res.Stores, err = h.d.DelStores(key, int32(cookie)); err != nil {
		log.Errorf("DelStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
	}
	return
}

// httpPingHandler http ping health
type httpPingHandler struct {
}

func (h httpPingHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	return
}