package main

import (
	"bfs/libs/errors"
	"bfs/libs/meta"
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

const (
	_pingOk = 0
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
		err      error
		n        *meta.Needle
		bucket   string
		filename string
		res      meta.Response
		ok       bool
		uerr     errors.Error
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket = r.FormValue("bucket"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if filename = r.FormValue("filename"); filename == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpGetWriter(r, wr, time.Now(), &res)
	if n, res.Stores, err = h.d.GetStores(bucket, filename); err != nil {
		log.Errorf("GetStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	res.Key = n.Key
	res.Cookie = n.Cookie
	res.Vid = n.Vid
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	d *Directory
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err    error
		n      *meta.Needle
		f      *meta.File
		bucket string
		res    meta.Response
		ok     bool
		uerr   errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	f = new(meta.File)
	if bucket = r.FormValue("bucket"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Filename = r.FormValue("filename"); f.Filename == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Sha1 = r.FormValue("sha1"); f.Sha1 == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if f.Mine = r.FormValue("mine"); f.Mine == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpUploadWriter(r, wr, time.Now(), &res)

	res.Ret = errors.RetOK
	if n, res.Stores, err = h.d.UploadStores(bucket, f); err != nil {
		if err == errors.ErrNeedleExist {
			// update file data
			res.Ret = errors.RetNeedleExist
			if n, res.Stores, err = h.d.GetStores(bucket, f.Filename); err != nil {
				log.Errorf("GetStores() error(%v)", err)
				if uerr, ok = err.(errors.Error); ok {
					res.Ret = int(uerr)
				} else {
					res.Ret = errors.RetInternalErr
				}
				return
			}
		} else {
			log.Errorf("UploadStores() error(%v)", err)
			if uerr, ok = err.(errors.Error); ok {
				res.Ret = int(uerr)
			} else {
				res.Ret = errors.RetInternalErr
			}
			return
		}
	}
	res.Key = n.Key
	res.Cookie = n.Cookie
	res.Vid = n.Vid
	return
}

// httpDelHandler
type httpDelHandler struct {
	d *Directory
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		n        *meta.Needle
		bucket   string
		filename string
		res      meta.Response
		ok       bool
		uerr     errors.Error
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if bucket = r.FormValue("bucket"); bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if filename = r.FormValue("filename"); filename == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	defer HttpDelWriter(r, wr, time.Now(), &res)
	if n, res.Stores, err = h.d.DelStores(bucket, filename); err != nil {
		log.Errorf("DelStores() error(%v)", err)
		if uerr, ok = err.(errors.Error); ok {
			res.Ret = int(uerr)
		} else {
			res.Ret = errors.RetInternalErr
		}
		return
	}
	res.Ret = errors.RetOK
	res.Key = n.Key
	res.Cookie = n.Cookie
	res.Vid = n.Vid
	return
}

// httpPingHandler http ping health
type httpPingHandler struct {
}

func (h httpPingHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		byteJson []byte
		res      = map[string]interface{}{"code": _pingOk}
		err      error
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
	}
	return
}
