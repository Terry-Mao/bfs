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

type server struct {
	d *Directory
}

// StartApi start api http listen.
func StartApi(addr string, d *Directory) {
	var s = &server{d: d}
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.HandleFunc("/get", s.get)
		serveMux.HandleFunc("/upload", s.upload)
		serveMux.HandleFunc("/del", s.del)
		serveMux.HandleFunc("/ping", s.ping)
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

func (s *server) get(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		bucket   string
		filename string
		res      meta.Response
		n        *meta.Needle
		f        *meta.File
		uerr     errors.Error
		err      error
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
	if n, f, res.Stores, err = s.d.GetStores(bucket, filename); err != nil {
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
	res.Mine = f.Mine
	if f.MTime != 0 {
		res.MTime = f.MTime
	} else {
		res.MTime = n.MTime
	}
	res.Sha1 = f.Sha1
	return
}

func (s *server) upload(wr http.ResponseWriter, r *http.Request) {
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
	if n, res.Stores, err = s.d.UploadStores(bucket, f); err != nil {
		if err == errors.ErrNeedleExist {
			// update file data
			res.Ret = errors.RetNeedleExist
			if n, _, res.Stores, err = s.d.GetStores(bucket, f.Filename); err != nil {
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

func (s *server) del(wr http.ResponseWriter, r *http.Request) {
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
	if n, res.Stores, err = s.d.DelStores(bucket, filename); err != nil {
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

func (s *server) ping(wr http.ResponseWriter, r *http.Request) {
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
