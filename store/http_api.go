package main

import (
	"bfs/libs/errors"
	"bfs/store/needle"
	"bfs/store/volume"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"strconv"
	"time"
)

// startApi start api http listen.
func (s *Server) startApi() {
	var (
		err      error
		serveMux = http.NewServeMux()
		server   = &http.Server{
			Addr:    s.conf.ApiListen,
			Handler: serveMux,
			// TODO read/write timeout
		}
	)
	serveMux.HandleFunc("/get", s.get)
	serveMux.HandleFunc("/upload", s.upload)
	serveMux.HandleFunc("/uploads", s.uploads)
	serveMux.HandleFunc("/del", s.del)
	if err = server.Serve(s.apiSvr); err != nil {
		log.Errorf("server.Serve() error(%v)", err)
	}
	log.Info("http api stop")
	return
}

func (s *Server) get(wr http.ResponseWriter, r *http.Request) {
	var (
		v                *volume.Volume
		n                *needle.Needle
		err              error
		vid, key, cookie int64
		ret              = http.StatusOK
		params           = r.URL.Query()
		now              = time.Now()
	)
	if r.Method != "GET" && r.Method != "HEAD" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpGetWriter(r, wr, now, &err, &ret)
	if !s.rl.Allow() {
		ret = http.StatusServiceUnavailable
		return
	}
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		ret = http.StatusBadRequest
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("key"), err)
		ret = http.StatusBadRequest
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("cookie"), err)
		ret = http.StatusBadRequest
		return
	}
	if v = s.store.Volumes[int32(vid)]; v != nil {
		if n, err = v.Read(key, int32(cookie)); err == nil {
			wr.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
			if _, err = wr.Write(n.Data); err != nil {
				log.Errorf("wr.Write() error(%v)", err)
				err = nil // avoid HttpGetWriter write header twice
			}
			n.Close()
		} else {
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

func (s *Server) upload(wr http.ResponseWriter, r *http.Request) {
	var (
		vid    int64
		key    int64
		cookie int64
		size   int64
		err    error
		str    string
		v      *volume.Volume
		n      *needle.Needle
		file   multipart.File
		res    = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if !s.wl.Allow() {
		err = errors.ErrServiceUnavailable
		return
	}
	if err = checkContentLength(r, s.conf.NeedleMaxSize); err != nil {
		return
	}
	str = r.FormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	str = r.FormValue("key")
	if key, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	str = r.FormValue("cookie")
	if cookie, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	if file, _, err = r.FormFile("file"); err != nil {
		log.Errorf("r.FormFile() error(%v)", err)
		err = errors.ErrInternal
		return
	}
	if size, err = checkFileSize(file, s.conf.NeedleMaxSize); err == nil {
		if v = s.store.Volumes[int32(vid)]; v != nil {
			n = needle.NewWriter(key, int32(cookie), int32(size))
			if err = n.ReadFrom(file); err == nil {
				err = v.Write(n)
			}
			n.Close()
		} else {
			err = errors.ErrVolumeNotExist
		}
	}
	file.Close()
	return
}

func (s *Server) uploads(wr http.ResponseWriter, r *http.Request) {
	var (
		i, nn   int
		err     error
		vid     int64
		key     int64
		cookie  int64
		size    int64
		str     string
		keys    []string
		cookies []string
		v       *volume.Volume
		file    multipart.File
		fh      *multipart.FileHeader
		fhs     []*multipart.FileHeader
		ns      *needle.Needles
		res     = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if !s.wl.Allow() {
		err = errors.ErrServiceUnavailable
		return
	}
	str = r.FormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	keys = r.MultipartForm.Value["keys"]
	cookies = r.MultipartForm.Value["cookies"]
	if len(keys) != len(cookies) {
		log.Errorf("param length not match, keys: %d, cookies: %d", len(keys), len(cookies))
		err = errors.ErrParam
		return
	}
	fhs = r.MultipartForm.File["file"]
	nn = len(fhs)
	if len(keys) != nn {
		log.Errorf("param length not match, keys: %d, cookies: %d, files: %d", len(keys), len(cookies), len(fhs))
		err = errors.ErrParam
		return
	}
	ns = needle.NewNeedles(nn)
	for i, fh = range fhs {
		if key, err = strconv.ParseInt(keys[i], 10, 64); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", keys[i], err)
			err = errors.ErrParam
			break
		}
		if cookie, err = strconv.ParseInt(cookies[i], 10, 32); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", cookies[i], err)
			err = errors.ErrParam
			break
		}
		if file, err = fh.Open(); err != nil {
			log.Errorf("fh.Open() error(%v)", err)
			break
		}
		if size, err = checkFileSize(file, s.conf.NeedleMaxSize); err == nil {
			err = ns.ReadFrom(key, int32(cookie), int32(size), file)
		}
		file.Close()
		if err != nil {
			break
		}
	}
	if err == nil {
		if v = s.store.Volumes[int32(vid)]; v != nil {
			err = v.Writes(ns)
		} else {
			err = errors.ErrVolumeNotExist
		}
	}
	ns.Close()
	return
}

func (s *Server) del(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		key, vid int64
		str      string
		v        *volume.Volume
		res      = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	if !s.dl.Allow() {
		err = errors.ErrServiceUnavailable
		return
	}
	str = r.PostFormValue("key")
	if key, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	str = r.PostFormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		err = errors.ErrParam
		return
	}
	if v = s.store.Volumes[int32(vid)]; v != nil {
		err = v.Delete(key)
	} else {
		err = errors.ErrVolumeNotExist
	}
	return
}
