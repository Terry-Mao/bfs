package main

import (
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	"github.com/Terry-Mao/bfs/store/volume"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"time"
)

type sizer interface {
	Size() int64
}

// fileSize get multipart.File size
func fileSize(file multipart.File) (size int64, err error) {
	var (
		ok bool
		sr sizer
		fr *os.File
		fi os.FileInfo
	)
	if sr, ok = file.(sizer); ok {
		size = sr.Size()
	} else if fr, ok = file.(*os.File); ok {
		if fi, err = fr.Stat(); err != nil {
			return
		}
		size = fi.Size()
	}
	return
}

// StartApi start api http listen.
func StartApi(addr string, s *Store, c *Config) {
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.Handle("/get", httpGetHandler{s: s})
		serveMux.Handle("/upload", httpUploadHandler{s: s, c: c})
		serveMux.Handle("/uploads", httpUploadsHandler{s: s, c: c})
		serveMux.Handle("/del", httpDelHandler{s: s})
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
	return
}

// httpGetHandler http upload a file.
type httpGetHandler struct {
	s *Store
}

func (h httpGetHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
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
	n = h.s.Needle()
	n.Key = key
	n.Cookie = int32(cookie)
	if v = h.s.Volumes[int32(vid)]; v != nil {
		if err = v.Get(n); err != nil {
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
	if err == nil {
		if r.Method == "GET" {
			if _, err = wr.Write(n.Data); err != nil {
				log.Errorf("wr.Write() error(%v)", err)
				ret = http.StatusInternalServerError
			}
		}
		if log.V(1) {
			log.Infof("get a needle: %v", n)
		}
	}
	h.s.FreeNeedle(n)
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	s *Store
	c *Config
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
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
	// check total content-length
	if size, err = strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err != nil {
		err = errors.ErrInternal
		return
	}
	if size > int64(h.c.NeedleMaxSize) {
		err = errors.ErrNeedleTooLarge
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
	defer file.Close()
	if size, err = fileSize(file); err != nil {
		log.Errorf("fileSize() error(%v)", err)
		err = errors.ErrInternal
		return
	}
	if size > int64(h.c.NeedleMaxSize) {
		err = errors.ErrNeedleTooLarge
		return
	}
	n = h.s.Needle()
	if err = n.WriteFrom(key, int32(cookie), int32(size), file); err == nil {
		if v = h.s.Volumes[int32(vid)]; v != nil {
			err = v.Add(n)
		} else {
			err = errors.ErrVolumeNotExist
		}
	}
	h.s.FreeNeedle(n)
	return
}

// httpUploads http upload files.
type httpUploadsHandler struct {
	s *Store
	c *Config
}

func (h httpUploadsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
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
		ns      *needle.Needles
		file    multipart.File
		fh      *multipart.FileHeader
		fhs     []*multipart.FileHeader
		res     = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
	// check total content-length
	if size, err = strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err != nil {
		err = errors.ErrInternal
		return
	}
	if size > int64(h.c.NeedleMaxSize*h.c.BatchMaxNum) {
		err = errors.ErrInternal
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
	ns = h.s.Needles(nn)
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
		defer file.Close()
		// check size
		if size, err = fileSize(file); err != nil {
			break
		}
		if size > int64(h.c.NeedleMaxSize) {
			err = errors.ErrNeedleTooLarge
			break
		}
		if err = ns.WriteFrom(key, int32(cookie), int32(size), file); err != nil {
			break
		}
	}
	if err == nil {
		if v = h.s.Volumes[int32(vid)]; v != nil {
			err = v.Write(ns)
		} else {
			err = errors.ErrVolumeNotExist
		}
	}
	h.s.FreeNeedles(nn, ns)
	return
}

type httpDelHandler struct {
	s *Store
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		v        *volume.Volume
		err      error
		key, vid int64
		str      string
		res      = map[string]interface{}{}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), &err, res)
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
	if v = h.s.Volumes[int32(vid)]; v != nil {
		err = v.Del(key)
	} else {
		err = errors.ErrVolumeNotExist
	}
	return
}
