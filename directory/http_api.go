package main

import (
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
		err         error
		key, cookie int64
		ret         int
		res         *GetResponse
		params      = r.URL.Query()
	)
	if r.Method != "GET" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpWriter(r, wr, time.Now(), &res, &ret)
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("key"), err)
		ret = http.StatusBadRequest
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("cookie"), err)
		ret = http.StatusBadRequest
		return
	}
	if res, ret, err = h.d.GetStores(key, int32(cookie)); err != nil {
		log.Errorf("GetStores() error(%v)", err)
		ret = http.StatusInternalServerError
	}
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	d *Directory
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
		num int64
		ret int
		res *UploadResponse
	)
	if r.Method != "POST" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpWriter(r, wr, time.Now(), &res, &ret)
	if num, err = strconv.ParseInt(r.FormValue("num"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("num"), err)
		ret = http.StatusBadRequest
		return
	}
	if res, ret, err = h.d.UploadStores(int(num)); err != nil {
		log.Errorf("UploadStores() error(%v)", err)
		ret = http.StatusInternalServerError
	}
	return
}

// httpDelHandler
type httpDelHandler struct {
	d *Directory
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err         error
		cookie, key int64
		ret         int
		res         *DelResponse
	)
	if r.Method != "POST" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpWriter(r, wr, time.Now(), &res, &ret)
	if key, err = strconv.ParseInt(r.FormValue("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("key"), err)
		ret = http.StatusBadRequest
		return
	}
	if cookie, err = strconv.ParseInt(r.FormValue("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("cookie"), err)
		ret = http.StatusBadRequest
		return
	}
	if res, ret, err = h.d.DelStores(key, int32(cookie)); err != nil {
		log.Errorf("DelStores() error(%v)", err)
		ret = http.StatusInternalServerError
	}
	return
}
