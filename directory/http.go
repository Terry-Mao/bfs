package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

// UploadResponse response for http upload req
type UploadResponse struct {
	Keys   []int64  `json:"keys"`
	Vid    int32    `json:"vid"`
	Cookie int32    `json:"cookie"`
	Stores []string `json:"stores"`
}

// GetResponse response for http get req
type GetResponse struct {
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

// DelResponse response for http del req
type DelResponse struct {
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

// HttpWriter
func HttpUploadWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res **UploadResponse, ret *int) {
	var (
		err      error
		byteJson []byte
	)
	if *ret != http.StatusOK {
		log.Errorf("HttpWriter ret error: %d", *ret)
		http.Error(wr, http.StatusText(*ret), *ret)
		return
	}
	if byteJson, err = json.Marshal(*res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", *res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), *ret)
}

// HttpWriter
func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res **GetResponse, ret *int) {
	var (
		err      error
		byteJson []byte
	)
	if *ret != http.StatusOK {
		log.Errorf("HttpWriter ret error: %d", *ret)
		http.Error(wr, http.StatusText(*ret), *ret)
		return
	}
	if byteJson, err = json.Marshal(*res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", *res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), *ret)
}

// HttpWriter
func HttpDelWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res **DelResponse, ret *int) {
	var (
		err      error
		byteJson []byte
	)
	if *ret != http.StatusOK {
		log.Errorf("HttpWriter ret error: %d", *ret)
		http.Error(wr, http.StatusText(*ret), *ret)
		return
	}
	if byteJson, err = json.Marshal(*res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", *res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), *ret)
}
