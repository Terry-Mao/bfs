package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

// KeyCookie one file : key and cookie
type KeyCookie struct {
	Key 	int64 `json:"key"`
	Cookie 	int32 `json:"cookie"`
}

// GetResponse response for http get req
type GetResponse struct {
	Ret    int32 	`json:"ret"`
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

// UploadResponse response for http upload req
type UploadResponse struct {
	Ret    int32 	`json:"ret"`
	Keys   []KeyCookie `json:"keys"`
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

// DelResponse response for http del req
type DelResponse struct {
	Ret    int32 	`json:"ret"`
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}

// HttpGetWriter
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

// HttpUploadWriter
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

// HttpDelWriter
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
