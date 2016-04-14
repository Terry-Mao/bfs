package main

import (
	"bfs/libs/meta"
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

// HttpGetWriter
func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
	var (
		err      error
		byteJson []byte
		ret      = res.Ret
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

// HttpUploadWriter
func HttpUploadWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
	var (
		err      error
		byteJson []byte
		ret      = res.Ret
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

// HttpDelWriter
func HttpDelWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *meta.Response) {
	var (
		err      error
		byteJson []byte
		ret      = res.Ret
	)
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("HttpWriter Write error(%v)", err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}
