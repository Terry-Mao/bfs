package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

// Response response for http req
type Response struct {
	Keys   []int64  `json:"keys,omitempty"`
	Vid    int32    `json:"vid,omitempty"`
	Cookie int32    `json:"cookie,omitempty"`
	Stores []string `json:"stores,omitempty"`
}

// HttpWriter
func HttpWriter(r *http.Request, wr http.ResponseWriter, start time.Time, res *Response, ret *int) {
	var (
		err      error
		byteJson []byte
	)
	if *ret != http.StatusOK {
		http.Error(wr, http.StatusText(*ret), *ret)
		return
	}
	if byteJson, err = json.Marshal(res); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", res, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), *ret)
}
