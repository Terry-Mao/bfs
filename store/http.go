package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

func HttpPostWriter(r *http.Request, wr http.ResponseWriter, start time.Time, result map[string]interface{}) {
	var (
		err      error
		byteJson []byte
		ret      = result["ret"].(int)
	)
	if byteJson, err = json.Marshal(result); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", result, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, err *error, ret *int) {
	if *ret != http.StatusOK {
		http.Error(wr, (*err).Error(), *ret)
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.URL.String(), time.Now().Sub(start).Seconds(), *ret)
}
