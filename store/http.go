package main

import (
	"encoding/json"
	"github.com/Terry-Mao/bfs/libs/errors"
	log "github.com/golang/glog"
	"net/http"
	"time"
)

func HttpPostWriter(r *http.Request, wr http.ResponseWriter, start time.Time, err *error, result map[string]interface{}) {
	var (
		ok       bool
		byteJson []byte
		err1     error
		uerr     errors.Error
		ret      = errors.RetOK
	)
	if *err != nil {
		if uerr, ok = (*err).(errors.Error); ok {
			ret = int(uerr)
		} else {
			ret = errors.RetInternalErr
		}
	}
	result["ret"] = ret
	if byteJson, err1 = json.Marshal(result); err1 != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", result, err1)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err1 = wr.Write(byteJson); err1 != nil {
		log.Errorf("http Write() error(%v)", err1)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret)
}

func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, err *error, ret *int) {
	var errStr string
	if *ret != http.StatusOK {
		if *err != nil {
			errStr = (*err).Error()
		}
		http.Error(wr, errStr, *ret)
	}
	log.Infof("%s path:%s(params:%s,time:%f,err:%s,ret:%v)", r.Method,
		r.URL.Path, r.URL.String(), time.Now().Sub(start).Seconds(), errStr, *ret)
}
