package main

import (
	"bfs/libs/errors"
	"bfs/libs/stat"
	"bfs/store/conf"
	"encoding/json"
	log "github.com/golang/glog"
	"golang.org/x/time/rate"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Server struct {
	store *Store
	conf  *conf.Config
	info  *stat.Info
	// server
	statSvr  net.Listener
	adminSvr net.Listener
	apiSvr   net.Listener
	// limit
	rl *rate.Limiter
	wl *rate.Limiter
	dl *rate.Limiter
}

func NewServer(s *Store, c *conf.Config) (svr *Server, err error) {
	svr = &Server{
		store: s,
		conf:  c,
		rl:    rate.NewLimiter(rate.Limit(c.Limit.Read.Rate), c.Limit.Read.Brust),
		wl:    rate.NewLimiter(rate.Limit(c.Limit.Write.Rate), c.Limit.Write.Brust),
		dl:    rate.NewLimiter(rate.Limit(c.Limit.Delete.Rate), c.Limit.Delete.Brust),
	}
	if svr.statSvr, err = net.Listen("tcp", c.StatListen); err != nil {
		log.Errorf("net.Listen(%s) error(%v)", c.StatListen, err)
		return
	}
	if svr.apiSvr, err = net.Listen("tcp", c.ApiListen); err != nil {
		log.Errorf("net.Listen(%s) error(%v)", c.ApiListen, err)
		return
	}
	if svr.adminSvr, err = net.Listen("tcp", c.AdminListen); err != nil {
		log.Errorf("net.Listen(%s) error(%v)", c.AdminListen, err)
		return
	}
	go svr.startStat()
	go svr.startApi()
	go svr.startAdmin()
	if c.Pprof {
		go StartPprof(c.PprofListen)
	}
	return
}

func (s *Server) Close() {
	if s.statSvr != nil {
		s.statSvr.Close()
	}
	if s.adminSvr != nil {
		s.adminSvr.Close()
	}
	if s.apiSvr != nil {
		s.apiSvr.Close()
	}
	return
}

type sizer interface {
	Size() int64
}

// checkFileSize get multipart.File size
func checkFileSize(file multipart.File, maxSize int) (size int64, err error) {
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
			log.Errorf("file.Stat() error(%v)", err)
			return
		}
		size = fi.Size()
	}
	if size > int64(maxSize) {
		err = errors.ErrNeedleTooLarge
	}
	return
}

func checkContentLength(r *http.Request, maxSize int) (err error) {
	var size int64
	// check total content-length
	if size, err = strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err != nil {
		err = errors.ErrInternal
		return
	}
	if size > int64(maxSize) {
		err = errors.ErrNeedleTooLarge
	}
	return
}

func HttpPostWriter(r *http.Request, wr http.ResponseWriter, start time.Time, err *error, result map[string]interface{}) {
	var (
		ok       bool
		byteJson []byte
		err1     error
		errStr   string
		uerr     errors.Error
		ret      = errors.RetOK
	)
	if *err != nil {
		errStr = (*err).Error()
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
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v[%v])", r.Method,
		r.URL.Path, r.Form.Encode(), time.Now().Sub(start).Seconds(), ret, errStr)
}

func HttpGetWriter(r *http.Request, wr http.ResponseWriter, start time.Time, err *error, ret *int) {
	var errStr string
	if *ret != http.StatusOK {
		if *err != nil {
			errStr = (*err).Error()
		}
		http.Error(wr, errStr, *ret)
	}
	log.Infof("%s path:%s(params:%s,time:%f,err:%s,ret:%v[%v])", r.Method,
		r.URL.Path, r.URL.String(), time.Now().Sub(start).Seconds(), errStr, *ret, errStr)
}
