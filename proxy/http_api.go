package main

import (
	"bfs/libs/errors"
	"bfs/proxy/auth"
	"bfs/proxy/bfs"
	"bfs/proxy/conf"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	log "github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	_httpServerReadTimeout  = 5 * time.Second
	_httpServerWriteTimeout = 2 * time.Second
)

// Server  http_server
type Server struct {
	bfs    *bfs.Bfs
	auth   *auth.Auth
	config *conf.Config
}

// Init init the http module.
func Init(config *conf.Config) (s *Server, err error) {
	s = &Server{}
	s.config = config
	s.bfs = bfs.NewBfs(config)
	if s.auth, err = auth.NewAuth(config); err != nil {
		return
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", s.httpDo)
		mux.HandleFunc("/ping", s.ping)
		server := &http.Server{Addr: config.HttpAddr, Handler: mux,
			ReadTimeout: _httpServerReadTimeout, WriteTimeout: _httpServerWriteTimeout}
		if err := server.ListenAndServe(); err != nil {
			return
		}
	}()
	return
}

func (s *Server) httpDo(wr http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	if err = s.auth.CheckAuth(r); err != nil {
		if uerr, ok := (err).(errors.Error); ok {
			http.Error(wr, "auth failed", int(uerr))
		}
		return
	}
	switch r.Method {
	case "HEAD":
		s.download(wr, r)
	case "GET":
		s.download(wr, r)
	case "PUT":
		s.upload(wr, r)
	case "DELETE":
		s.delete(wr, r)
	}
	return
}

// download
func (s *Server) download(wr http.ResponseWriter, r *http.Request) {
	var (
		content  []byte
		bucket   string
		filename string
		ss       []string
		start    time.Time
		err      error
	)
	start = time.Now()
	log.Infof("download url: %s", r.URL.String())
	if !strings.HasPrefix(r.URL.Path, "/bfs") {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	ss = strings.Split(strings.TrimPrefix(r.URL.Path, "/bfs")[1:], "/")
	if bucket = ss[0]; bucket == "" {
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if len(ss) >= 2 {
		filename = r.URL.Path[len(bucket)+len("/bfs")+2:]
		if filename == "" {
			http.Error(wr, "bad request", http.StatusBadRequest)
			return
		}
	}
	if content, err = s.bfs.Get(bucket, filename); err != nil {
		log.Errorf("s.bfs.Get error(%v), bucket: %s, filename: %s, time long:%f", err, bucket, filename, time.Now().Sub(start).Seconds())
		if err == errors.ErrNeedleNotExist {
			http.Error(wr, "404", http.StatusNotFound)
			return
		}
		http.Error(wr, "Internal Server error", http.StatusInternalServerError)
		return
	}
	ss = strings.Split(filename, ".")
	wr.Header().Set("Content-Type", http.DetectContentType(content))
	wr.Header().Set("Content-Length", strconv.Itoa(len(content)))
	wr.Header().Set("Server", "Bfs")
	if r.Method == "GET" {
		wr.Write(content)
	}
	log.Infof("download url:%s, time long:%f", r.URL.String(), time.Now().Sub(start).Seconds())
	return
}

// upload upload file.
func (s *Server) upload(wr http.ResponseWriter, r *http.Request) {
	var (
		body     []byte
		ss       []string
		start    time.Time
		mine     string
		bucket   string
		filename string
		location string
		sha1sum  string
		sha      [sha1.Size]byte
		err      error
	)
	start = time.Now()
	log.Infof("upload url:%s", r.URL.String())
	mine = r.Header.Get("Content-Type")
	if mine == "" {
		wr.Header().Set("Code", strconv.Itoa(http.StatusBadRequest))
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		log.Errorf("ioutil.ReadAll(r.Body) error(%s)", err)
		wr.Header().Set("Code", strconv.Itoa(http.StatusBadRequest))
		return
	}
	r.Body.Close()
	// check content length
	if len(body) > s.config.MaxFileSize {
		wr.Header().Set("Code", strconv.Itoa(http.StatusRequestEntityTooLarge))
		return
	}
	sha = sha1.Sum(body)
	sha1sum = hex.EncodeToString(sha[:])
	ss = strings.Split(r.URL.Path[1:], "/")
	if bucket = ss[0]; bucket == "" {
		wr.Header().Set("Code", strconv.Itoa(http.StatusBadRequest))
		return
	}
	if len(ss) >= 2 {
		filename = r.URL.Path[len(bucket)+2:]
	}
	if filename == "" {
		ext := mine[strings.IndexByte(mine, '/')+1:]
		if ext == "jpeg" {
			ext = "jpg"
		}
		filename = sha1sum + "." + ext
	}
	if err = s.bfs.Upload(bucket, filename, mine, sha1sum, body); err != nil && err != errors.ErrNeedleExist {
		log.Errorf("s.bfs.Upload error(%v), bucket: %s, filename: %s, time long:%f", err, bucket, filename, time.Now().Sub(start).Seconds())
		wr.Header().Set("Code", strconv.Itoa(http.StatusInternalServerError))
		return
	}
	location = s.config.Domain + path.Join(bucket, filename)
	wr.Header().Set("Code", strconv.Itoa(http.StatusOK))
	wr.Header().Set("Location", location)
	wr.Header().Set("ETag", sha1sum)
	log.Infof("upload url:%s, location: %s , time long:%f", r.URL.String(), location, time.Now().Sub(start).Seconds())
	return
}

// delete
func (s *Server) delete(wr http.ResponseWriter, r *http.Request) {
	var (
		bucket   string
		filename string
		ss       []string
		start    time.Time
		err      error
	)
	start = time.Now()
	log.Infof("delete url:%s", r.URL.String())
	ss = strings.Split(r.URL.Path[1:], "/")
	if bucket = ss[0]; bucket == "" {
		wr.Header().Set("Code", strconv.Itoa(http.StatusRequestEntityTooLarge))
		return
	}
	if len(ss) >= 2 {
		filename = r.URL.Path[len(bucket)+2:]
		if filename == "" {
			wr.Header().Set("Code", strconv.Itoa(http.StatusBadRequest))
			return
		}
	}
	if err = s.bfs.Delete(bucket, filename); err != nil {
		log.Errorf("s.bfs.Delete error(%v), bucket: %s, filename: %s, time long:%f", err, bucket, filename, time.Now().Sub(start).Seconds())
		if err == errors.ErrNeedleNotExist {
			http.Error(wr, "404", http.StatusNotFound)
			return
		}
		wr.Header().Set("Code", strconv.Itoa(http.StatusInternalServerError))
		return
	}
	wr.Header().Set("Code", strconv.Itoa(http.StatusOK))
	log.Infof("delete url:%s, time long:%f", r.URL.String(), time.Now().Sub(start).Seconds())
	return
}

// monitorPing sure program now runs correctly, when return http status 200.
func (s *Server) ping(wr http.ResponseWriter, r *http.Request) {
	var (
		byteJson []byte
		f        *os.File
		res      = map[string]interface{}{"code": 0}
		err      error
	)
	if f, err = os.Open("/tmp/proxy.ping"); err == nil {
		// ping check
		res["code"] = http.StatusInternalServerError
		f.Close()
	}
	if err = s.bfs.Ping(); err != nil {
		http.Error(wr, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		res["code"] = http.StatusInternalServerError
	}
	if byteJson, err = json.Marshal(res); err != nil {
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		return
	}
	return
}
