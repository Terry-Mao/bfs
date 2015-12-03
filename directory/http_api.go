package main

import (
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"time"
)

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
		serveMux.Handle("/dels", httpDelsHandler{s: s, c: c})
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
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	d *Directory
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
}

// httpUploads http upload files.
type httpUploadsHandler struct {
	d *Directory
}

func (h httpUploadsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
}

type httpDelHandler struct {
	d *Directory
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {

}

type httpDelsHandler struct {
	d *Directory
}

func (h httpDelsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	
}
