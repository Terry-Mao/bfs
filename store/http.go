package main

import (
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"strconv"
)

const (
	httpMaxUploadFiles = 9
	httpParamSpliter   = ","
)

func StartHTTP(s *Store, addr string) {
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.Handle("/upload", httpUploadHandler{s: s})
		serveMux.Handle("/get", httpGetHandler{s: s})
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	s *Store
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		n                int
		v                *Volume
		buf              []byte
		err              error
		file             multipart.File
		param            string
		vid, key, cookie int64
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err = r.ParseMultipartForm(NeedleMaxSize); err != nil {
		http.Error(wr, "internal server error", http.StatusInternalServerError)
		return
	}
	param = r.FormValue("vid")
	if vid, err = strconv.ParseInt(param, 10, 32); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		log.Errorf("param vid: %s error", param)
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		http.Error(wr, "volume not found", http.StatusNotFound)
		return
	}
	param = r.FormValue("key")
	if key, err = strconv.ParseInt(param, 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		log.Errorf("param key: %s error", param)
		return
	}
	param = r.FormValue("cookie")
	if cookie, err = strconv.ParseInt(param, 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		log.Errorf("param cookie: %s error", param)
		return
	}
	if file, _, err = r.FormFile("file"); err != nil {
		http.Error(wr, "internal server error", http.StatusInternalServerError)
		return
	}
	buf = v.Buffer()
	if n, err = file.Read(buf); err == nil {
		err = v.Add(key, cookie, buf[:n])
	}
	file.Close()
	v.FreeBuffer(buf)
	if err != nil {
		http.Error(wr, err.Error(), http.StatusInternalServerError)
	} else {
		http.Error(wr, "ok", http.StatusOK)
	}
	return
}

// httpGetHandler http upload a file.
type httpGetHandler struct {
	s *Store
}

func (h httpGetHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		v                *Volume
		buf, data        []byte
		err              error
		param            string
		vid, key, cookie int64
		params           = r.URL.Query()
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	param = params.Get("vid")
	if vid, err = strconv.ParseInt(param, 10, 32); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		http.Error(wr, "volume not found", http.StatusNotFound)
		return
	}
	param = params.Get("key")
	if key, err = strconv.ParseInt(param, 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	param = params.Get("cookie")
	if cookie, err = strconv.ParseInt(param, 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	buf = v.Buffer()
	if data, err = v.Get(key, cookie, buf); err == nil {
		wr.Write(data)
	} else {
		http.Error(wr, err.Error(), http.StatusInternalServerError)
	}
	v.FreeBuffer(buf)
	return
}

// httpUploads http upload files.
func httpUploads(wr http.ResponseWriter, r *http.Request) {
	/*
		var (
			fh                  *multipart.FileHeader
			fhs                 []*multipart.FileHeader
			file                multipart.File
			err                 error
			params, key, cookie string
			keys, cookies       []string
			res                 = map[string]interface{}{}
		)
		if r.Method != "POST" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		params = r.Form.Encode()
		defer retPostWriter(r, wr, &params, time.Now(), res)
		keys = strings.Split(r.FormValue("keys"), httpParamSpliter)
		cookies = strings.Split(r.FormValue("cookies"), httpParamSpliter)
		if err = r.ParseMultipartForm(NeedleMaxSize); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if r.MultipartForm != nil {
			if fhs = r.MultipartForm.File["file"]; len(fhs) > httpMaxUploadFiles {
				res["ret"] = RetUploadMaxFile
				return
			}
		}
		// get a buffer
		// get a needle
		// lock
		for _, fh = range fhs {
			// check length
			if file, err = fh.Open(); err != nil {
				goto failed
			}
			if n, err = file.Read(buf); err != nil {
				file.Close()
				goto failed
			}
			// append
			if err = v.Write(n); err != nil {
				file.Close()
				goto failed
			}
			file.Close()
		}
		// flush
		// unlock
		// free needle
		// free buffer
		return
	*/
}
