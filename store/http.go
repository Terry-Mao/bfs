package main

import (
	"encoding/json"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	httpMaxUploadFiles = 9
	httpMaxDelFiles    = 9
	httpParamSpliter   = ","
	// error code
	httpOK            = 1
	httpNoVolume      = 1000
	httpUploadErr     = 1001
	httpUploadMaxFile = 1002
	httpDelErr        = 1003
	httpDelMaxFile    = 1004
	httpParamErr      = 65534
	httpInternalErr   = 65535
)

func StartHTTP(s *Store, addr string) {
	go func() {
		var (
			err      error
			serveMux = http.NewServeMux()
		)
		serveMux.Handle("/get", httpGetHandler{s: s})
		serveMux.Handle("/upload", httpUploadHandler{s: s})
		serveMux.Handle("/uploads", httpUploadsHandler{s: s})
		serveMux.Handle("/del", httpDelHandler{s: s})
		serveMux.Handle("/dels", httpDelsHandler{s: s})
		if err = http.ListenAndServe(addr, serveMux); err != nil {
			log.Errorf("http.ListenAndServe(\"%s\") error(%v)", addr, err)
			return
		}
	}()
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
		vid, key, cookie int64
		params           = r.URL.Query()
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		http.Error(wr, "volume not found", http.StatusNotFound)
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

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	s *Store
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		v                *Volume
		n                int
		vid, key, cookie int64
		err              error
		buf              []byte
		file             multipart.File
		res              = map[string]interface{}{"ret": httpOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer httpPostWriter(r, wr, time.Now(), res)
	if err = r.ParseMultipartForm(NeedleMaxSize); err != nil {
		res["ret"] = httpInternalErr
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		res["ret"] = httpParamErr
		return
	}
	if key, err = strconv.ParseInt(r.FormValue("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("key"), err)
		res["ret"] = httpParamErr
		return
	}
	if cookie, err = strconv.ParseInt(r.FormValue("cookie"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("cookie"), err)
		res["ret"] = httpParamErr
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		res["ret"] = httpNoVolume
		return
	}
	if file, _, err = r.FormFile("file"); err != nil {
		res["ret"] = httpInternalErr
		return
	}
	buf = v.Buffer()
	if n, err = file.Read(buf); err == nil {
		err = v.Add(key, cookie, buf[:n])
	}
	file.Close()
	v.FreeBuffer(buf)
	if err != nil {
		res["ret"] = httpUploadErr
	}
	return
}

// httpUploads http upload files.
type httpUploadsHandler struct {
	s *Store
}

func (h httpUploadsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		i, wn               int
		v                   *Volume
		n                   *Needle
		buf                 []byte
		str                 string
		err                 error
		vid, key, cookie    int64
		keyStrs, cookieStrs []string
		keys, cookies       []int64
		fh                  *multipart.FileHeader
		fhs                 []*multipart.FileHeader
		file                multipart.File
		res                 = map[string]interface{}{"ret": httpOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer httpPostWriter(r, wr, time.Now(), res)
	if err = r.ParseMultipartForm(NeedleMaxSize); err != nil {
		res["ret"] = httpInternalErr
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		res["ret"] = httpParamErr
		return
	}
	keyStrs = strings.Split(r.FormValue("keys"), httpParamSpliter)
	cookieStrs = strings.Split(r.FormValue("cookies"), httpParamSpliter)
	if len(keyStrs) != len(cookieStrs) {
		log.Errorf("param length not match, keys: %d, cookies: %d",
			len(keyStrs), len(cookieStrs))
		res["ret"] = httpParamErr
		return
	}
	for i, str = range keyStrs {
		if key, err = strconv.ParseInt(str, 10, 64); err != nil {
			res["ret"] = httpParamErr
			return
		}
		if cookie, err = strconv.ParseInt(str, 10, 64); err != nil {
			res["ret"] = httpParamErr
			return
		}
		keys = append(keys, key)
		cookies = append(cookies, cookie)
	}
	if r.MultipartForm != nil {
		if fhs = r.MultipartForm.File["file"]; len(fhs) > httpMaxUploadFiles {
			res["ret"] = httpUploadMaxFile
			return
		}
	}
	if len(keys) != len(fhs) {
		log.Errorf("param length not match, keys: %d, cookies: %d, files: %d",
			len(keys), len(cookies), len(fhs))
		res["ret"] = httpParamErr
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		res["ret"] = httpNoVolume
		return
	}
	// TODO?
	// use a large buffer stored all file buffer
	// this can let the lock without file read and needle parse.
	buf = v.Buffer()
	n = v.Needle()
	v.Lock()
	for i, fh = range fhs {
		if file, err = fh.Open(); err == nil {
			if wn, err = file.Read(buf); err == nil {
				if err = n.Parse(keys[i], cookies[i], buf[:wn]); err == nil {
					err = v.Write(n)
				}
			}
			file.Close()
		}
		if err != nil {
			goto free
		}
	}
	v.Flush()
free:
	v.Unlock()
	v.FreeNeedle(n)
	v.FreeBuffer(buf)
	if err != nil {
		res["ret"] = httpUploadErr
	}
	return
}

type httpDelHandler struct {
	s *Store
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		v        *Volume
		key, vid int64
		res      = map[string]interface{}{"ret": httpOK}
		params   = r.URL.Query()
	)
	if r.Method != "DELETE" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer httpPostWriter(r, wr, time.Now(), res)
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("key"), err)
		res["ret"] = httpParamErr
		return
	}
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		res["ret"] = httpParamErr
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		res["ret"] = httpNoVolume
		return
	}
	if err = v.Del(key); err != nil {
		res["ret"] = httpDelErr
	}
	return
}

type httpDelsHandler struct {
	s *Store
}

func (h httpDelsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		err      error
		v        *Volume
		str      string
		key, vid int64
		keyStrs  []string
		res      = map[string]interface{}{"ret": httpOK}
		params   = r.URL.Query()
	)
	if r.Method != "DELETE" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer httpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		res["ret"] = httpParamErr
		return
	}
	if v = h.s.Volume(int32(vid)); v == nil {
		res["ret"] = httpNoVolume
		return
	}
	if keyStrs = strings.Split(params.Get("keys"), httpParamSpliter); len(keyStrs) > httpMaxDelFiles {
		res["ret"] = httpDelMaxFile
		return
	}
	for _, str = range keyStrs {
		if key, err = strconv.ParseInt(str, 10, 64); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
			res["ret"] = httpParamErr
			return
		}
		if err = v.Del(key); err != nil {
			res["ret"] = httpDelErr
		}
	}
	return
}

func httpPostWriter(r *http.Request, wr http.ResponseWriter, start time.Time, result map[string]interface{}) {
	var (
		ret      = result["ret"].(int)
		params   = r.Form
		byteJson []byte
		err      error
	)
	if byteJson, err = json.Marshal(result); err != nil {
		log.Errorf("json.Marshal(\"%v\") failed (%v)", result, err)
		return
	}
	wr.Header().Set("Content-Type", "application/json;charset=utf-8")
	if _, err = wr.Write(byteJson); err != nil {
		log.Errorf("wr.Write(%s, %s) failed (%v)", r.URL.Path, params.Encode(), err)
		return
	}
	log.Infof("%s path:%s(params:%s,time:%f,ret:%v)", r.Method,
		r.URL.Path, params.Encode(), time.Now().Sub(start).Seconds(), ret)
}
