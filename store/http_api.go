package main

import (
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"time"
)

type sizer interface {
	Size() int64
}

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
	s *Store
}

func (h httpGetHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		v                *Volume
		err              error
		buf, data        []byte
		vid, key, cookie int64
		params           = r.URL.Query()
	)
	if r.Method != "GET" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("key"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("cookie"), err)
		http.Error(wr, "bad request", http.StatusBadRequest)
		return
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		http.Error(wr, "not found", http.StatusNotFound)
		return
	}
	buf = v.Buffer(1)
	if data, err = v.Get(key, int32(cookie), buf); err == nil {
		wr.Write(data)
	} else {
		http.Error(wr, err.Error(), http.StatusInternalServerError)
	}
	v.FreeBuffer(1, buf)
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	s *Store
	c *Config
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		rn       int
		vid      int64
		key      int64
		cookie   int64
		size     int64
		err      error
		buf      []byte
		v        *Volume
		n        *needle.Needle
		file     multipart.File
		sr       sizer
		fr       *os.File
		fi       os.FileInfo
		storeErr errors.StoreError
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	// check total content-length
	if size, err = strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	if size > int64(h.c.NeedleMaxSize) {
		res["ret"] = errors.RetNeedleTooLarge
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		res["ret"] = errors.RetParamErr
		return
	}
	if key, err = strconv.ParseInt(r.FormValue("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("key"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	if cookie, err = strconv.ParseInt(r.FormValue("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("cookie"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		res["ret"] = errors.RetNoVolume
		return
	}
	if file, _, err = r.FormFile("file"); err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	if sr, ok = file.(sizer); ok {
		size = sr.Size()
	} else if fr, ok = file.(*os.File); ok {
		if fi, err = fr.Stat(); err != nil {
			res["ret"] = errors.RetInternalErr
			return
		}
		size = fi.Size()
	}
	if size > int64(h.c.NeedleMaxSize) {
		res["ret"] = errors.RetNeedleTooLarge
		return
	}
	n = v.Needle()
	buf = v.Buffer(1)
	rn, err = file.Read(buf)
	file.Close()
	if err == nil {
		n.Parse(key, int32(cookie), buf[:rn])
		err = v.Add(n)
	}
	v.FreeBuffer(1, buf)
	v.FreeNeedle(n)
	if err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	return
}

// httpUploads http upload files.
type httpUploadsHandler struct {
	s *Store
	c *Config
}

func (h httpUploadsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		i, rn, tn, nn int
		ok            bool
		buf           []byte
		str           string
		err           error
		vid           int64
		key           int64
		cookie        int64
		size          int64
		offsets       []int
		keys          []int64
		cookies       []int32
		keyStrs       []string
		cookieStrs    []string
		sr            sizer
		fr            *os.File
		fi            os.FileInfo
		v             *Volume
		n             *needle.Needle
		storeErr      errors.StoreError
		file          multipart.File
		fh            *multipart.FileHeader
		fhs           []*multipart.FileHeader
		res           = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	// check total content-length
	if size, err = strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64); err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	if size > int64(h.c.NeedleMaxSize*h.c.BatchMaxNum) {
		res["ret"] = errors.RetNeedleTooLarge
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"), err)
		res["ret"] = errors.RetParamErr
		return
	}
	keyStrs = r.MultipartForm.Value["keys"]
	cookieStrs = r.MultipartForm.Value["cookies"]
	if len(keyStrs) != len(cookieStrs) {
		log.Errorf("param length not match, keys: %d, cookies: %d", len(keyStrs), len(cookieStrs))
		res["ret"] = errors.RetParamErr
		return
	}
	for i, str = range keyStrs {
		if key, err = strconv.ParseInt(str, 10, 64); err != nil {
			res["ret"] = errors.RetParamErr
			return
		}
		if cookie, err = strconv.ParseInt(str, 10, 32); err != nil {
			res["ret"] = errors.RetParamErr
			return
		}
		keys = append(keys, key)
		cookies = append(cookies, int32(cookie))
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		res["ret"] = errors.RetNoVolume
		return
	}
	fhs = r.MultipartForm.File["file"]
	nn = len(fhs)
	if len(keys) != nn {
		log.Errorf("param length not match, keys: %d, cookies: %d, files: %d", len(keys), len(cookies), len(fhs))
		res["ret"] = errors.RetParamErr
		return
	}
	offsets = make([]int, nn*2)
	buf = v.Buffer(nn)
	n = v.Needle()
	for i, fh = range fhs {
		file, err = fh.Open()
		file.Close()
		if err != nil {
			log.Errorf("fh.Open() error(%v)", err)
			break
		}
		// check size
		if sr, ok = file.(sizer); ok {
			size = sr.Size()
		} else if fr, ok = file.(*os.File); ok {
			if fi, err = fr.Stat(); err != nil {
				res["ret"] = errors.RetInternalErr
				return
			}
			size = fi.Size()
		}
		if size > int64(h.c.NeedleMaxSize) {
			res["ret"] = errors.RetNeedleTooLarge
			return
		}
		if rn, err = file.Read(buf); err != nil {
			log.Errorf("file.Read() error(%v)", err)
			break
		}
		offsets[i] = tn
		tn += rn
		offsets[i+1] = tn
	}
	if err == nil {
		v.Lock()
		for i = 0; i < nn; i++ {
			n.Parse(keys[i], cookies[i], buf[offsets[i]:offsets[i+1]])
			if err = v.Write(n); err != nil {
				break
			}
		}
		if err == nil {
			err = v.Flush()
		}
		v.Unlock()
	}
	if err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	v.FreeNeedle(n)
	v.FreeBuffer(nn, buf)
	return
}

type httpDelHandler struct {
	s *Store
}

func (h httpDelHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok       bool
		err      error
		storeErr errors.StoreError
		v        *Volume
		key, vid int64
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if key, err = strconv.ParseInt(r.PostFormValue("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.PostFormValue("key"), err)
		res["ret"] = errors.RetParamErr
		return
	}
	if vid, err = strconv.ParseInt(r.PostFormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.PostFormValue("vid"), err)
		res["ret"] = errors.RetParamErr
		return
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		res["ret"] = errors.RetNoVolume
		return
	}
	if err = v.Del(key); err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	return
}

type httpDelsHandler struct {
	s *Store
	c *Config
}

func (h httpDelsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		v        *Volume
		err      error
		ok       bool
		storeErr errors.StoreError
		str      string
		key, vid int64
		keyStrs  []string
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	if vid, err = strconv.ParseInt(r.PostFormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.PostFormValue("vid"), err)
		res["ret"] = errors.RetParamErr
		return
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		res["ret"] = errors.RetNoVolume
		return
	}
	if keyStrs = r.PostForm["keys"]; len(keyStrs) > h.c.BatchMaxNum {
		res["ret"] = errors.RetDelMaxFile
		return
	}
	for _, str = range keyStrs {
		if key, err = strconv.ParseInt(str, 10, 64); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
			res["ret"] = errors.RetParamErr
			return
		}
		if err = v.Del(key); err != nil {
			if storeErr, ok = err.(errors.StoreError); ok {
				res["ret"] = int(storeErr)
			} else {
				res["ret"] = errors.RetInternalErr
			}
		}
	}
	return
}
