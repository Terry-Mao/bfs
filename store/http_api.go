package main

import (
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	log "github.com/golang/glog"
	"mime/multipart"
	"net/http"
	"strconv"
	"time"
)

// StartApi start api http listen.
func StartApi(s *Store, addr string) {
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
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		http.Error(wr, "bad request, param error", http.StatusBadRequest)
		return
	}
	if v = h.s.Volumes[int32(vid)]; v == nil {
		http.Error(wr, "volume not found", http.StatusNotFound)
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
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok               bool
		storeErr         errors.StoreError
		v                *Volume
		n                *needle.Needle
		rn               int
		vid, key, cookie int64
		err              error
		buf              []byte
		file             multipart.File
		res              = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	// TODO
	if err = r.ParseMultipartForm(100); err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
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
	// TODO check max file size
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
}

func (h httpUploadsHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		i, rn, tn, nn       int
		v                   *Volume
		n                   *needle.Needle
		ok                  bool
		storeErr            errors.StoreError
		buf                 []byte
		str                 string
		err                 error
		vid, key, cookie    int64
		keyStrs, cookieStrs []string
		keys                []int64
		cookies             []int32
		offsets             []int
		fh                  *multipart.FileHeader
		fhs                 []*multipart.FileHeader
		file                multipart.File
		res                 = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	// TODO
	if err = r.ParseMultipartForm(100); err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	if vid, err = strconv.ParseInt(r.FormValue("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", r.FormValue("vid"),
			err)
		res["ret"] = errors.RetParamErr
		return
	}
	keyStrs = r.MultipartForm.Value["keys"]
	cookieStrs = r.MultipartForm.Value["cookies"]
	if len(keyStrs) != len(cookieStrs) {
		log.Errorf("param length not match, keys: %d, cookies: %d",
			len(keyStrs), len(cookieStrs))
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
	if r.MultipartForm != nil {
		fhs = r.MultipartForm.File["file"]
	}
	nn = len(fhs)
	if len(keys) != nn {
		log.Errorf("param length not match, keys: %d, cookies: %d, files: %d",
			len(keys), len(cookies), len(fhs))
		res["ret"] = errors.RetParamErr
		return
	}
	if err = v.ValidBatch(nn); err == nil {
		offsets = make([]int, nn*2)
		buf = v.Buffer(nn)
		n = v.Needle()
		for i, fh = range fhs {
			file, err = fh.Open()
			file.Close()
			if err == nil {
				if rn, err = file.Read(buf); err == nil {
					goto free
				}
				offsets[i] = tn
				offsets[i+1] = tn + rn
				tn += rn
			}
			if err != nil {
				goto free
			}
		}
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
	free:
		v.FreeNeedle(n)
		v.FreeBuffer(nn, buf)
	}
	if err != nil {
		if storeErr, ok = err.(errors.StoreError); ok {
			res["ret"] = int(storeErr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
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
	if keyStrs = r.PostForm["keys"]; len(keyStrs) > HttpMaxDelFiles {
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
