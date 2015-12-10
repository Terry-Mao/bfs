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
		now              = time.Now()
		v                *Volume
		n                *needle.Needle
		ns               []needle.Needle
		err              error
		buf              []byte
		vid, key, cookie int64
		ret              = http.StatusOK
		params           = r.URL.Query()
	)
	if r.Method != "GET" && r.Method != "HEAD" {
		ret = http.StatusMethodNotAllowed
		http.Error(wr, "method not allowed", ret)
		return
	}
	defer HttpGetWriter(r, wr, now, &err, &ret)
	if vid, err = strconv.ParseInt(params.Get("vid"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("vid"), err)
		ret = http.StatusBadRequest
		return
	}
	if key, err = strconv.ParseInt(params.Get("key"), 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("key"), err)
		ret = http.StatusBadRequest
		return
	}
	if cookie, err = strconv.ParseInt(params.Get("cookie"), 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", params.Get("cookie"), err)
		ret = http.StatusBadRequest
		return
	}
	buf = h.s.Buffer(1)
	ns = h.s.Needle(1)
	n = &(ns[0])
	h.s.RLockVolume()
	if v = h.s.Volumes[int32(vid)]; v != nil {
		if err = v.Get(key, int32(cookie), buf, n); err != nil {
			ret = http.StatusInternalServerError
		}
	} else {
		ret = http.StatusNotFound
		err = errors.ErrVolumeNotExist
	}
	h.s.RUnlockVolume()
	if err == nil {
		if r.Method == "GET" {
			if _, err = wr.Write(n.Data); err != nil {
				log.Errorf("wr.Write() error(%v)", err)
				ret = http.StatusInternalServerError
			}
		}
		if log.V(1) {
			log.Infof("get a needle: %v", n)
		}
	}
	h.s.FreeBuffer(1, buf)
	h.s.FreeNeedle(1, ns)
	return
}

// httpUploadHandler http upload a file.
type httpUploadHandler struct {
	s *Store
	c *Config
}

func (h httpUploadHandler) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	var (
		ok     bool
		rn     int
		vid    int64
		key    int64
		cookie int64
		size   int64
		err    error
		str    string
		buf    []byte
		v      *Volume
		n      *needle.Needle
		ns     []needle.Needle
		file   multipart.File
		sr     sizer
		fr     *os.File
		fi     os.FileInfo
		uerr   errors.Error
		res    = map[string]interface{}{"ret": errors.RetOK}
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
	str = r.FormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	str = r.FormValue("key")
	if key, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	str = r.FormValue("cookie")
	if cookie, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
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
	ns = h.s.Needle(1)
	n = &(ns[0])
	buf = h.s.Buffer(1)
	// fill buf skip needle header offset
	// --------------------
	// header (data) footer
	// --------------------
	rn, err = file.Read(buf[needle.HeaderSize:])
	file.Close()
	if err != nil {
		res["ret"] = errors.RetInternalErr
		return
	}
	n.Init(key, int32(cookie), buf[needle.HeaderSize:rn])
	n.WriteHeader(buf)
	n.WriteFooter(buf[needle.HeaderSize+rn:], false)
	h.s.RLockVolume()
	if v = h.s.Volumes[int32(vid)]; v != nil {
		err = v.Add(n, buf)
	} else {
		err = errors.ErrVolumeNotExist
	}
	h.s.RUnlockVolume()
	h.s.FreeBuffer(1, buf[:n.TotalSize])
	h.s.FreeNeedle(1, ns)
	if err != nil {
		if uerr, ok = err.(errors.Error); ok {
			res["ret"] = int(uerr)
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
		i, rn, tn, nn, nb int
		ok                bool
		buf               []byte
		err               error
		vid               int64
		key               int64
		cookie            int64
		size              int64
		str               string
		keys              []string
		cookies           []string
		sr                sizer
		fr                *os.File
		fi                os.FileInfo
		v                 *Volume
		n                 *needle.Needle
		ns                []needle.Needle
		uerr              errors.Error
		file              multipart.File
		fh                *multipart.FileHeader
		fhs               []*multipart.FileHeader
		res               = map[string]interface{}{"ret": errors.RetOK}
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
	str = r.FormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	keys = r.MultipartForm.Value["keys"]
	cookies = r.MultipartForm.Value["cookies"]
	if len(keys) != len(cookies) {
		log.Errorf("param length not match, keys: %d, cookies: %d", len(keys), len(cookies))
		res["ret"] = errors.RetParamErr
		return
	}
	fhs = r.MultipartForm.File["file"]
	nn = len(fhs)
	if len(keys) != nn {
		log.Errorf("param length not match, keys: %d, cookies: %d, files: %d", len(keys), len(cookies), len(fhs))
		res["ret"] = errors.RetParamErr
		return
	}
	nb = int(size-1)/(h.c.NeedleMaxSize) + 1
	buf = h.s.Buffer(nb)
	ns = h.s.Needle(nn)
	tn = needle.HeaderSize
	for i, fh = range fhs {
		if key, err = strconv.ParseInt(keys[i], 10, 64); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", keys[i], err)
			err = errors.ErrParam
			break
		}
		if cookie, err = strconv.ParseInt(cookies[i], 10, 32); err != nil {
			log.Errorf("strconv.ParseInt(\"%s\") error(%v)", cookies[i], err)
			err = errors.ErrParam
			break
		}
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
				break
			}
			size = fi.Size()
		}
		if size > int64(h.c.NeedleMaxSize) {
			err = errors.ErrNeedleTooLarge
			break
		}
		if rn, err = file.Read(buf[tn:]); err != nil {
			log.Errorf("file.Read() error(%v)", err)
			break
		}
		n = &(ns[i])
		n.Init(key, int32(cookie), buf[tn:tn+rn])
		n.WriteHeader(buf[tn-needle.HeaderSize:])
		n.WriteFooter(buf[tn+rn:], false)
		tn += rn + needle.FooterSize
	}
	if err == nil {
		h.s.RLockVolume()
		if v = h.s.Volumes[int32(vid)]; v != nil {
			err = v.Write(ns, buf)
		} else {
			err = errors.ErrVolumeNotExist
		}
		h.s.RUnlockVolume()
	}
	h.s.FreeBuffer(nb, buf)
	h.s.FreeNeedle(nn, ns)
	if err != nil {
		if uerr, ok = err.(errors.Error); ok {
			res["ret"] = int(uerr)
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
		v        *Volume
		ok       bool
		err      error
		key, vid int64
		str      string
		uerr     errors.Error
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	str = r.PostFormValue("key")
	if key, err = strconv.ParseInt(str, 10, 64); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	str = r.PostFormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	h.s.RLockVolume()
	if v = h.s.Volumes[int32(vid)]; v != nil {
		err = v.Del(key)
	} else {
		err = errors.ErrVolumeNotExist
	}
	h.s.RUnlockVolume()
	if err != nil {
		if uerr, ok = err.(errors.Error); ok {
			res["ret"] = int(uerr)
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
		ok       bool
		err      error
		str      string
		key, vid int64
		keyStrs  []string
		uerr     errors.Error
		res      = map[string]interface{}{"ret": errors.RetOK}
	)
	if r.Method != "POST" {
		http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer HttpPostWriter(r, wr, time.Now(), res)
	str = r.PostFormValue("vid")
	if vid, err = strconv.ParseInt(str, 10, 32); err != nil {
		log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
		res["ret"] = errors.RetParamErr
		return
	}
	if keyStrs = r.PostForm["keys"]; len(keyStrs) > h.c.BatchMaxNum {
		res["ret"] = errors.RetDelMaxFile
		return
	}
	h.s.RLockVolume()
	if v = h.s.Volumes[int32(vid)]; v != nil {
		for _, str = range keyStrs {
			if key, err = strconv.ParseInt(str, 10, 64); err == nil {
				err = v.Del(key)
			} else {
				log.Errorf("strconv.ParseInt(\"%s\") error(%v)", str, err)
				err = errors.ErrParam
			}
		}
	} else {
		err = errors.ErrVolumeNotExist
	}
	h.s.RUnlockVolume()
	if err != nil {
		if uerr, ok = err.(errors.Error); ok {
			res["ret"] = int(uerr)
		} else {
			res["ret"] = errors.RetInternalErr
		}
	}
	return
}
