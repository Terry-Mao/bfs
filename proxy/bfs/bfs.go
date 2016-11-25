package bfs

import (
	"bfs/libs/errors"
	"bfs/libs/meta"
	"bfs/proxy/conf"
	"bytes"
	"encoding/json"
	"fmt"
	itime "github.com/Terry-Mao/marmot/time"
	log "github.com/golang/glog"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// api
	_directoryGetApi    = "http://%s/get"
	_directoryUploadApi = "http://%s/upload"
	_directoryDelApi    = "http://%s/del"
	_storeGetApi        = "http://%s/get"
	_storeUploadApi     = "http://%s/upload"
	_storeDelApi        = "http://%s/del"
)

var (
	_timer     = itime.NewTimer(1024)
	_transport = &http.Transport{
		Dial: func(netw, addr string) (c net.Conn, err error) {
			if c, err = net.DialTimeout(netw, addr, 2*time.Second); err != nil {
				return nil, err
			}
			return c, nil
		},
		DisableCompression: true,
	}
	_client = &http.Client{
		Transport: _transport,
	}
	_canceler = _transport.CancelRequest
	// random store node
	_rand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type Bfs struct {
	c *conf.Config
}

func New(c *conf.Config) (b *Bfs) {
	b = &Bfs{}
	b.c = c
	return
}

// Get
func (b *Bfs) Get(bucket, filename string) (src io.ReadCloser, ctlen int, mtime int64, sha1, mine string, err error) {
	var (
		i, ix, l int
		uri      string
		req      *http.Request
		resp     *http.Response
		res      meta.Response
		params   = url.Values{}
	)
	params.Set("bucket", bucket)
	params.Set("filename", filename)
	uri = fmt.Sprintf(_directoryGetApi, b.c.BfsAddr)
	if err = Http("GET", uri, params, nil, &res); err != nil {
		log.Errorf("GET called Http error(%v)", err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("http.Get directory res.Ret: %d %s", res.Ret, uri)
		if res.Ret == errors.RetNeedleNotExist {
			err = errors.ErrNeedleNotExist
		} else {
			err = errors.ErrInternal
		}
		return
	}
	mtime = res.MTime
	sha1 = res.Sha1
	mine = res.Mine
	params = url.Values{}
	l = len(res.Stores)
	ix = _rand.Intn(l)
	for i = 0; i < l; i++ {
		params.Set("key", strconv.FormatInt(res.Key, 10))
		params.Set("cookie", strconv.FormatInt(int64(res.Cookie), 10))
		params.Set("vid", strconv.FormatInt(int64(res.Vid), 10))
		uri = fmt.Sprintf(_storeGetApi, res.Stores[(ix+i)%l]) + "?" + params.Encode()
		if req, err = http.NewRequest("GET", uri, nil); err != nil {
			continue
		}
		td := _timer.Start(5*time.Second, func() {
			_canceler(req)
		})
		if resp, err = _client.Do(req); err != nil {
			log.Errorf("_client.do(%s) error(%v)", uri, err)
			continue
		}
		td.Stop()
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}
		src = resp.Body
		ctlen = int(resp.ContentLength)
		break
	}
	if err == nil {
		if resp.StatusCode == http.StatusServiceUnavailable {
			err = errors.ErrStoreNotAvailable
		} else if resp.StatusCode == http.StatusNotFound {
			err = errors.ErrNeedleNotExist
		}
	}
	return
}

// Upload
func (b *Bfs) Upload(bucket, filename, mine, sha1 string, buf []byte) (err error) {
	var (
		params = url.Values{}
		uri    string
		host   string
		res    meta.Response
		sRet   meta.StoreRet
	)
	params.Set("bucket", bucket)
	params.Set("filename", filename)
	params.Set("mine", mine)
	params.Set("sha1", sha1)
	uri = fmt.Sprintf(_directoryUploadApi, b.c.BfsAddr)
	if err = Http("POST", uri, params, nil, &res); err != nil {
		return
	}
	if res.Ret != errors.RetOK && res.Ret != errors.RetNeedleExist {
		log.Errorf("http.Post directory res.Ret: %d %s", res.Ret, uri)
		err = errors.ErrInternal
		return
	}
	// same sha1sum.
	if strings.HasPrefix(filename, sha1) && res.Ret == errors.RetNeedleExist {
		err = errors.ErrNeedleExist
		return
	}

	params = url.Values{}
	for _, host = range res.Stores {
		params.Set("key", strconv.FormatInt(res.Key, 10))
		params.Set("cookie", strconv.FormatInt(int64(res.Cookie), 10))
		params.Set("vid", strconv.FormatInt(int64(res.Vid), 10))
		uri = fmt.Sprintf(_storeUploadApi, host)
		if err = Http("POST", uri, params, buf, &sRet); err != nil {
			return
		}
		if sRet.Ret != 1 {
			log.Errorf("http.Post store sRet.Ret: %d  %s %d %d %d", sRet.Ret, uri, res.Key, res.Cookie, res.Vid)
			err = errors.ErrInternal
			return
		}
	}
	if res.Ret == errors.RetNeedleExist {
		err = errors.ErrNeedleExist
	}
	log.Infof("bfs.upload bucket:%s filename:%s key:%d cookie:%d vid:%d", bucket, filename, res.Key, res.Cookie, res.Vid)
	return
}

// Delete
func (b *Bfs) Delete(bucket, filename string) (err error) {
	var (
		params = url.Values{}
		host   string
		uri    string
		res    meta.Response
		sRet   meta.StoreRet
	)
	params.Set("bucket", bucket)
	params.Set("filename", filename)
	uri = fmt.Sprintf(_directoryDelApi, b.c.BfsAddr)
	if err = Http("POST", uri, params, nil, &res); err != nil {
		log.Errorf("Delete called Http error(%v)", err)
		return
	}
	if res.Ret != errors.RetOK {
		log.Errorf("http.Get directory res.Ret: %d %s", res.Ret, uri)
		if res.Ret == errors.RetNeedleNotExist {
			err = errors.ErrNeedleNotExist
		} else {
			err = errors.ErrInternal
		}
		return
	}

	params = url.Values{}
	for _, host = range res.Stores {
		params.Set("key", strconv.FormatInt(res.Key, 10))
		params.Set("vid", strconv.FormatInt(int64(res.Vid), 10))
		uri = fmt.Sprintf(_storeDelApi, host)
		if err = Http("POST", uri, params, nil, &sRet); err != nil {
			log.Errorf("Update called Http error(%v)", err)
			return
		}
		if sRet.Ret != 1 {
			log.Errorf("Delete store sRet.Ret: %d  %s", sRet.Ret, uri)
			err = errors.ErrInternal
			return
		}
	}
	return
}

// Ping
func (b *Bfs) Ping() error {
	return nil
}

// Http params
func Http(method, uri string, params url.Values, buf []byte, res interface{}) (err error) {
	var (
		body    []byte
		w       *multipart.Writer
		bw      io.Writer
		bufdata = &bytes.Buffer{}
		req     *http.Request
		resp    *http.Response
		ru      string
		enc     string
		ctype   string
	)
	enc = params.Encode()
	if enc != "" {
		ru = uri + "?" + enc
	}
	if method == "GET" {
		if req, err = http.NewRequest("GET", ru, nil); err != nil {
			return
		}
	} else {
		if buf == nil {
			if req, err = http.NewRequest("POST", uri, strings.NewReader(enc)); err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		} else {
			w = multipart.NewWriter(bufdata)
			if bw, err = w.CreateFormFile("file", "1.jpg"); err != nil {
				return
			}
			if _, err = bw.Write(buf); err != nil {
				return
			}
			for key, _ := range params {
				w.WriteField(key, params.Get(key))
			}
			ctype = w.FormDataContentType()
			if err = w.Close(); err != nil {
				return
			}
			if req, err = http.NewRequest("POST", uri, bufdata); err != nil {
				return
			}
			req.Header.Set("Content-Type", ctype)
		}
	}
	td := _timer.Start(5*time.Second, func() {
		_canceler(req)
	})
	if resp, err = _client.Do(req); err != nil {
		log.Errorf("_client.Do(%s) error(%v)", ru, err)
		return
	}
	td.Stop()
	defer resp.Body.Close()
	if res == nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Errorf("_client.Do(%s) status: %d", ru, resp.StatusCode)
		return
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("ioutil.ReadAll(%s) uri(%s) error(%v)", body, ru, err)
		return
	}
	if err = json.Unmarshal(body, res); err != nil {
		log.Errorf("json.Unmarshal(%s) uri(%s) error(%v)", body, ru, err)
	}
	return
}
