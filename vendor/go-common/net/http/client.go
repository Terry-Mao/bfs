package http

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	xhttp "net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/net/netutil"
	"go-common/net/trace"
	"go-common/stat"
	xtime "go-common/time"
)

const (
	_family  = "http_client"
	_minRead = 16 * 1024 // 16kb
)

var (
	// ErrBreaker error of breaker not allowed
	ErrBreaker       = errors.New("breaker not allowed")
	_noKickUserAgent = "haoguanwei@bilibili.com "
)

func init() {
	n, err := os.Hostname()
	if err == nil {
		_noKickUserAgent = _noKickUserAgent + runtime.Version() + " " + n
	}
}

type td struct {
	path    string
	timeout time.Duration
}

// Client is http client.
type Client struct {
	conf      *conf.HTTPClient
	client    *xhttp.Client
	dialer    *net.Dialer
	transport *xhttp.Transport

	breaker atomic.Value
	timeout atomic.Value
	bch     chan string
	tch     chan td
	Stats   stat.Stat
}

// NewClient new a http client.
func NewClient(c *conf.HTTPClient) *Client {
	client := new(Client)
	client.conf = c
	client.dialer = &net.Dialer{
		Timeout:   time.Duration(c.Dial),
		KeepAlive: time.Duration(c.KeepAlive),
	}
	client.transport = &xhttp.Transport{
		DialContext:     client.dialer.DialContext,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client.client = &xhttp.Client{
		Transport: client.transport,
	}
	if c.Breaker != nil {
		client.breaker.Store(make(map[string]*netutil.Breaker))
		client.bch = make(chan string, 10)
	}
	client.timeout.Store(make(map[string]time.Duration))
	client.tch = make(chan td, 10)
	go client.evproc()
	return client
}

func (client *Client) evproc() {
	for {
		select {
		case v := <-client.bch:
			bo := client.breaker.Load().(map[string]*netutil.Breaker)
			if _, ok := bo[v]; ok {
				continue
			}
			bn := make(map[string]*netutil.Breaker, len(bo)+1)
			bn[v] = netutil.NewBreaker(client.conf.Breaker)
			if client.Stats != nil {
				h := func(state int32) {
					client.Stats.State("breaker "+v, int64(state))
				}
				bn[v].State = h
			}
			for k, v := range bo {
				bn[k] = v
			}
			client.breaker.Store(bn)
		case v := <-client.tch:
			to := client.timeout.Load().(map[string]time.Duration)
			tn := make(map[string]time.Duration, len(to)+1)
			tn[v.path] = v.timeout
			for k, v := range to {
				tn[k] = v
			}
			client.timeout.Store(tn)
		}
	}
}

// SetKeepAlive set http client keepalive.
func (client *Client) SetKeepAlive(d time.Duration) {
	client.dialer.KeepAlive = d
	client.conf.KeepAlive = xtime.Duration(d)
}

// SetDialTimeout set http client dial timeout.
func (client *Client) SetDialTimeout(d time.Duration) {
	client.dialer.Timeout = d
	client.conf.Dial = xtime.Duration(d)
}

// SetTimeout set http client timeout.
func (client *Client) SetTimeout(d time.Duration) {
	client.conf.Timeout = xtime.Duration(d)
}

// SetPathTimeout set http client timeout.
func (client *Client) SetPathTimeout(path string, d time.Duration) {
	client.tch <- td{path: path, timeout: d}
}

// Get issues a GET to the specified URL.
func (client *Client) Get(c context.Context, uri, ip string, params url.Values, res interface{}) (err error) {
	req, err := newRequest(xhttp.MethodGet, uri, ip, params)
	if err != nil {
		return
	}
	return client.Do(c, req, res)
}

// Post issues a Post to the specified URL.
func (client *Client) Post(c context.Context, uri, ip string, params url.Values, res interface{}) (err error) {
	req, err := newRequest(xhttp.MethodPost, uri, ip, params)
	if err != nil {
		return
	}
	return client.Do(c, req, res)
}

func (client *Client) onBreaker(breaker *netutil.Breaker, err *error) {
	if err != nil && *err != nil {
		breaker.Fail()
	} else {
		breaker.Success()
	}
}

// Do sends an HTTP request and returns an HTTP response.
func (client *Client) Do(c context.Context, req *xhttp.Request, res interface{}) (err error) {
	var (
		ok       bool
		bs       []byte
		timeout  time.Duration
		cancel   func()
		t        *trace.Trace2
		resp     *xhttp.Response
		breaker  *netutil.Breaker
		breakers map[string]*netutil.Breaker
	)
	// trace
	if t, ok = trace.FromContext2(c); ok {
		t = t.Fork(_family, req.Host+req.URL.Path, req.RemoteAddr)
		t.WithHTTP(req)
		t.Client(req.Method)
		defer t.Done(&err)
	}
	// breaker
	if breakers, ok = client.breaker.Load().(map[string]*netutil.Breaker); ok {
		if breaker, ok = breakers[req.URL.Path]; ok {
			if !breaker.Allow() {
				err = ErrBreaker
				return
			}
			defer client.onBreaker(breaker, &err)
		} else {
			select {
			case client.bch <- req.URL.Path:
			default:
			}
		}
	}
	if client.Stats != nil {
		now := time.Now()
		defer func() {
			client.Stats.Timing(req.URL.Host+req.URL.Path, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	// TODO timeout use full path
	if timeout, ok = client.timeout.Load().(map[string]time.Duration)[req.URL.Path]; !ok {
		timeout = time.Duration(client.conf.Timeout)
	}
	c, cancel = context.WithTimeout(c, timeout)
	defer cancel()
	req = req.WithContext(c)
	// header
	req.Header.Set("User-Agent", _noKickUserAgent)
	if resp, err = client.client.Do(req); err != nil {
		log.Error("httpClient.Do(%s) error(%v)", realURL(req), err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= xhttp.StatusInternalServerError {
		err = ErrStatusCode
		log.Error("readAll(%s) uri(%s) error(%v)", bs, realURL(req), err)
		return
	}
	if bs, err = readAll(resp.Body, _minRead); err != nil {
		log.Error("readAll(%s) uri(%s) error(%v)", bs, realURL(req), err)
		return
	}
	if res != nil {
		if err = json.Unmarshal(bs, res); err != nil {
			log.Error("json.Unmarshal(%s) uri(%s) error(%v)", bs, realURL(req), err)
		}
	}
	return
}

// readAll reads from r until an error or EOF and returns the data it read
// from the internal buffer allocated with a specified capacity.
func readAll(r io.Reader, capacity int64) (b []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, capacity))
	// If the buffer overflows, we will get bytes.ErrTooLarge.
	// Return that as an error. Any other panic remains.
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()
	_, err = buf.ReadFrom(r)
	return buf.Bytes(), err
}

// Sign calc appkey and appsecret sign.
func Sign(params url.Values) (query string, err error) {
	if len(params) == 0 {
		return
	}
	if params.Get("appkey") == "" {
		err = fmt.Errorf("utils http get must have parameter appkey")
		return
	}
	if params.Get("appsecret") == "" {
		err = fmt.Errorf("utils http get must have parameter appsecret")
		return
	}
	if params.Get("sign") != "" {
		err = fmt.Errorf("utils http get must have not parameter sign")
		return
	}
	// sign
	secret := params.Get("appsecret")
	params.Del("appsecret")
	tmp := params.Encode()
	if strings.IndexByte(tmp, '+') > -1 {
		tmp = strings.Replace(tmp, "+", "%20", -1)
	}
	mh := md5.Sum([]byte(tmp + secret))
	params.Set("sign", hex.EncodeToString(mh[:]))
	query = params.Encode()
	return
}

// newRequest new http request with method, uri, ip and values.
func newRequest(method, uri, realIP string, params url.Values) (req *xhttp.Request, err error) {
	enc, err := Sign(params)
	if err != nil {
		log.Error("http check params or sign error(%v)", err)
		return
	}
	ru := uri
	if enc != "" {
		ru = uri + "?" + enc
	}
	if method == xhttp.MethodGet {
		req, err = xhttp.NewRequest(xhttp.MethodGet, ru, nil)
	} else {
		req, err = xhttp.NewRequest(xhttp.MethodPost, uri, strings.NewReader(enc))
	}
	if err != nil {
		log.Error("http.NewRequest(%s, %s) error(%v)", method, ru, err)
		return
	}
	if realIP != "" {
		req.Header.Set("X-BACKEND-BILI-REAL-IP", realIP)
	}
	if method == xhttp.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req.Header.Set("User-Agent", _noKickUserAgent)
	return
}

// realUrl return url with http://host/params.
func realURL(req *xhttp.Request) string {
	if req.Method == xhttp.MethodGet {
		return req.URL.String()
	} else if req.Method == xhttp.MethodPost {
		ru := req.URL.Path
		if req.Body != nil {
			rd, ok := req.Body.(io.Reader)
			if ok {
				buf := bytes.NewBuffer([]byte{})
				buf.ReadFrom(rd)
				ru = ru + "?" + buf.String()
			}
		}
		return ru
	}
	return req.URL.Path
}
