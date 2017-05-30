package context

import (
	ctx "context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Context web context interface
type Context interface {
	ctx.Context
	Request() *http.Request
	Response() http.ResponseWriter
	Result() map[string]interface{}
	Cancel()
	Now() time.Time
	Get(string) (interface{}, bool)
	Set(string, interface{})
	RemoteIP() string
	User() string
}

// webCtx only used in xhttp/router
type webCtx struct {
	ctx.Context
	cancel   ctx.CancelFunc
	req      *http.Request
	resp     http.ResponseWriter
	res      map[string]interface{}
	now      time.Time
	lock     sync.RWMutex
	data     map[string]interface{}
	remoteIP string
	user     string
}

// NewContext new a web context.
func NewContext(c ctx.Context, user string, req *http.Request, resp http.ResponseWriter) Context {
	wc := &webCtx{req: req, resp: resp, now: time.Now()}
	wc.Context, wc.cancel = ctx.WithCancel(c)
	wc.remoteIP = remoteIP(req)
	wc.user = user
	return wc
}

// Request get a http request.
func (c *webCtx) Request() *http.Request {
	return c.req
}

// Response get a http response.
func (c *webCtx) Response() http.ResponseWriter {
	return c.resp
}

// Cancel cancel handlers.
func (c *webCtx) Cancel() {
	c.cancel()
}

// Now get current time.
func (c *webCtx) Now() time.Time {
	return c.now
}

// Result set a web request when nil then return it.
func (c *webCtx) Result() (res map[string]interface{}) {
	if res = c.res; res == nil {
		res = make(map[string]interface{})
		c.res = res
	}
	return
}

// Get a value by key in context.
func (c *webCtx) Get(key string) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.data == nil {
		return nil, false
	}
	v, ok := c.data[key]
	return v, ok
}

// Set a key-value to the context.
func (c *webCtx) Set(key string, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.data == nil {
		c.data = make(map[string]interface{})
	}
	c.data[key] = value
	return
}

// RemoteAddr allows HTTP servers and other software to record
// the network address that sent the request.
func (c *webCtx) RemoteIP() string {
	return c.remoteIP
}

func (c *webCtx) User() string {
	return c.user
}

// remoteIP get remote real ip.
func remoteIP(r *http.Request) (remote string) {
	if remote = r.Header.Get("X-BACKEND-BILI-REAL-IP"); remote != "" && remote != "null" {
		return
	}
	var xff = r.Header.Get("X-Forwarded-For")
	if idx := strings.IndexByte(xff, ','); idx > -1 {
		if remote = strings.TrimSpace(xff[:idx]); remote != "" {
			return
		}
	}
	if remote = r.Header.Get("X-Real-IP"); remote != "" {
		return
	}
	remote = r.RemoteAddr[:strings.Index(r.RemoteAddr, ":")]
	return
}
