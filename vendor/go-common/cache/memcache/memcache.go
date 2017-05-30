package memcache

import (
	"context"
	"strings"
	"time"

	"go-common/conf"
	"go-common/net/trace"
	"go-common/stat"
	"golang/gomemcache/memcache"
)

const (
	_family = "memcache"
	_maxTTL = 30*24*60*60 - 1
)

// Conn represents a connection to a Memcache server.
type Conn struct {
	p   *Pool
	c   memcache.Conn
	ctx context.Context
}

// Pool memcache conn pool.
type Pool struct {
	*memcache.Pool
	c     *conf.Memcache
	Stats stat.Stat
}

// NewPool new a memcache conn pool.
func NewPool(c *conf.Memcache) (p *Pool) {
	p = &Pool{c: c}
	cnop := memcache.DialConnectTimeout(time.Duration(c.DialTimeout))
	rdop := memcache.DialReadTimeout(time.Duration(c.ReadTimeout))
	wrop := memcache.DialWriteTimeout(time.Duration(c.WriteTimeout))
	p.Pool = memcache.NewPool(func() (memcache.Conn, error) {
		return memcache.Dial(c.Proto, c.Addr, cnop, rdop, wrop)
	}, c.Idle)
	p.IdleTimeout = time.Duration(c.IdleTimeout)
	p.MaxActive = c.Active
	return
}

// Get gets a connection. The application must close the returned connection.
func (p *Pool) Get(ctx context.Context) *Conn {
	return &Conn{p: p, c: p.Pool.Get(), ctx: ctx}
}

// Close closes the connection.
func (p *Pool) Close() error {
	return p.Pool.Close()
}

// Close closes the connection.
func (c *Conn) Close() error {
	return c.c.Close()
}

// Err returns a non-nil value if the connection is broken. The returned
func (c *Conn) Err() error {
	return c.c.Err()
}

// Store sends a command to the server for store data.
func (c *Conn) Store(cmd, key string, value []byte, flags uint32, timeout int32, cas uint64) (err error) {
	if timeout > _maxTTL {
		timeout = _maxTTL
	}
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, cmd, c.p.c.Addr)
		t.Client(key)
		defer t.Done(&err)
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:"+cmd, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	err = c.c.Store(cmd, key, value, flags, timeout, cas)
	return
}

// Get sends a command to the server for gets data.
func (c *Conn) Get(cmd string, cb func(*memcache.Reply), keys ...string) (err error) {
	var (
		r   *memcache.Reply
		res []*memcache.Reply
	)
	if res, err = c.Gets(cmd, keys...); err != nil {
		return
	}
	for _, r = range res {
		cb(r)
	}
	return
}

// Get2 sends a command to the server for gets data.
func (c *Conn) Get2(cmd string, key string) (res *memcache.Reply, err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, cmd, c.p.c.Addr)
		t.Client(key)
		defer t.Done(&err)
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:"+cmd, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Get(cmd, key)
	return
}

// Gets sends a command to the server for gets data.
func (c *Conn) Gets(cmd string, keys ...string) (res []*memcache.Reply, err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, cmd, c.p.c.Addr)
		t.Client(strings.Join(keys, ","))
		defer t.Done(&err)
	}
	promCmd := cmd
	if len(keys) > 1 {
		promCmd = "gets"
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:"+promCmd, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Gets(cmd, keys...)
	return
}

// Touch sends a command to the server for touch expire.
func (c *Conn) Touch(key string, timeout int32) (err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, "touch", c.p.c.Addr)
		t.Client(key)
		defer t.Done(&err)
	}
	if timeout > _maxTTL {
		timeout = _maxTTL
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:touch", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	err = c.c.Touch(key, timeout)
	return
}

// Delete sends a command to the server for delete data.
func (c *Conn) Delete(key string) (err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, "delete", c.p.c.Addr)
		t.Client(key)
		defer t.Done(&err)
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:delete", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	err = c.c.Delete(key)
	return
}

// IncrDecr sends a command to the server for incr/decr data.
func (c *Conn) IncrDecr(cmd string, key string, delta uint64) (res uint64, err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, cmd, c.p.c.Addr)
		t.Client(key)
		defer t.Done(&err)
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("memcache:"+cmd, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.IncrDecr(cmd, key, delta)
	return
}
