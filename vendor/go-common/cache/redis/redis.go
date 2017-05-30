package redis

import (
	"context"
	"fmt"
	"time"

	"go-common/net/trace"
	"go-common/stat"
	xtime "go-common/time"
	"golang/redigo/redis"
)

const (
	_family = "redis"
)

// Config client settings.
type Config struct {
	Name         string // redis name, for trace
	Proto        string
	Addr         string
	Auth         string
	Active       int // pool
	Idle         int // pool
	DialTimeout  xtime.Duration
	ReadTimeout  xtime.Duration
	WriteTimeout xtime.Duration
	IdleTimeout  xtime.Duration
}

type conn struct {
	p   *Pool
	c   redis.Conn
	t   *trace.Trace2
	ctx context.Context
}

// Pool redis pool.
type Pool struct {
	*redis.Pool
	Stats stat.Stat
	c     *Config
}

// NewPool new a redis pool.
func NewPool(c *Config) (p *Pool) {
	p = &Pool{c: c}
	cnop := redis.DialConnectTimeout(time.Duration(c.DialTimeout))
	rdop := redis.DialReadTimeout(time.Duration(c.ReadTimeout))
	wrop := redis.DialWriteTimeout(time.Duration(c.WriteTimeout))
	auop := redis.DialPassword(c.Auth)
	// new pool
	p.Pool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial(c.Proto, c.Addr, cnop, rdop, wrop, auop)
	}, c.Idle)
	p.IdleTimeout = time.Duration(c.IdleTimeout)
	p.MaxActive = c.Active
	return
}

// NewConn new a redis conn.
func NewConn(c *Config) (cn redis.Conn, err error) {
	cnop := redis.DialConnectTimeout(time.Duration(c.DialTimeout))
	rdop := redis.DialReadTimeout(time.Duration(c.ReadTimeout))
	wrop := redis.DialWriteTimeout(time.Duration(c.WriteTimeout))
	auop := redis.DialPassword(c.Auth)
	// new conn
	cn, err = redis.Dial(c.Proto, c.Addr, cnop, rdop, wrop, auop)
	return
}

// Get get a redis conn.
func (p *Pool) Get(ctx context.Context) redis.Conn {
	return &conn{p: p, c: p.Pool.Get(), ctx: ctx}
}

// Close close the redis pool.
func (p *Pool) Close() error {
	return p.Pool.Close()
}

func (c *conn) Err() error {
	return c.c.Err()
}

func (c *conn) Close() error {
	return c.c.Close()
}

func key(args interface{}) (key string) {
	keys, _ := args.([]interface{})
	if keys != nil && len(keys) > 0 {
		key, _ = keys[0].(string)
	}
	return
}

func (c *conn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if t, ok := trace.FromContext2(c.ctx); ok {
		t = t.Fork(_family, commandName, c.p.c.Addr)
		t.Client(key(args))
		defer t.Done(&err)
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("redis:"+commandName, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	reply, err = c.c.Do(commandName, args...)
	return
}

func (c *conn) Send(commandName string, args ...interface{}) (err error) {
	if c.t == nil {
		if t, ok := trace.FromContext2(c.ctx); ok {
			c.t = t.Fork(_family, "pipeline", c.p.c.Addr)
			c.t.Client("")
			c.t.Annotation(fmt.Sprintf("%s %s", commandName, key(args)))
		}
	} else {
		c.t.Annotation(fmt.Sprintf("%s %s", commandName, key(args)))
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("redis:"+commandName, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	err = c.c.Send(commandName, args...)
	return
}

func (c *conn) Flush() error {
	return c.c.Flush()
}

func (c *conn) Receive() (reply interface{}, err error) {
	if c.t != nil {
		defer c.t.Done(&err)
		c.t = nil
	}
	if c.p.Stats != nil {
		now := time.Now()
		defer func() {
			c.p.Stats.Timing("redis:receive", int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	reply, err = c.c.Receive()
	return
}
