package databus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/net/trace"
	"golang/redigo/redis"
)

const (
	_family     = "databus"
	_actionSub  = "sub"
	_actionPub  = "pub"
	_actionAll  = "pubsub"
	_cmdPub     = "SET"
	_cmdSub     = "MGET"
	_authFormat = "%s:%s@%s/topic=%s&role=%s&offset=%s"
	_retryDelay = 1 * time.Second
	_open       = int32(0)
	_closed     = int32(1)
)

var (
	// ErrAction action error.
	ErrAction = errors.New("action unknown")
	// ErrFull chan full
	ErrFull = errors.New("chan full")
)

// Message Data.
type Message struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	d         *Databus
}

// Commit ack message.
func (m *Message) Commit() (err error) {
	select {
	case m.d.acks <- m:
	default:
		err = ErrFull
	}
	return
}

// Databus databus struct.
type Databus struct {
	conf *conf.Databus
	p    *redis.Pool
	msgs chan *Message
	acks chan *Message

	closed int32
}

// New new a databus.
func New(c *conf.Databus) *Databus {
	if c.Offset == "" {
		c.Offset = "old"
	}
	if c.Buffer == 0 {
		c.Buffer = 1024
	}
	d := &Databus{
		msgs:   make(chan *Message, c.Buffer),
		acks:   make(chan *Message, c.Buffer),
		closed: _open,
	}
	d.conf = c
	if c.Action == _actionSub || c.Action == _actionAll {
		go d.subproc()
	}
	if c.Action == _actionPub || c.Action == _actionAll {
		// new pool
		d.p = redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial(c.Redis.Proto, c.Redis.Addr, d.redisOptions()...)
		}, c.Redis.Idle)
		d.p.IdleTimeout = time.Duration(c.Redis.IdleTimeout)
		d.p.MaxActive = c.Redis.Active
	}
	return d
}

func (d *Databus) redisOptions() []redis.DialOption {
	cnop := redis.DialConnectTimeout(time.Duration(d.conf.Redis.DialTimeout))
	rdop := redis.DialReadTimeout(time.Duration(d.conf.Redis.ReadTimeout))
	wrop := redis.DialWriteTimeout(time.Duration(d.conf.Redis.WriteTimeout))
	auop := redis.DialPassword(fmt.Sprintf(_authFormat, d.conf.Key, d.conf.Secret, d.conf.Group, d.conf.Topic, d.conf.Action, d.conf.Offset))
	return []redis.DialOption{cnop, rdop, wrop, auop}
}

func (d *Databus) subproc() {
	var (
		err    error
		r      []byte
		res    [][]byte
		c      redis.Conn
		marked = make(map[int32]int64)
		commit = make(map[int32]int64)
	)
	for {
		if atomic.LoadInt32(&d.closed) == _closed {
			if c != nil {
				c.Close()
			}
			close(d.msgs)
			return
		}
		if c == nil || c.Err() != nil {
			if c, err = redis.Dial(d.conf.Redis.Proto, d.conf.Redis.Addr, d.redisOptions()...); err != nil {
				log.Error("redis.Dial(%s@%s) retry error(%v)", d.conf.Redis.Proto, d.conf.Redis.Addr, err)
				time.Sleep(_retryDelay)
				continue
			}
		}
		select {
		case m := <-d.acks:
			if m.Offset > marked[m.Partition] {
				marked[m.Partition] = m.Offset
				commit[m.Partition] = m.Offset
			}
			continue
		default:
		}
		// TODO pipeline commit offset
		for k, v := range commit {
			if _, err = c.Do("SET", k, v); err != nil {
				c.Close()
				log.Error("conn.Do(SET,%d,%d) commit error(%v)", k, v, err)
				continue
			}
			delete(commit, k)
		}
		// pull messages
		if res, err = redis.ByteSlices(c.Do(_cmdSub, "")); err != nil {
			c.Close()
			log.Error("conn.Do(MGET) error(%v)", err)
			continue
		}
		for _, r = range res {
			msg := &Message{d: d}
			if err = json.Unmarshal(r, msg); err != nil {
				log.Error("json.Unmarshal(%s) error(%v)", r, err)
				continue
			}
			d.msgs <- msg
		}
	}
}

// Messages get message chan.
func (d *Databus) Messages() <-chan *Message {
	return d.msgs
}

// Send send message to databus.
func (d *Databus) Send(c context.Context, k string, v interface{}) (err error) {
	var b []byte
	// trace info
	if t, ok := trace.FromContext2(c); ok {
		t = t.Fork(_family, _cmdPub, d.conf.Redis.Addr)
		t.Client(k)
		defer t.Done(&err)
	}
	// send message
	if b, err = json.Marshal(v); err != nil {
		log.Error("json.Marshal(%v) error(%v)", v, err)
		return
	}
	conn := d.p.Get()
	if _, err = conn.Do(_cmdPub, k, b); err != nil {
		log.Error("conn.Do(%s,%s,%s) error(%v)", _cmdPub, k, b, err)
	}
	conn.Close()
	return
}

// Close close databus conn.
func (d *Databus) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&d.closed, _open, _closed) {
		return
	}
	if d.p != nil {
		d.p.Close()
	}
	return nil
}
