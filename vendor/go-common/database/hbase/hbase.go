package hbase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go-common/conf"
	"go-common/net/trace"
	"go-common/stat"
	"golang/gohbase"
	hconf "golang/gohbase/conf"
	"golang/gohbase/hbase"
	"golang/gohbase/hrpc"
)

const (
	_family = "hbase_client"
)

// Client hbase client.
type Client struct {
	c        gohbase.Client
	testCell *hbase.HBaseCell
	addr     string
	Stats    stat.Stat
}

// NewClient new a hbase client.
func NewClient(c *conf.HBase, options ...gohbase.Option) *Client {
	var testRowKey string
	if c.TestRowKey != "" {
		testRowKey = c.TestRowKey
	} else {
		testRowKey = "test"
	}
	return &Client{
		c: gohbase.NewClient(hconf.NewConf(
			c.Zookeeper.Addrs,
			c.Zookeeper.Root,
			c.Master,
			c.Meta,
			time.Duration(c.Zookeeper.Timeout),
			0,
			0,
			time.Duration(c.DialTimeout),
		), options...),
		testCell: &hbase.HBaseCell{
			"test",
			testRowKey,
			"test",
			"test",
			"test",
		},
		addr: strings.Join(c.Zookeeper.Addrs, ","),
	}
}

// Scan do a scan command.
func (c *Client) Scan(ctx context.Context, s *hrpc.Scan) (res []*hrpc.Result, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, s.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(s.Table()), string(s.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+s.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Scan(s)
	return
}

// Get do a get command.
func (c *Client) Get(ctx context.Context, g *hrpc.Get) (res *hrpc.Result, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, g.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(g.Table()), string(g.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+g.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Get(g)
	return
}

// Put do a put command.
func (c *Client) Put(ctx context.Context, p *hrpc.Mutate) (res *hrpc.Result, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, p.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(p.Table()), string(p.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+p.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Put(p)
	return
}

// Delete do a delete command.
func (c *Client) Delete(ctx context.Context, d *hrpc.Mutate) (res *hrpc.Result, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, d.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(d.Table()), string(d.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+d.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Delete(d)
	return
}

// Append do a append command.
func (c *Client) Append(ctx context.Context, a *hrpc.Mutate) (res *hrpc.Result, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, a.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(a.Table()), string(a.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+a.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Append(a)
	return
}

// Increment do a incr command.
func (c *Client) Increment(ctx context.Context, i *hrpc.Mutate) (res int64, err error) {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, i.CallType().Name, c.addr)
		t.Client(fmt.Sprintf("%s.%s", string(i.Table()), string(i.Key())))
		defer t.Done(&err)
	}
	if c.Stats != nil {
		now := time.Now()
		defer func() {
			c.Stats.Timing("hbase:"+i.CallType().Name, int64(time.Now().Sub(now)/time.Millisecond))
		}()
	}
	res, err = c.c.Increment(i)
	return
}

// Calls do multiple command.
func (c *Client) Calls(ctx context.Context, cs []hrpc.Call) []gohbase.CallResult {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, "calls", c.addr)
		t.Client("")
		defer t.Done(nil)
	}
	defer func() {
		now := time.Now()
		if c.Stats != nil {
			c.Stats.Timing("hbase:calls", int64(time.Now().Sub(now)/time.Millisecond))
		}
	}()
	return c.c.Calls(cs)
}

// Go async go.
func (c *Client) Go(ctx context.Context, cs *hrpc.Calls) []gohbase.CallResult {
	if t, ok := trace.FromContext2(ctx); ok {
		t = t.Fork(_family, "go", c.addr)
		t.Client("")
		defer t.Done(nil)
	}
	defer func() {
		now := time.Now()
		if c.Stats != nil {
			c.Stats.Timing("hbase:go", int64(time.Now().Sub(now)/time.Millisecond))
		}
	}()
	return c.c.Go(cs)
}

// Ping ping.
func (c *Client) Ping(ctx context.Context) (err error) {
	if c.testCell.Valid() {
		var m *hrpc.Mutate
		values := map[string]map[string][]byte{
			c.testCell.Family: map[string][]byte{
				c.testCell.Qualifier: []byte(c.testCell.Value),
			},
		}
		ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		m, err = hrpc.NewPutStr(ctx, c.testCell.Table, c.testCell.RowKey, values)
		if err == nil {
			_, err = c.Put(ctx, m)
		}
	}
	return
}

// Close close client.
func (c *Client) Close() error {
	return c.c.Close()
}
