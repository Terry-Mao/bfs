package hbase

import (
	"strings"
	"time"

	"bfs/directory/conf"
	"bfs/libs/errors"
	"bfs/libs/gohbase"
	hconf "bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/hbase"
	"bfs/libs/meta"

	log "github.com/golang/glog"
)

const (
	_family = "hbase_client"
	_prefix = "bucket_"
)

// Client hbase client.
type Client struct {
	c        gohbase.Client
	testCell *hbase.HBaseCell
	addr     string
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
			c.ZookeeperHbase.Addrs,
			c.ZookeeperHbase.Root,
			c.Master,
			c.Meta,
			time.Duration(c.ZookeeperHbase.Timeout),
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
		addr: strings.Join(c.ZookeeperHbase.Addrs, ","),
	}
}

// Put put file and needle into hbase
func (c *Client) Put(bucket string, f *meta.File, n *meta.Needle) (err error) {
	if err = c.putFile(bucket, f); err != nil {
		return
	}
	if err = c.putNeedle(n); err != nil && err != errors.ErrNeedleExist {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, f.Filename)
		c.delFile(bucket, f.Filename)
	}
	return
}

// Get get needle from hbase
func (c *Client) Get(bucket, filename string) (n *meta.Needle, f *meta.File, err error) {
	if f, err = c.getFile(bucket, filename); err != nil {
		return
	}
	if n, err = c.getNeedle(f.Key); err == errors.ErrNeedleNotExist {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, filename)
		c.delFile(bucket, filename)
	}
	return
}

// Del del file and needle from hbase
func (c *Client) Del(bucket, filename string) (err error) {
	var (
		f *meta.File
	)
	if f, err = c.getFile(bucket, filename); err != nil {
		return
	}
	if err = c.delFile(bucket, filename); err != nil {
		return
	}
	err = c.delNeedle(f.Key)
	return
}
