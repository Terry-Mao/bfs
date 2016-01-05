package hbase

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"github.com/Terry-Mao/bfs/directory/hbase/hbasethrift"
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/libs/meta"
	log "github.com/golang/glog"
	"time"
)

var (
	table            = []byte("bfsmeta")
	familyBasic      = []byte("basic")
	columnVid        = []byte("vid")
	columnCookie     = []byte("cookie")
	columnInsertTime = []byte("insert_time")
)

// HbaseConn
type HbaseConn struct {
	// key, vid, cookie
	kbuf [8]byte
	vbuf [4]byte
	cbuf [4]byte
	ibuf [8]byte
	tget hbasethrift.TGet
	tput hbasethrift.TPut
	tdel hbasethrift.TDelete
	// thrift conn
	conn *hbasethrift.THBaseServiceClient
}

// key get a hbase tget key.
func (c *HbaseConn) key(key int64) []byte {
	var (
		sb [sha1.Size]byte
		b  = c.kbuf[:]
	)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}

type HBaseClient struct {
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	return &HBaseClient{}
}

// Get get meta data from hbase.
func (h *HBaseClient) Get(key int64) (n *meta.Needle, err error) {
	var (
		c  *HbaseConn
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	c.tget.Row = c.key(key)
	if r, err = c.conn.Get(table, &c.tget); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		return
	}
	n = new(meta.Needle)
	n.Key = key
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, familyBasic) {
			if bytes.Equal(cv.Qualifier, columnVid) {
				n.Vid = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, columnCookie) {
				n.Cookie = int32(binary.BigEndian.Uint32(cv.Value))
			}
		}
	}
	return
}

// Put overwriting is bug,  banned
func (h *HBaseClient) Put(n *meta.Needle) (err error) {
	var (
		exist bool
		c     *HbaseConn
		key   []byte
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	key = c.key(n.Key)
	c.tget.Row = key
	if exist, err = c.conn.Exists(table, &c.tget); err != nil {
		hbasePool.Put(c, true)
		return
	}
	if exist {
		return errors.ErrNeedleExist
	}
	binary.BigEndian.PutUint32(c.vbuf[:], uint32(n.Vid))
	binary.BigEndian.PutUint32(c.cbuf[:], uint32(n.Cookie))
	binary.BigEndian.PutUint64(c.ibuf[:], uint64(time.Now().UnixNano()))
	c.tput.Row = key
	if err = c.conn.Put(table, &c.tput); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// Del delete the hbase colume vid and cookie by the key.
func (h *HBaseClient) Del(key int64) (err error) {
	var (
		c *HbaseConn
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	c.tdel.Row = c.key(key)
	if err = c.conn.DeleteSingle(table, &c.tdel); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}
