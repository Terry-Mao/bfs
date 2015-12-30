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

type HBaseClient struct {
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	return &HBaseClient{}
}

type HBaseData struct {
	// key, vid, cookie
	kbuf [8]byte
	vbuf [4]byte
	cbuf [4]byte
	ibuf [8]byte
	tget hbasethrift.TGet
	tput hbasethrift.TPut
	tdel hbasethrift.TDelete
}

// NewHBaseClient
func NewHBaseData() *HBaseData {
	d := &HBaseData{}
	d.tput.ColumnValues = []*hbasethrift.TColumnValue{
		// vid
		&hbasethrift.TColumnValue{
			Family:    familyBasic,
			Qualifier: columnVid,
			Value:     d.vbuf[:],
		},
		// cookie
		&hbasethrift.TColumnValue{
			Family:    familyBasic,
			Qualifier: columnCookie,
			Value:     d.cbuf[:],
		},
		// insert_time
		&hbasethrift.TColumnValue{
			Family:    familyBasic,
			Qualifier: columnInsertTime,
			Value:     d.ibuf[:],
		},
	}
	d.tdel.Columns = []*hbasethrift.TColumn{
		// vid
		&hbasethrift.TColumn{
			Family:    familyBasic,
			Qualifier: columnVid,
		},
		// cookie
		&hbasethrift.TColumn{
			Family:    familyBasic,
			Qualifier: columnCookie,
		},
		// insert_time
		&hbasethrift.TColumn{
			Family:    familyBasic,
			Qualifier: columnInsertTime,
		},
	}
	return d
}

// key get a hbase tget key.
func (d *HBaseData) key(key int64) []byte {
	var (
		sb [sha1.Size]byte
		b  = d.kbuf[:]
	)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}

// Get get meta data from hbase.
func (h *HBaseClient) Get(key int64) (n *meta.Needle, err error) {
	var (
		c  interface{}
		d  interface{}
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, d, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	d.(*HBaseData).tget.Row = d.(*HBaseData).key(key)
	if r, err = c.(hbasethrift.THBaseService).Get(table, &d.(*HBaseData).tget); err != nil {
		hbasePool.Put(c, d, true)
		return
	}
	hbasePool.Put(c, d, false)
	if len(r.ColumnValues) == 0 {
		return
	}
	n = new(meta.Needle)
	n.Key = key
	for _, cv = range r.ColumnValues {
		if cv != nil {
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
		c     interface{}
		d     interface{}
		key   []byte
	)
	if c, d, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	key = d.(*HBaseData).key(n.Key)
	d.(*HBaseData).tget.Row = key
	if exist, err = c.(hbasethrift.THBaseService).Exists(table, &d.(*HBaseData).tget); err != nil {
		hbasePool.Put(c, d, true)
		return
	}
	if exist {
		return errors.ErrNeedleExist
	}
	binary.BigEndian.PutUint32(d.(*HBaseData).vbuf[:], uint32(n.Vid))
	binary.BigEndian.PutUint32(d.(*HBaseData).cbuf[:], uint32(n.Cookie))
	binary.BigEndian.PutUint64(d.(*HBaseData).ibuf[:], uint64(time.Now().UnixNano()))
	d.(*HBaseData).tput.Row = key
	if err = c.(hbasethrift.THBaseService).Put(table, &d.(*HBaseData).tput); err != nil {
		hbasePool.Put(c, d, true)
		return
	}
	hbasePool.Put(c, d, false)
	return
}

// Del delete the hbase colume vid and cookie by the key.
func (h *HBaseClient) Del(key int64) (err error) {
	var (
		c interface{}
		d interface{}
	)
	if c, d, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	d.(*HBaseData).tdel.Row = d.(*HBaseData).key(key)
	if err = c.(hbasethrift.THBaseService).DeleteSingle(table, &d.(*HBaseData).tdel); err != nil {
		hbasePool.Put(c, d, true)
		return
	}
	hbasePool.Put(c, d, false)
	return
}
