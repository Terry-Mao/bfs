package hbase

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"github.com/Terry-Mao/bfs/directory/hbase/hbasethrift"
	"github.com/Terry-Mao/bfs/libs/meta"
	"github.com/Terry-Mao/bfs/libs/errors"
	log "github.com/golang/glog"
)

var (
	table        = []byte("bfsmeta")
	familyBasic  = []byte("basic")
	columnVid    = []byte("vid")
	columnCookie = []byte("cookie")
)

type HBaseClient struct {
	// key, vid, cookie
	kbuf [8]byte
	vbuf [4]byte
	cbuf [4]byte
	tget  hbasethrift.TGet
	tput hbasethrift.TPut
	tdel hbasethrift.TDelete
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	h := &HBaseClient{}
	h.tput.ColumnValues = []*hbasethrift.TColumnValue{
		// vid
		&hbasethrift.TColumnValue{
			Family:    familyBasic,
			Qualifier: columnVid,
			Value:     h.vbuf[:],
		},
		// cookie
		&hbasethrift.TColumnValue{
			Family:    familyBasic,
			Qualifier: columnCookie,
			Value:     h.cbuf[:],
		},
	}
	h.tdel.Columns = []*hbasethrift.TColumn{
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
	}
	return h
}

// key get a hbase tget key.
func (h *HBaseClient) key(key int64) []byte {
	var ( 
		sb [sha1.Size]byte
		b = h.kbuf[:]
	)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}

// Get get meta data from hbase.
func (h *HBaseClient) Get(key int64) (n *meta.Needle, err error) {
	var (
		c  interface{}
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	h.tget.Row = h.key(key)
	if r, err = c.(hbasethrift.THBaseService).Get(table, &h.tget); err != nil {
		return
	}
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
		key =  h.key(n.Key)
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	h.tget.Row = key
	if exist, err = c.(hbasethrift.THBaseService).Exists(table, &h.tget); err != nil {
		return
	}
	if exist {
		return errors.ErrNeedleExist
	}
	binary.BigEndian.PutUint32(h.vbuf[:], uint32(n.Vid))
	binary.BigEndian.PutUint32(h.cbuf[:], uint32(n.Cookie))
	h.tput.Row = key
	err = c.(hbasethrift.THBaseService).Put(table, &h.tput)
	return
}

// Del delete the hbase colume vid and cookie by the key.
func (h *HBaseClient) Del(key int64) (err error) {
	var (
		c interface{}
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	h.tdel.Row = h.key(key)
	err = c.(hbasethrift.THBaseService).DeleteSingle(table, &h.tdel)
	return
}
