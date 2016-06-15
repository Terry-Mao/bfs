package hbase

import (
	"bfs/directory/hbase/hbasethrift"
	"bfs/libs/errors"
	"bfs/libs/meta"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	log "github.com/golang/glog"
	"time"
)

const (
	_prefix = "bucket_"
)

var (
	_table = []byte("bfsmeta") // default bucket

	_familyBasic      = []byte("basic") // basic store info column family
	_columnVid        = []byte("vid")
	_columnCookie     = []byte("cookie")
	_columnUpdateTime = []byte("update_time")

	_familyFile   = []byte("bfsfile") // file info column family
	_columnKey    = []byte("key")
	_columnSha1   = []byte("sha1")
	_columnMine   = []byte("mine")
	_columnStatus = []byte("status")
	// _columnUpdateTime = []byte("update_time")
)

type HBaseClient struct {
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	return &HBaseClient{}
}

// Get get needle from hbase
func (h *HBaseClient) Get(bucket, filename string) (n *meta.Needle, f *meta.File, err error) {
	if f, err = h.getFile(bucket, filename); err != nil {
		return
	}
	if n, err = h.getNeedle(f.Key); err == errors.ErrNeedleNotExist {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, filename)
		h.delFile(bucket, filename)
	}
	return
}

// Put put file and needle into hbase
func (h *HBaseClient) Put(bucket string, f *meta.File, n *meta.Needle) (err error) {
	if err = h.putFile(bucket, f); err != nil {
		return
	}
	if err = h.putNeedle(n); err != errors.ErrNeedleExist && err != nil {
		log.Warningf("table not match: bucket: %s  filename: %s", bucket, f.Filename)
		h.delFile(bucket, f.Filename)
	}
	return
}

// Del del file and needle from hbase
func (h *HBaseClient) Del(bucket, filename string) (err error) {
	var (
		f *meta.File
	)
	if f, err = h.getFile(bucket, filename); err != nil {
		return
	}
	if err = h.delFile(bucket, filename); err != nil {
		return
	}
	err = h.delNeedle(f.Key)
	return
}

// getNeedle get meta data from hbase.bfsmeta
func (h *HBaseClient) getNeedle(key int64) (n *meta.Needle, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(key)
	if r, err = c.Get(_table, &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	n = new(meta.Needle)
	n.Key = key
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyBasic) {
			if bytes.Equal(cv.Qualifier, _columnVid) {
				n.Vid = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnCookie) {
				n.Cookie = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				n.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			}
		}
	}
	return
}

// putNeedle overwriting is bug,  banned
func (h *HBaseClient) putNeedle(n *meta.Needle) (err error) {
	var (
		ks    []byte
		vbuf  = make([]byte, 4)
		cbuf  = make([]byte, 4)
		ubuf  = make([]byte, 8)
		exist bool
		c     *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(n.Key)
	if exist, err = c.Exists(_table, &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	if exist {
		hbasePool.Put(c, false)
		return errors.ErrNeedleExist
	}
	binary.BigEndian.PutUint32(vbuf, uint32(n.Vid))
	binary.BigEndian.PutUint32(cbuf, uint32(n.Cookie))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	if err = c.Put(_table, &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnVid,
				Value:     vbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnCookie,
				Value:     cbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyBasic,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// delNeedle delete the hbase.bfsmeta colume vid and cookie by the key.
func (h *HBaseClient) delNeedle(key int64) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = h.key(key)
	if err = c.DeleteSingle(_table, &hbasethrift.TDelete{
		Row: ks,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnVid,
			},
			&hbasethrift.TColumn{
				Family:    _familyBasic,
				Qualifier: _columnCookie,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// getFile get file data from hbase.bucket_xxx.
func (h *HBaseClient) getFile(bucket, filename string) (f *meta.File, err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
		r  *hbasethrift.TResult_
		cv *hbasethrift.TColumnValue
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if r, err = c.Get(h.tableName(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	if len(r.ColumnValues) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	f = new(meta.File)
	f.Filename = filename
	for _, cv = range r.ColumnValues {
		if cv == nil {
			continue
		}
		if bytes.Equal(cv.Family, _familyFile) {
			if bytes.Equal(cv.Qualifier, _columnKey) {
				f.Key = int64(binary.BigEndian.Uint64(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnSha1) {
				f.Sha1 = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnMine) {
				f.Mine = string(cv.GetValue())
			} else if bytes.Equal(cv.Qualifier, _columnStatus) {
				f.Status = int32(binary.BigEndian.Uint32(cv.Value))
			} else if bytes.Equal(cv.Qualifier, _columnUpdateTime) {
				f.MTime = int64(binary.BigEndian.Uint64(cv.Value))
			}
		}
	}
	return
}

// putFile overwriting is bug,  banned
func (h *HBaseClient) putFile(bucket string, f *meta.File) (err error) {
	var (
		ks    []byte
		kbuf  = make([]byte, 8)
		stbuf = make([]byte, 4)
		ubuf  = make([]byte, 8)
		exist bool
		c     *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(f.Filename)
	if exist, err = c.Exists(h.tableName(bucket), &hbasethrift.TGet{Row: ks}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	if exist {
		err = h.updateFile(c, bucket, f.Filename, f.Sha1)
		hbasePool.Put(c, err != nil)
		return errors.ErrNeedleExist
	}
	binary.BigEndian.PutUint64(kbuf, uint64(f.Key))
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	if err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnKey,
				Value:     kbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(f.Sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnMine,
				Value:     []byte(f.Mine),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnStatus,
				Value:     stbuf,
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// updateFile overwriting is bug,  banned
func (h *HBaseClient) updateFile(c *hbasethrift.THBaseServiceClient, bucket, filename, sha1 string) (err error) {
	var (
		ks   []byte
		ubuf = make([]byte, 8)
	)
	ks = []byte(filename)
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	err = c.Put(h.tableName(bucket), &hbasethrift.TPut{
		Row: ks,
		ColumnValues: []*hbasethrift.TColumnValue{
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnSha1,
				Value:     []byte(sha1),
			},
			&hbasethrift.TColumnValue{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
				Value:     ubuf,
			},
		},
	})
	return
}

// delFile delete file from hbase.bucket_xxx.
func (h *HBaseClient) delFile(bucket, filename string) (err error) {
	var (
		ks []byte
		c  *hbasethrift.THBaseServiceClient
	)
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	ks = []byte(filename)
	if err = c.DeleteSingle(h.tableName(bucket), &hbasethrift.TDelete{
		Row: ks,
		Columns: []*hbasethrift.TColumn{
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnKey,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnSha1,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnMine,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnStatus,
			},
			&hbasethrift.TColumn{
				Family:    _familyFile,
				Qualifier: _columnUpdateTime,
			},
		},
	}); err != nil {
		hbasePool.Put(c, true)
		return
	}
	hbasePool.Put(c, false)
	return
}

// key hbase bfsmeta
func (h *HBaseClient) key(key int64) []byte {
	var (
		sb [sha1.Size]byte
		b  []byte
	)
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}

// tableName name of bucket table
func (h *HBaseClient) tableName(bucket string) []byte {
	return []byte(_prefix + bucket)
}
