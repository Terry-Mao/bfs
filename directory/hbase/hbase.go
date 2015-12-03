package hbase

import (
	"directory/hbase/hbasethrift"
	"directory/hbase/meta"
	"bytes"
	"time"
	"errors"
	"encoding/binary"
	log "github.com/golang/glog"
)

const (
	retrySleep = time.Second * 1
	retryCount = 3
)

type HBaseClient struct {
}

// NewHBaseClient
func NewHBaseClient() *HBaseClient {
	return &HBaseClient{}
}

// Get if m return nil means not found
func (h *HBaseClient) Get(key int64) (m *meta.Meta, err error) {
	var (
		ks	= make([]byte, 8)
		i     int
		v     uint32
		c     interface{}
		r     *hbasethrift.TResult_
		cv    *hbasethrift.TColumnValue
	)
	binary.BigEndian.PutUint64(ks, uint64(key))
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	for i = 0; i < retryCount; i++ {
		if r, err = c.(hbasethrift.THBaseService).Get(meta.HbaseTable, &hbasethrift.TGet{Row: ks}); err == nil {
			break
		}
		time.Sleep(retrySleep)
	}
	if err != nil {
		log.Errorf("client.Get error(%v)", err)
		return
	}
	if len(r.ColumnValues) == 0 {
		return
	}
	m = &meta.Meta{}
	m.Key = key
	for _, cv = range r.ColumnValues {
		if cv != nil {
			v = binary.BigEndian.Uint32(cv.Value)
			if bytes.Equal(cv.Family, meta.HbaseFamilyBasic) {
				if bytes.Equal(cv.Qualifier, meta.HbaseColumnVid) {
					m.Vid = int32(v)
				} else if bytes.Equal(cv.Qualifier, meta.HbaseColumnCookie) {
					m.Cookie = int32(v)
				}
			}
		}
	}
	return
}

// Put overwriting is bug,  banned
func (h *HBaseClient) Put(m *meta.Meta) (err error) {
	var (
		i     int
		ks  = make([]byte, 8)
		vs  = make([]byte, 4)
		cs  = make([]byte, 4)
		c     interface{}
		exist = false
	)
	if nil == m {
		return errors.New("meta.Meta is nil")
	}
	binary.BigEndian.PutUint64(ks, uint64(m.Key))
	binary.BigEndian.PutUint32(vs, uint32(m.Vid))
	binary.BigEndian.PutUint32(cs, uint32(m.Cookie))
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	for i = 0; i < retryCount; i++ {
		if exist, err = c.(hbasethrift.THBaseService).Exists(meta.HbaseTable, &hbasethrift.TGet{Row: ks}); err == nil {
			break
		}
		time.Sleep(retrySleep)
	}
	if err != nil {
		log.Errorf("client.Exists error(%v)", err)
		return
	}
	if exist {
		return errors.New(fmt.Sprintf("key already exists in hbase  key:%v", m.Key))
	}
	for i = 0; i < retryCount; i++ {
		if err = c.(hbasethrift.THBaseService).Put(meta.HbaseTable, &hbasethrift.TPut{
			Row: ks,
			ColumnValues: []*hbasethrift.TColumnValue{
				&hbasethrift.TColumnValue{
					Family:    meta.HbaseFamilyBasic,
					Qualifier: meta.HbaseColumnVid,
					Value:     vs,
				},
				&hbasethrift.TColumnValue{
					Family:    meta.HbaseFamilyBasic,
					Qualifier: meta.HbaseColumnCookie,
					Value:     cs,
				},
			},
		}); err == nil {
			break
		}
		time.Sleep(retrySleep)
	}
	if err != nil {
		log.Errorf("client.Put error(%v)", err)
	}
	return
}

// Del
func (h *HBaseClient) Del(key int64) (err error) {
	var (
		i     int
		ks  = make([]byte, 8)
		c     interface{}
	)
	binary.BigEndian.PutUint64(ks, uint64(key))
	if c, err = hbasePool.Get(); err != nil {
		log.Errorf("hbasePool.Get() error(%v)", err)
		return
	}
	defer hbasePool.Put(c, false)
	for i = 0; i < retryCount; i++ {
		if err = c.(hbasethrift.THBaseService).DeleteSingle(meta.HbaseTable, &hbasethrift.TDelete{
			Row: ks,
			Columns: []*hbasethrift.TColumn{
				&hbasethrift.TColumn{
					Family:    meta.HbaseFamilyBasic,
					Qualifier: meta.HbaseColumnVid,
				},
				&hbasethrift.TColumn{
					Family:    meta.HbaseFamilyBasic,
					Qualifier: meta.HbaseColumnCookie,
				},
			},
		}); err == nil {
			break
		}
		time.Sleep(retrySleep)
	}
	if err != nil {
		log.Errorf("client.DeleteSingle error(%v)", err)
	}
	return
}