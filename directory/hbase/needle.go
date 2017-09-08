package hbase

import (
	"bfs/libs/errors"
	"bfs/libs/gohbase/hrpc"
	"bfs/libs/meta"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"

	log "github.com/golang/glog"
)

var (
	_table = []byte("bfsmeta")

	_familyBasic      = "basic" // basic store info column family
	_columnVid        = "vid"
	_columnCookie     = "cookie"
	_columnUpdateTime = "update_time"
)

func (c *Client) delNeedle(key int64) (err error) {
	var (
		mutate *hrpc.Mutate
	)
	if mutate, err = hrpc.NewDel(context.Background(), _table, c.key(key), nil); err != nil {
		log.Errorf("Client.delNeedle.NewDel(%v) error:%v", key, err.Error())
		return
	}
	if _, err = c.c.Delete(mutate); err != nil {
		log.Errorf("Client.delNeedle.Delete(%v) error:%v", key, err.Error())
	}
	return
}

func (c *Client) putNeedle(n *meta.Needle) (err error) {
	var (
		mutate *hrpc.Mutate
		vbuf   = make([]byte, 4)
		cbuf   = make([]byte, 4)
		ubuf   = make([]byte, 8)
		exist  bool
	)
	if exist, err = c.existNeedle(n.Key); err != nil {
		return
	}
	if exist {
		err = errors.ErrNeedleExist
		return
	}
	binary.BigEndian.PutUint32(vbuf, uint32(n.Vid))
	binary.BigEndian.PutUint32(cbuf, uint32(n.Cookie))
	binary.BigEndian.PutUint64(ubuf, uint64(n.MTime))
	values := map[string]map[string][]byte{
		_familyBasic: map[string][]byte{
			_columnVid:        vbuf,
			_columnCookie:     cbuf,
			_columnUpdateTime: ubuf,
		},
	}
	if mutate, err = hrpc.NewPut(context.Background(), _table, c.key(n.Key), values); err != nil {
		log.Errorf("Client.putNeedle.NewPut(%v) error:%v", n.Key, err.Error())
		return
	}
	if _, err = c.c.Put(mutate); err != nil {
		log.Errorf("Client.putNeedle.Put(%v) error:%v", n.Key, err.Error())
	}
	return
}

func (c *Client) existNeedle(key int64) (exist bool, err error) {
	var (
		getter *hrpc.Get
		result *hrpc.Result
	)
	if getter, err = hrpc.NewGet(context.Background(), _table, c.key(key)); err != nil {
		log.Errorf("Client.existNeedle.NewGet(%v) error:%v", key, err.Error())
		return
	}
	result, err = c.c.Get(getter)
	if err != nil {
		log.Errorf("Client.existNeedle.Get(%v) error:%v", key, err.Error())
		return
	}
	if result == nil || len(result.Cells) == 0 {
		return
	}
	exist = true
	return
}

func (c *Client) getNeedle(key int64) (n *meta.Needle, err error) {
	var (
		getter *hrpc.Get
		result *hrpc.Result
	)
	if getter, err = hrpc.NewGet(context.Background(), _table, c.key(key)); err != nil {
		log.Errorf("Client.getNeedle.NewGet(%v) error:%v", key, err.Error())
		return
	}
	result, err = c.c.Get(getter)
	if err != nil {
		log.Errorf("Client.getNeedle.Get(%v) error:%v", key, err.Error())
		return
	}
	if result == nil || len(result.Cells) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	n = &meta.Needle{
		Key: key,
	}
	for _, cell := range result.Cells {
		if cell == nil {
			continue
		}
		if bytes.Equal(cell.Family, []byte(_familyBasic)) {
			if bytes.Equal(cell.Qualifier, []byte(_columnVid)) {
				n.Vid = int32(binary.BigEndian.Uint32(cell.Value))
			} else if bytes.Equal(cell.Qualifier, []byte(_columnCookie)) {
				n.Cookie = int32(binary.BigEndian.Uint32(cell.Value))
			} else if bytes.Equal(cell.Qualifier, []byte(_columnUpdateTime)) {
				n.MTime = int64(binary.BigEndian.Uint64(cell.Value))
			}
		}
	}
	return
}

func (c *Client) key(key int64) []byte {
	var (
		sb [sha1.Size]byte
		b  []byte
	)
	b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(key))
	sb = sha1.Sum(b)
	return sb[:]
}
