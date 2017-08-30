package hbase

import (
	"bfs/libs/errors"
	"bfs/libs/gohbase/hrpc"
	"bfs/libs/meta"
	"bytes"
	"context"
	"encoding/binary"
	"time"

	log "github.com/golang/glog"
)

var (
	_familyFile   = "bfsfile" // file info column family
	_columnKey    = "key"
	_columnSha1   = "sha1"
	_columnMine   = "mine"
	_columnStatus = "status"
)

func (c *Client) getFile(bucket, filename string) (f *meta.File, err error) {
	g, err := hrpc.NewGet(context.Background(), c.tableName(bucket), []byte(filename))
	if err != nil {
		log.Errorf("Client.getFile.NewGet(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	result, err := c.c.Get(g)
	if err != nil {
		log.Errorf("Client.getFile.Get(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	if result == nil || len(result.Cells) == 0 {
		err = errors.ErrNeedleNotExist
		return
	}
	f = &meta.File{
		Filename: filename,
	}
	for _, cell := range result.Cells {
		if cell == nil {
			continue
		}
		if bytes.Equal(cell.Family, []byte(_familyFile)) {
			if bytes.Equal(cell.Qualifier, []byte(_columnKey)) {
				f.Key = int64(binary.BigEndian.Uint64(cell.Value))
			} else if bytes.Equal(cell.Qualifier, []byte(_columnSha1)) {
				f.Sha1 = string(cell.Value)
			} else if bytes.Equal(cell.Qualifier, []byte(_columnMine)) {
				f.Mine = string(cell.Value)
			} else if bytes.Equal(cell.Qualifier, []byte(_columnStatus)) {
				f.Status = int32(binary.BigEndian.Uint32(cell.Value))
			} else if bytes.Equal(cell.Qualifier, []byte(_columnUpdateTime)) {
				f.MTime = int64(binary.BigEndian.Uint64(cell.Value))
			}
		}
	}
	return
}

func (c *Client) existFile(bucket, filename string) (exist bool, err error) {
	g, err := hrpc.NewGet(context.Background(), c.tableName(bucket), []byte(filename))
	if err != nil {
		log.Errorf("Client.existFile.NewGet(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	result, err := c.c.Get(g)
	if err != nil {
		log.Errorf("Client.existFile.Get(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	if result == nil || len(result.Cells) == 0 {
		return
	}
	exist = true
	return
}

func (c *Client) putFile(bucket string, f *meta.File) (err error) {
	var (
		kbuf   = make([]byte, 8)
		stbuf  = make([]byte, 4)
		ubuf   = make([]byte, 8)
		mutate *hrpc.Mutate
		exist  bool
	)
	exist, err = c.existFile(bucket, f.Filename)
	if err != nil {
		log.Errorf("Client.putFile.existFile(%v,%v) error:%v", bucket, f.Filename, err.Error())
		return
	}
	if exist {
		if err = c.updateFile(bucket, f.Filename, f.Sha1); err != nil {
			return
		}
		err = errors.ErrNeedleExist
		return
	}
	binary.BigEndian.PutUint64(kbuf, uint64(f.Key))
	binary.BigEndian.PutUint32(stbuf, uint32(f.Status))
	binary.BigEndian.PutUint64(ubuf, uint64(f.MTime))
	values := map[string]map[string][]byte{
		_familyFile: map[string][]byte{
			_columnKey:        kbuf,
			_columnSha1:       []byte(f.Sha1),
			_columnMine:       []byte(f.Mine),
			_columnStatus:     stbuf,
			_columnUpdateTime: ubuf,
		},
	}
	if mutate, err = hrpc.NewPut(context.Background(), c.tableName(bucket), []byte(f.Filename), values); err != nil {
		log.Errorf("Client.putFile.NewPut(%v,%v) error:%v", bucket, f.Filename, err.Error())
		return
	}
	if _, err = c.c.Put(mutate); err != nil {
		log.Errorf("Client.putFile.Put(%v,%v) error:%v", bucket, f.Filename, err.Error())
	}
	return
}

func (c *Client) delFile(bucket, filename string) (err error) {
	var (
		mutate *hrpc.Mutate
	)
	if mutate, err = hrpc.NewDel(context.Background(), c.tableName(bucket), []byte(filename), nil); err != nil {
		log.Errorf("Client.delFile.NewDel(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	if _, err = c.c.Delete(mutate); err != nil {
		log.Errorf("Client.delFile.Delete(%v,%v) error:%v", bucket, filename, err.Error())
	}
	return
}

func (c *Client) updateFile(bucket, filename, sha1 string) (err error) {
	var (
		ubuf   = make([]byte, 8)
		mutate *hrpc.Mutate
	)
	binary.BigEndian.PutUint64(ubuf, uint64(time.Now().UnixNano()))
	values := map[string]map[string][]byte{
		_familyFile: map[string][]byte{
			_columnSha1:       []byte(sha1),
			_columnUpdateTime: ubuf,
		},
	}
	if mutate, err = hrpc.NewPut(context.Background(), c.tableName(bucket), []byte(filename), values); err != nil {
		log.Errorf("Client.updateFile.NewPut(%v,%v) error:%v", bucket, filename, err.Error())
		return
	}
	if _, err = c.c.Put(mutate); err != nil {
		log.Errorf("Client.updateFile.Put(%v,%v) error:%v", bucket, filename, err.Error())
	}
	return
}

func (c *Client) tableName(bucket string) []byte {
	return []byte(_prefix + bucket)
}
