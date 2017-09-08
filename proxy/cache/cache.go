package cache

import (
	"encoding/json"
	"fmt"
	"time"

	"bfs/libs/memcache"
	"bfs/libs/meta"

	log "github.com/golang/glog"
)

// Cache proxy cache.
type Cache struct {
	mc     *memcache.Pool
	expire int32
}

// New new cache instance.
func New(c *memcache.Config, expire time.Duration) (cache *Cache) {
	cache = &Cache{}
	cache.expire = int32(time.Duration(expire) / time.Second)
	cache.mc = memcache.NewPool(c)
	return
}

// Ping check memcache health
func (c *Cache) Ping() (err error) {
	conn := c.mc.Get()
	err = conn.Store("set", "ping", []byte{1}, 0, c.expire, 0)
	conn.Close()
	return
}

func fileKey(bucket, filename string) string {
	return fmt.Sprintf("f/%s/%s", bucket, filename)
}

func metaKey(bucket, filename string) string {
	return fmt.Sprintf("m/%s/%s", bucket, filename)
}

// Meta get meta info from cache.
func (c *Cache) Meta(bucket, fileName string) (mf *meta.File, err error) {
	var (
		bs  []byte
		key = metaKey(bucket, fileName)
	)
	bs, err = c.get(key)
	if err != nil {
		if err == memcache.ErrNotFound {
			err = nil
			return
		}
		log.Errorf("cache Meta.get(%v) error(%v)", key, err)
		return
	}
	mf = new(meta.File)
	if err = json.Unmarshal(bs, mf); err != nil {
		log.Errorf("cache Meta.Unmarshal(%s) error(%v)", bs, err)
	}
	return
}

// SetMeta set meta into cache.
func (c *Cache) SetMeta(bucket, fileName string, mf *meta.File) (err error) {
	key := metaKey(bucket, fileName)
	bs, err := json.Marshal(mf)
	if err != nil {
		log.Errorf("cache setMeta() Marshal(%s) error(%v)", bs, err)
		return
	}
	if err = c.set(key, bs, c.expire); err != nil {
		log.Errorf("cache setMeta() set(%s,%s) error(%v)", key, string(bs), err)
	}
	return
}

// DelMeta del meta from cache.
func (c *Cache) DelMeta(bucket, fileName string) (err error) {
	key := metaKey(bucket, fileName)
	if err = c.del(key); err != nil {
		log.Errorf("cache DelMeta(%s) error(%v)", key, err)
	}
	return
}

// File get file from cache.
func (c *Cache) File(bucket, fileName string) (bs []byte, err error) {
	key := fileKey(bucket, fileName)
	bs, err = c.get(key)
	if err != nil {
		if err == memcache.ErrNotFound {
			err = nil
			return
		}
		log.Errorf("cache File(%s) error(%v)", key, err)
	}
	return
}

// SetFile set file into cache.
func (c *Cache) SetFile(bucket, fileName string, bs []byte) (err error) {
	key := fileKey(bucket, fileName)
	if err = c.set(key, bs, c.expire); err != nil {
		log.Errorf("cache setFile(%s) error(%v)", key, err)
	}
	return
}

// DelFile del file from cache.
func (c *Cache) DelFile(bucket, fileName string) (err error) {
	key := fileKey(bucket, fileName)
	if err = c.del(key); err != nil {
		log.Errorf("cache DelFile(%s) error(%v)", key, err)
	}
	return
}

func (c *Cache) set(key string, bs []byte, expire int32) (err error) {
	conn := c.mc.Get()
	defer conn.Close()
	return conn.Store("set", key, bs, 0, expire, 0)
}

func (c *Cache) get(key string) (bs []byte, err error) {
	var (
		conn = c.mc.Get()
	)
	defer conn.Close()
	if bs, err = conn.Get2("get", key); err != nil {
		return
	}
	return
}

func (c *Cache) del(key string) (err error) {
	conn := c.mc.Get()
	defer conn.Close()
	return conn.Delete(key)
}
