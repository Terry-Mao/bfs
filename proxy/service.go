package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"time"

	"bfs/libs/errors"
	"bfs/libs/meta"
	"bfs/proxy/bfs"
	"bfs/proxy/cache"
	"bfs/proxy/conf"

	log "github.com/golang/glog"
	"golang.org/x/time/rate"
)

const _mcMaxLength = 1024 * 1024 // memcache max value is 1MB

// Service .
type Service struct {
	cache     *cache.Cache
	bfs       *bfs.Bfs
	cacheChan chan func()
	rl        *rate.Limiter
}

// NewService new service
func NewService(c *conf.Config) (s *Service) {
	s = &Service{
		cache:     cache.New(c.Mc, time.Duration(c.ExpireMc)),
		bfs:       bfs.New(c),
		rl:        rate.NewLimiter(rate.Limit(c.Limit.Rate), c.Limit.Brust),
		cacheChan: make(chan func(), 1024),
	}
	go s.cacheproc()
	return
}

func (s *Service) addCache(f func()) {
	select {
	case s.cacheChan <- f:
	default:
		log.Warningf("s.cacheChan is full")
	}
}

func (s *Service) cacheproc() {
	for f := range s.cacheChan {
		f()
	}
}

// Get get
func (s *Service) Get(bucket, filename string) (src io.ReadCloser, ctlen int, mtime int64, sha1, mine string, err error) {
	var (
		mf *meta.File
		bs []byte
	)
	if mf, err = s.cache.Meta(bucket, filename); err == nil && mf != nil {
		if bs, err = s.cache.File(bucket, filename); err == nil && len(bs) > 0 {
			mtime = mf.MTime
			sha1 = mf.Sha1
			mine = mf.Mine
			ctlen = len(bs)
			src = ioutil.NopCloser(bytes.NewReader(bs))
			return
		}
	}
	if !s.rl.Allow() {
		err = errors.ErrServiceUnavailable
		log.Errorf("service.bfs.Get.RateLimit(%s,%s),error(%v)", bucket, filename, err)
		return
	}
	// get from bfs
	if src, ctlen, mtime, sha1, mine, err = s.bfs.Get(bucket, filename); err != nil {
		log.Errorf("service.bfs.Get(%s,%s),error(%v)", bucket, filename, err)
	}
	return
}

// Upload upload
func (s *Service) Upload(bucket, filename, mine, sha1 string, buf []byte) (err error) {
	var (
		mtime = time.Now().UnixNano()
		mf    *meta.File
	)
	if err = s.bfs.Upload(bucket, filename, mine, sha1, mtime, buf); err != nil && err != errors.ErrNeedleExist {
		log.Errorf("service.bfs.Upload(%s,%s),error(%s)", bucket, filename, err)
		return
	}
	mf = &meta.File{
		MTime: mtime,
		Sha1:  sha1,
		Mine:  mine,
	}
	if len(buf) < _mcMaxLength {
		s.addCache(func() {
			s.cache.SetMeta(bucket, filename, mf)
			s.cache.SetFile(bucket, filename, buf)
		})
	}
	return
}

// Delete delete
func (s *Service) Delete(bucket, filename string) (err error) {
	if err = s.bfs.Delete(bucket, filename); err != nil {
		log.Errorf("service.bfs.Delete(%s,%s),error(%v)", bucket, filename, err)
		return
	}
	s.cache.DelMeta(bucket, filename)
	s.cache.DelFile(bucket, filename)
	return
}

// Ping .
func (s *Service) Ping() (err error) {
	if err = s.bfs.Ping(); err != nil {
		return
	}
	err = s.cache.Ping()
	return
}
