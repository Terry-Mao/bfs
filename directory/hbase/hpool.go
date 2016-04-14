package hbase

import (
	"bfs/directory/conf"
	"bfs/directory/hbase/hbasethrift"
	"git.apache.org/thrift.git/lib/go/thrift"
	log "github.com/golang/glog"
)

var (
	hbasePool *Pool
	config    *conf.Config
)

func Init(config *conf.Config) error {
	config = config
	// init hbase thrift pool
	hbasePool = New(func() (c *hbasethrift.THBaseServiceClient, err error) {
		var trans thrift.TTransport
		if trans, err = thrift.NewTSocketTimeout(config.HBase.Addr, config.HBase.Timeout.Duration); err != nil {
			log.Error("thrift.NewTSocketTimeout error(%v)", err)
			return
		}
		trans = thrift.NewTFramedTransport(trans)
		c = hbasethrift.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if err = trans.Open(); err != nil {
			log.Error("trans.Open error(%v)", err)
		}
		return
	}, func(c *hbasethrift.THBaseServiceClient) error {
		if c != nil && c.Transport != nil {
			c.Transport.Close()
		}
		return nil
	}, config.HBase.MaxIdle)
	hbasePool.MaxActive = config.HBase.MaxActive
	hbasePool.IdleTimeout = config.HBase.LvsTimeout.Duration
	return nil
}

func Close() {
}
