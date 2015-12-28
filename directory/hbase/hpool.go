package hbase

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/Terry-Mao/bfs/directory/hbase/hbasethrift"
	log "github.com/golang/glog"
	"time"
)

var (
	hbasePool *Pool
)

func Init(hbaseAddr string, hbaseTimeout time.Duration, hbaseMaxIdle, hbaseMaxActive int) error {
	// init hbase thrift pool
	hbasePool = New(func() (client interface{}, err error) {
		var trans thrift.TTransport
		trans, err = thrift.NewTSocketTimeout(hbaseAddr, hbaseTimeout)
		if err != nil {
			log.Error("thrift.NewTSocketTimeout error(%v)", err)
			return
		}
		trans = thrift.NewTFramedTransport(trans)
		client = hbasethrift.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if err = trans.Open(); err != nil {
			log.Error("trans.Open error(%v)", err)
		}
		return
	}, func(c interface{}) error {
		client, ok := c.(hbasethrift.THBaseServiceClient)
		if ok && client.Transport != nil {
			client.Transport.Close()
		}
		return nil
	}, hbaseMaxIdle)
	hbasePool.MaxActive = hbaseMaxActive
	return nil
}

func Close() {
}
