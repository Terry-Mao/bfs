package hbase

import (
	"bilizone/conf"
	"bilizone/model/hbase"
	"common/pool"
	"git.apache.org/thrift.git/lib/go/thrift"
	log "github.com/felixhao/log4go"
)

var (
	hbasePool *pool.Pool
)

func Init() error {
	// init hbase thrift pool
	hbasePool = pool.New(func() (client interface{}, err error) {
		var trans thrift.TTransport
		trans, err = thrift.NewTSocketTimeout(conf.MyConf.HbaseAddr, conf.MyConf.HbaseTimeout)
		if err != nil {
			log.Error("thrift.NewTSocketTimeout error(%v)", err)
			return
		}
		trans = thrift.NewTFramedTransport(trans)
		client = hbase.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if err = trans.Open(); err != nil {
			log.Error("trans.Open error(%v)", err)
		}
		return
	}, func(c interface{}) error {
		client, ok := c.(hbase.THBaseServiceClient)
		if ok && client.Transport != nil {
			client.Transport.Close()
		}
		return nil
	}, conf.MyConf.HbaseMaxIdle)
	hbasePool.MaxActive = conf.MyConf.HbaseMaxActive
	return nil
}

func Close() {
}
