package hbase

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/Terry-Mao/bfs/directory/hbase/hbasethrift"
	log "github.com/golang/glog"
	"time"
)

const (
	lvsTimeout = time.Second * 80
)

var (
	hbasePool *Pool
)

func Init(hbaseAddr string, hbaseTimeout time.Duration, hbaseMaxIdle, hbaseMaxActive int) error {
	// init hbase thrift pool
	hbasePool = New(func() (c *HbaseConn, err error) {
		var trans thrift.TTransport
		trans, err = thrift.NewTSocketTimeout(hbaseAddr, hbaseTimeout)
		if err != nil {
			log.Error("thrift.NewTSocketTimeout error(%v)", err)
			return
		}
		trans = thrift.NewTFramedTransport(trans)
		c = new(HbaseConn)
		c.conn = hbasethrift.NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		if err = trans.Open(); err != nil {
			log.Error("trans.Open error(%v)", err)
			return
		}
		c.tput.ColumnValues = []*hbasethrift.TColumnValue{
			// vid
			&hbasethrift.TColumnValue{
				Family:    familyBasic,
				Qualifier: columnVid,
				Value:     c.vbuf[:],
			},
			// cookie
			&hbasethrift.TColumnValue{
				Family:    familyBasic,
				Qualifier: columnCookie,
				Value:     c.cbuf[:],
			},
			// insert_time
			&hbasethrift.TColumnValue{
				Family:    familyBasic,
				Qualifier: columnInsertTime,
				Value:     c.ibuf[:],
			},
		}
		c.tdel.Columns = []*hbasethrift.TColumn{
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
			// insert_time
			&hbasethrift.TColumn{
				Family:    familyBasic,
				Qualifier: columnInsertTime,
			},
		}
		return
	}, func(c *HbaseConn) error {
		client := c.conn
		if client != nil && client.Transport != nil {
			client.Transport.Close()
		}
		return nil
	}, hbaseMaxIdle)
	hbasePool.MaxActive = hbaseMaxActive
	hbasePool.IdleTimeout = lvsTimeout
	return nil
}

func Close() {
}
