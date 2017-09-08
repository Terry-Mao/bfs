package hbase

import (
	"bfs/directory/conf"
	"bfs/libs/errors"
	"bfs/libs/meta"
	xtime "bfs/libs/time"
	"fmt"
	"testing"
	"time"
)

func getClient() *Client {
	d, err := time.ParseDuration("1s")
	if err != nil {
		panic(err)
	}
	return NewClient(&conf.HBase{
		Master:        "",
		Meta:          "",
		TestRowKey:    "",
		DialTimeout:   xtime.Duration(d),
		ReadTimeout:   xtime.Duration(d),
		ReadsTimeout:  xtime.Duration(d),
		WriteTimeout:  xtime.Duration(d),
		WritesTimeout: xtime.Duration(d),
		ZookeeperHbase: &conf.ZookeeperHbase{
			Root:    "",
			Addrs:   []string{"localhost:2181"},
			Timeout: xtime.Duration(d),
		},
	})
}

func TestGet(t *testing.T) {
	c := getClient()
	fmt.Println(c.c)
	n, f, err := c.Get("test", "guhaotest123.jpg")
	if err != nil && err != errors.ErrNeedleNotExist {
		t.Fatalf("err:%v", err.Error())
	}
	t.Logf("vid:%v,key:%v,cookie:%v,fkey:%v", n.Vid, n.Key, n.Cookie, f.Key)
}

func TestPut(t *testing.T) {
	c := getClient()
	mf := &meta.File{
		Filename: "guhaotest111.jpg",
		Key:      1234567,
		Sha1:     "12312312312312312",
		Mine:     "image/jpg",
		Status:   123,
		MTime:    time.Now().Unix(),
	}
	mn := &meta.Needle{
		Key:    1234567,
		Cookie: 123333,
		Vid:    1,
		MTime:  time.Now().Unix(),
	}
	if err := c.Put("test", mf, mn); err != nil {
		t.Fatalf("err:%v", err.Error())
	}
	t.Logf("pass:%v", mf)
}

func TestDelete(t *testing.T) {
	c := getClient()
	if err := c.Del("test", "guhaotest.jpg"); err != nil {
		t.Fatalf("err:%v", err.Error())
	}
}
