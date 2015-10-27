package main

import (
	"testing"
)

func TestConfig(t *testing.T) {
	var (
		c    *Config
		err  error
		file = "./test/store.yaml"
	)
	t.Log("NewConfig()")
	if c, err = NewConfig(file); err != nil {
		t.Errorf("NewConfig(\"%s\") error(%v)", file, err)
		t.FailNow()
	}
	if c.Index != "/tmp/hijohn.idx" {
		t.FailNow()
	}
	if c.Stat != "localhost:6061" {
		t.FailNow()
	}
	if !c.Pprof.Enable {
		t.FailNow()
	}
	if c.Pprof.Addr != "localhost:6060" {
		t.FailNow()
	}
	if len(c.Zookeeper.Addrs) != 1 || c.Zookeeper.Addrs[0] != "localhost:2181" {
		t.FailNow()
	}
}
