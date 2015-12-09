package main

import (
	"testing"
	"time"
	//	"fmt"
)

func TestDispatcher(t *testing.T) {
	var (
		err    error
		config *Config
		zk     *Zookeeper
		d      *Directory
		ds     *Dispatcher
	)
	if config, err = NewConfig("./directory.conf"); err != nil {
		t.Errorf("NewConfig() error(%v)", err)
		return
	}

	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack", "/volume", "/group"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	if d, err = NewDirectory(config, zk); err != nil {
		t.Errorf("NewDirectory() error(%v)", err)
		t.FailNow()
	}
	ds = NewDispatcher(d)
	if err = ds.Update(); err != nil {
		t.Errorf("Update() error(%v)", err)
		t.FailNow()
	}
}
