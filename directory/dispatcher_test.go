package main

import (
	"bfs/directory/conf"
	dzk "bfs/directory/zk"
	"testing"
	"time"
	//	"fmt"
)

func TestDispatcher(t *testing.T) {
	var (
		err    error
		config *conf.Config
		zk     *dzk.Zookeeper
		d      *Directory
		ds     *Dispatcher
		vid    int32
	)
	if config, err = conf.NewConfig("./directory.toml"); err != nil {
		t.Errorf("NewConfig() error(%v)", err)
		return
	}

	if zk, err = dzk.NewZookeeper(config); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	defer zk.Close()
	if d, err = NewDirectory(config); err != nil {
		t.Errorf("NewDirectory() error(%v)", err)
		t.FailNow()
	}
	time.Sleep(5 * time.Second)
	ds = NewDispatcher()
	if err = ds.Update(d.group, d.store, d.volume, d.storeVolume); err != nil {
		t.Errorf("Update() error(%v)", err)
		t.FailNow()
	}
	if vid, err = ds.VolumeID(d.group, d.storeVolume); err != nil {
		t.Errorf("Update() error(%v)", err)
		t.FailNow()
	}
	t.Logf("vid:%v", vid)
}
