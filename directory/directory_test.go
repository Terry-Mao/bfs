package main

import (
	"testing"
	"time"
)

func TestDirectory(t *testing.T) {
	var (
		err    error
		config *Config
		zk     *Zookeeper
		d      *Directory
	)
	if config, err = NewConfig("./directory.conf"); err != nil {
		t.Errorf("NewConfig() error(%v)", err)
		t.FailNow()
	}

	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack", "/volume", "/group"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	if d, err = NewDirectory(config, zk); err != nil {
		t.Errorf("NewDirectory() error(%v)", err)
		t.FailNow()
	}
	if _, err = d.syncStores(); err != nil {
		t.Errorf("syncStores() error(%v)", err)
		t.FailNow()
	}
	if _, err = d.syncGroups(); err != nil {
		t.Errorf("syncGroups() error(%v)", err)
		t.FailNow()
	}
	if err = d.syncVolumes(); err != nil {
		t.Errorf("syncVolumes() error(%v)", err)
		t.FailNow()
	}
}
