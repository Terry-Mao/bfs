package main

import (
	"testing"
	// "time"

	"bfs/directory/conf"
	dzk "bfs/directory/zk"
)

func TestDirectory(t *testing.T) {
	var (
		err    error
		config *conf.Config
		zk     *dzk.Zookeeper
		d      *Directory
	)
	if config, err = conf.NewConfig("./directory.toml"); err != nil {
		t.Errorf("NewConfig() error(%v)", err)
		t.FailNow()
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
	if _, err = d.syncStores(); err != nil {
		t.Errorf("syncStores() error(%v)", err)
		t.FailNow()
	}
	if err = d.syncGroups(); err != nil {
		t.Errorf("syncGroups() error(%v)", err)
		t.FailNow()
	}
	if err = d.syncVolumes(); err != nil {
		t.Errorf("syncVolumes() error(%v)", err)
		t.FailNow()
	}
}
