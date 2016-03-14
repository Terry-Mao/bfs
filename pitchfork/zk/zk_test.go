package zk

import (
	"fmt"
	"testing"
	"time"
)

func TestZk(t *testing.T) {

	var (
		config *Config
		zk     *Zookeeper
		err    error
	)

	if config, err = NewConfig(configFile); err != nil {
		t.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}

	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}

	pathStore := fmt.Sprintf("%s/%s/%s", config.ZookeeperStoreRoot, "bfs-1", "47E273ED-CD3A-4D6A-94CE-554BA9B195EB")
	status := uint32(0)
	if err = zk.setStoreStatus(pathStore, status); err != nil {
		t.Errorf("setStoreStatus() error(%v)", err)
		t.FailNow()
	}
}
