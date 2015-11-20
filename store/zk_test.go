package main

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestZookeeper(t *testing.T) {
	var (
		v        *Volume
		zk       *Zookeeper
		err      error
		lines    []string
		root     = "/rack"
		rack     = "rack-a"
		serverId = "store-a"
		bfile    = "./test/hijohn_1"
		ifile    = "./test/hijohn_1.idx"
	)
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second, root, rack, serverId); err != nil {
		t.Errorf("Newzookeeper() error(%v)", err)
		t.FailNow()
	}
	zk.DelVolume(1)
	zk.DelVolume(2)
	if v, err = NewVolume(1, bfile, ifile, testConf); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		t.FailNow()
	}
	v.Close()
	if err = zk.AddVolume(v); err != nil {
		t.Errorf("zk.AddVolume() error(%v)", err)
		t.FailNow()
	}
	v.Id = 2
	if err = zk.AddVolume(v); err != nil {
		t.Errorf("zk.AddVolume() error(%v)", err)
		t.FailNow()
	}
	if lines, err = zk.Volumes(); err != nil {
		t.Errorf("zk.Volumes() error(%v)", err)
		t.FailNow()
	}
	if len(lines) != 2 || lines[0] != fmt.Sprintf("%s,%s,%d", bfile, ifile, 1) || lines[1] != fmt.Sprintf("%s,%s,%d", bfile, ifile, 2) {
		t.FailNow()
	}
}
