package main

import (
	"fmt"
	"testing"
	"time"
)

func TestZookeeper(t *testing.T) {
	var (
		zk    *Zookeeper
		err   error
		lines []string
		fpath = "/rack/rack-a/store-a/"
		bfile = "/tmp/hijohn_1"
		ifile = "/tmp/hijohn_1.idx"
	)
	t.Log("NewZookeeper()")
	if zk, err = NewZookeeper([]string{"localhost:2181"}, time.Second, fpath); err != nil {
		t.Errorf("Newzookeeper() error(%v)", err)
		t.FailNow()
	}
	zk.DelVolume(1)
	zk.DelVolume(2)
	t.Log("AddVolume()")
	if err = zk.AddVolume(1, bfile, ifile); err != nil {
		t.Errorf("zk.AddVolume() error(%v)", err)
		t.FailNow()
	}
	t.Log("AddVolume()")
	if err = zk.AddVolume(2, bfile, ifile); err != nil {
		t.Errorf("zk.AddVolume() error(%v)", err)
		t.FailNow()
	}
	t.Log("Volumes")
	if lines, err = zk.Volumes(); err != nil {
		t.Errorf("zk.Volumes() error(%v)", err)
		t.FailNow()
	}
	if len(lines) != 2 || lines[0] != fmt.Sprintf("%s,%s,%d", bfile, ifile, 1) || lines[1] != fmt.Sprintf("%s,%s,%d", bfile, ifile, 2) {
		t.FailNow()
	}
}
