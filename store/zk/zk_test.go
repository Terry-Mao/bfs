package zk

import (
	"bfs/store/conf"
	"fmt"
	"testing"
	"time"
)

var (
	testConf = &conf.Config{
		Zookeeper: &conf.Zookeeper{
			Root:     "/rack",
			Rack:     "rack-a",
			ServerId: "store-a",
			Addrs:    []string{"localhost:2181"},
			Timeout:  conf.Duration{time.Second},
		},
	}
)

func volumeMeta(bfile, ifile string, id int32) []byte {
	return []byte(fmt.Sprintf("%s,%s,%d", bfile, ifile, id))
}

func TestZookeeper(t *testing.T) {
	var (
		zk    *Zookeeper
		err   error
		lines []string
		bfile = "./test/hijohn_1"
		ifile = "./test/hijohn_1.idx"
	)
	if zk, err = NewZookeeper(testConf); err != nil {
		t.Errorf("Newzookeeper() error(%v)", err)
		t.FailNow()
	}
	zk.DelVolume(1)
	zk.DelVolume(2)
	if err = zk.AddVolume(1, volumeMeta(bfile, ifile, 1)); err != nil {
		t.Errorf("zk.AddVolume() error(%v)", err)
		t.FailNow()
	}
	if err = zk.AddVolume(2, volumeMeta(bfile, ifile, 2)); err != nil {
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
