package main

import (
	"github.com/Terry-Mao/bfs/store/needle"
	"os"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	var (
		s      *Store
		z      *Zookeeper
		v      *Volume
		err    error
		buf    []byte
		data   = []byte("test")
		n      = &needle.Needle{}
		bfile  = "./test/block_1"
		ifile  = "./test/block_1.idx"
		b2file = "./test/block_2"
		i2file = "./test/block_2.idx"
		b3file = "./test/block_3"
		i3file = "./test/block_3.idx"
	)
	os.Remove(testConf.StoreIndex)
	os.Remove(bfile)
	os.Remove(ifile)
	os.Remove(b2file)
	os.Remove(i2file)
	os.Remove(b3file)
	os.Remove(i3file)
	defer os.Remove(testConf.StoreIndex)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	defer os.Remove(b2file)
	defer os.Remove(i2file)
	defer os.Remove(b3file)
	defer os.Remove(i3file)
	if z, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack/test/"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	z.DelVolume(1)
	if s, err = NewStore(z, testConf); err != nil {
		t.Errorf("NewStore() error(%v)", err)
		t.FailNow()
	}
	defer s.Close()
	if _, err = s.AddFreeVolume(2, "./test", "./test"); err != nil {
		t.Errorf("s.AddFreeVolume() error(%v)", err)
		t.FailNow()
	}
	if v, err = s.AddVolume(1); err != nil {
		t.Errorf("AddVolume() error(%v)", err)
		t.FailNow()
	}
	if v = s.Volumes[1]; v == nil {
		t.Error("Volume(1) not exist")
		t.FailNow()
	}
	n.Parse(1, 1, data)
	if err = v.Add(n); err != nil {
		t.Errorf("v.Add(1) error(%v)", err)
		t.FailNow()
	}
	buf = v.Buffer(1)
	defer v.FreeBuffer(1, buf)
	if _, err = v.Get(1, 1, buf); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		t.FailNow()
	}
	if err = s.BulkVolume(2, b2file, i2file); err != nil {
		t.Errorf("Bulk(1) error(%v)", err)
		t.FailNow()
	}
	if v = s.Volumes[2]; v == nil {
		t.Error("Volume(2) not exist")
		t.FailNow()
	}
	if err = v.Add(n); err != nil {
		t.Errorf("v.Add() error(%v)", err)
		t.FailNow()
	}
	if _, err = v.Get(1, 1, buf); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		t.FailNow()
	}
	if err = s.CompactVolume(1); err != nil {
		t.Errorf("Compress(1) error(%v)", err)
		t.FailNow()
	}
	if v = s.Volumes[1]; v == nil {
		t.Error("Volume(1) not exist")
		t.FailNow()
	}
	if _, err = v.Get(1, 1, buf); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		t.FailNow()
	}
	s.DelVolume(1)
	if v = s.Volumes[1]; v != nil {
		t.Error(err)
		t.FailNow()
	}
}
