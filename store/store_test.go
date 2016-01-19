package main

import (
	"github.com/Terry-Mao/bfs/store/needle"
	"os"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	var (
		s       *Store
		z       *Zookeeper
		v       *Volume
		err     error
		data    = []byte("test")
		n       = &needle.Needle{}
		bfile   = "./test/_free_block_1"
		ifile   = "./test/_free_block_1.idx"
		vbfile  = "./test/1_0"
		vifile  = "./test/1_0.idx"
		b2file  = "./test/_free_block_2"
		i2file  = "./test/_free_block_2.idx"
		vb2file = "./test/2_0"
		vi2file = "./test/2_0.idx"
	)
	os.Remove(testConf.VolumeIndex)
	os.Remove(testConf.FreeVolumeIndex)
	os.Remove(bfile)
	os.Remove(ifile)
	os.Remove(b2file)
	os.Remove(i2file)
	os.Remove(vbfile)
	os.Remove(vifile)
	os.Remove(vb2file)
	os.Remove(vi2file)
	os.Remove("./test/1_1")
	os.Remove("./test/1_1.idx")
	os.Remove("./test/1_2")
	os.Remove("./test/1_2.idx")
	defer os.Remove(testConf.VolumeIndex)
	defer os.Remove(testConf.FreeVolumeIndex)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	defer os.Remove(b2file)
	defer os.Remove(i2file)
	defer os.Remove(vbfile)
	defer os.Remove(vifile)
	defer os.Remove(vb2file)
	defer os.Remove(vi2file)
	defer os.Remove("./test/1_1")
	defer os.Remove("./test/1_1.idx")
	defer os.Remove("./test/1_2")
	defer os.Remove("./test/1_2.idx")
	if z, err = NewZookeeper([]string{"localhost:2181"}, time.Second*1, "/rack", "", "test"); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	z.DelVolume(1)
	z.DelVolume(2)
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
	n.Buffer = make([]byte, testConf.NeedleMaxSize)
	n.Init(1, 1, data)
	n.Write()
	if err = v.Add(n); err != nil {
		t.Errorf("v.Add(1) error(%v)", err)
		t.FailNow()
	}
	n.Key = 1
	n.Cookie = 1
	if err = v.Get(n); err != nil {
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
	n.Key = 1
	n.Cookie = 1
	if err = v.Get(n); err != nil {
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
	n.Key = 1
	n.Cookie = 1
	if err = v.Get(n); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		t.FailNow()
	}
	s.DelVolume(1)
	if v = s.Volumes[1]; v != nil {
		t.Error(err)
		t.FailNow()
	}
}
