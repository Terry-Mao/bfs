package main

import (
	"bfs/store/needle"
	"bfs/store/volume"
	"bfs/store/zk"
	"bytes"
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	var (
		s   *Store
		z   *zk.Zookeeper
		v   *volume.Volume
		n   *needle.Needle
		err error
		buf = &bytes.Buffer{}
	)
	os.Remove(testConf.Store.VolumeIndex)
	os.Remove(testConf.Store.FreeVolumeIndex)
	os.Remove("./test/_free_block_1")
	os.Remove("./test/_free_block_1.idx")
	os.Remove("./test/_free_block_2")
	os.Remove("./test/_free_block_2.idx")
	os.Remove("./test/_free_block_3")
	os.Remove("./test/_free_block_3.idx")
	os.Remove("./test/1_0")
	os.Remove("./test/1_0.idx")
	os.Remove("./test/1_1")
	os.Remove("./test/1_1.idx")
	os.Remove("./test/block_store_1")
	os.Remove("./test/block_store_1.idx")
	defer os.Remove(testConf.Store.VolumeIndex)
	defer os.Remove(testConf.Store.FreeVolumeIndex)
	defer os.Remove("./test/_free_block_1")
	defer os.Remove("./test/_free_block_1.idx")
	defer os.Remove("./test/_free_block_2")
	defer os.Remove("./test/_free_block_2.idx")
	defer os.Remove("./test/_free_block_3")
	defer os.Remove("./test/_free_block_3.idx")
	defer os.Remove("./test/1_0")
	defer os.Remove("./test/1_0.idx")
	defer os.Remove("./test/1_1")
	defer os.Remove("./test/1_1.idx")
	defer os.Remove("./test/block_store_1")
	defer os.Remove("./test/block_store_1.idx")
	if z, err = zk.NewZookeeper(testConf); err != nil {
		t.Errorf("NewZookeeper() error(%v)", err)
		t.FailNow()
	}
	defer z.Close()
	z.DelVolume(1)
	z.DelVolume(2)
	z.DelVolume(3)
	defer z.DelVolume(1)
	defer z.DelVolume(2)
	defer z.DelVolume(3)
	if s, err = NewStore(testConf); err != nil {
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
	buf.WriteString("test")
	n = needle.NewWriter(1, 1, 4)
	if err = n.ReadFrom(buf); err != nil {
		t.Errorf("n.ReadFrom() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("v.Add(1) error(%v)", err)
		t.FailNow()
	}
	if _, err = v.Read(1, 1); err != nil {
		t.Errorf("v.WriteTo(1) error(%v)", err)
		t.FailNow()
	}
	if err = s.BulkVolume(2, "./test/block_store_1", "./test/block_store_1.idx"); err != nil {
		t.Errorf("Bulk(1) error(%v)", err)
		t.FailNow()
	}
	if v = s.Volumes[2]; v == nil {
		t.Error("Volume(2) not exist")
		t.FailNow()
	}
	buf.WriteString("test")
	n = needle.NewWriter(1, 1, 4)
	if err = n.ReadFrom(buf); err != nil {
		t.Errorf("n.ReadFrom() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("v.Add() error(%v)", err)
		t.FailNow()
	}
	if n, err = v.Read(1, 1); err != nil {
		t.Errorf("v.WriteTo(1) error(%v)", err)
		t.FailNow()
	} else {
		n.Close()
	}
	if err = s.CompactVolume(1); err != nil {
		t.Errorf("Compress(1) error(%v)", err)
		t.FailNow()
	}
	if v = s.Volumes[1]; v == nil {
		t.Error("Volume(1) not exist")
		t.FailNow()
	}
	if n, err = v.Read(1, 1); err != nil {
		t.Errorf("v.WriteTo(1) error(%v)", err)
		t.FailNow()
	} else {
		n.Close()
	}
	s.DelVolume(1)
	if v = s.Volumes[1]; v != nil {
		t.Error(err)
		t.FailNow()
	}
}
