package main

import (
	"os"
	"testing"
	"time"
)

func init() {
}

func TestIndex(t *testing.T) {
	var (
		file    = "./test/test.idx"
		needles = make(map[int64]NeedleCache)
		noffset uint32
	)
	i, err := NewIndexer(file, 10, 1024)
	if err != nil {
		t.Errorf("NewIndexer(\"%s\", 10, 1024)", file)
		goto failed
	}
	defer os.Remove(file)
	// test add
	if err = i.Add(1, 1, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	if err = i.Add(2, 2, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	// test append
	if err = i.Append(5, 3, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	if err = i.Append(6, 4, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	i.Signal()
	time.Sleep(1 * time.Second)
	// test recovery
	if noffset, err = i.Recovery(needles); err != nil {
		t.Errorf("i.Recovery() error(%v)", err)
		goto failed
	}
	// add 4 index, start with 5
	if noffset != 5 {
		t.Errorf("noffset: %d not match", noffset)
		goto failed
	}
	if o, s := needles[1].Value(); o != 1 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if o, s := needles[2].Value(); o != 2 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if o, s := needles[5].Value(); o != 3 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if o, s := needles[6].Value(); o != 4 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	// test write
	if err = i.Write(10, 5, 8); err != nil {
		t.Error("i.Write() error(%v)", err)
		goto failed
	}
	// test flush
	if err = i.Flush(); err != nil {
		t.Error("i.Flush() error(%v)", err)
		goto failed
	}
	// test recovery
	if noffset, err = i.Recovery(needles); err != nil {
		t.Errorf("i.Recovery() error(%v)", err)
		goto failed
	}
	// add 5 index, start with 6
	if noffset != 6 {
		t.Errorf("noffset: %d not match", noffset)
		goto failed
	}
	if o, s := needles[10].Value(); o != 5 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
failed:
	i.Close()
	if err != nil {
		t.FailNow()
	}
}

func TestIndex1(t *testing.T) {
	var (
		file    = "./test/test1.idx"
		needles = make(map[int64]NeedleCache)
		noffset uint32
	)
	i, err := NewIndexer(file, 10, 1024)
	if err != nil {
		t.Errorf("NewIndexer(\"%s\", 10, 1024)", file)
		goto failed
	}
	defer os.Remove(file)
	// test add
	if err = i.Add(1, 1, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	time.Sleep(1 * time.Second)
	// write a error data
	if _, err = i.f.Write([]byte("test")); err != nil {
		t.Errorf("i.Write() error(%v)", err)
		goto failed
	}
	// try recovery
	if noffset, err = i.Recovery(needles); err != nil {
		t.Errorf("i.Recovery() error(%v)", err)
		goto failed
	}
	// add 1 index, 1 error data, noffset must be 2
	if noffset != 2 {
		t.Errorf("noffset: %d not match", noffset)
		goto failed
	}
	if o, s := needles[1].Value(); o != 1 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if err = i.Add(2, 2, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	time.Sleep(1 * time.Second)
	// try recovery
	if noffset, err = i.Recovery(needles); err != nil {
		t.Errorf("i.Recovery() error(%v)", err)
		goto failed
	}
	// add 2 index, 1 error data, noffset must be 3
	if noffset != 3 {
		t.Errorf("noffset: %d not match", noffset)
		goto failed
	}
	if o, s := needles[2].Value(); o != 2 && s != 8 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
failed:
	i.Close()
	if err != nil {
		t.FailNow()
	}
}
