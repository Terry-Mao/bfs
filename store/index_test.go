package main

import (
	"fmt"
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
	defer os.Remove(file)
	i, err := NewIndexer(file, 10)
	if err != nil {
		t.Errorf("NewIndexer(\"%s\", 10, 1024)", file)
		goto failed
	}
	// test add
	t.Log("Test Add(1)")
	if err = i.Add(1, 1, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	t.Log("Test Add(2)")
	if err = i.Add(2, 2, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	// test append
	t.Log("Test Append(5)")
	if err = i.Append(5, 3, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	t.Log("Test Append(6)")
	if err = i.Append(6, 4, 8); err != nil {
		t.Errorf("i.Add() error(%v)", err)
		goto failed
	}
	i.Signal()
	time.Sleep(1 * time.Second)
	// test recovery
	t.Log("Test Recovery()")
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
		err = fmt.Errorf("needle.Value(1) not match")
		t.Error(err)
		goto failed
	}
	if o, s := needles[2].Value(); o != 2 && s != 8 {
		err = fmt.Errorf("needle.Value(2) not match")
		t.Error(err)
		goto failed
	}
	if o, s := needles[5].Value(); o != 3 && s != 8 {
		err = fmt.Errorf("needle.Value(5) not match")
		t.Error(err)
		goto failed
	}
	if o, s := needles[6].Value(); o != 4 && s != 8 {
		err = fmt.Errorf("needle.Value(6) not match")
		t.Error(err)
		goto failed
	}
	// test write
	t.Log("Test Recovery() wrong data")
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
	i, err := NewIndexer(file, 10)
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
	if i != nil {
		i.Close()
	}
	if err != nil {
		t.FailNow()
	}
}
