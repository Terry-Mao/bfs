package main

import (
	"fmt"
	"os"
	"testing"
)

func TestVolume(t *testing.T) {
	var (
		v, nv  *Volume
		err    error
		data   = []byte("test")
		buf    = make([]byte, 40)
		bfile  = "./test/test.volume"
		ifile  = "./test/test.volume.idx"
		nbfile = "./test/testn.volume"
		nifile = "./test/testn.volume.idx"
	)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	t.Log("NewVolume()")
	if v, err = NewVolume(1, bfile, ifile); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		goto failed
	}
	t.Log("Add(1)")
	if err = v.Add(1, 1, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	t.Log("Dup Add(1)")
	if err = v.Add(1, 1, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	if o, s := v.needles[1].Value(); o != 6 && s != 40 {
		err = fmt.Errorf("needle.Value(1) not match")
		t.Error(err)
		goto failed
	}
	t.Log("Add(2)")
	if err = v.Add(2, 2, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	t.Log("Add(3)")
	if err = v.Add(3, 3, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	t.Log("Write(4)")
	if err = v.Write(4, 4, data); err != nil {
		t.Errorf("Write() error(%v)", err)
		goto failed
	}
	t.Log("Write(5)")
	if err = v.Write(5, 5, data); err != nil {
		t.Errorf("Write() error(%v)", err)
		goto failed
	}
	t.Log("Write(6)")
	if err = v.Write(6, 6, data); err != nil {
		t.Errorf("Write() error(%v)", err)
		goto failed
	}
	t.Log("Flush")
	if err = v.Flush(); err != nil {
		t.Errorf("Flush() error(%v)", err)
		goto failed
	}
	t.Log("Del(3)")
	if err = v.Del(3); err != nil {
		t.Errorf("Del error(%v)", err)
		goto failed
	}
	t.Log("Get(3)")
	if _, err = v.Get(3, 3, buf); err != ErrNeedleDeleted {
		err = fmt.Errorf("err must be ErrNeedleDeleted")
		t.Error(err)
		goto failed
	} else {
		err = nil
	}
	t.Log("StartCompress")
	defer os.Remove(nbfile)
	defer os.Remove(nifile)
	if nv, err = NewVolume(1, nbfile, nifile); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		goto failed
	}
	if err = v.StartCompress(nv); err != nil {
		t.Errorf("StartCompress() error(%v)", err)
		goto failed
	}
	if _, err = nv.Get(1, 1, buf); err != nil {
		t.Errorf("Get(1) error(%v)", err)
		goto failed
	}
	if _, err = nv.Get(2, 2, buf); err != nil {
		t.Errorf("Get(1) error(%v)", err)
		goto failed
	}
	if _, err = nv.Get(4, 4, buf); err != nil {
		t.Errorf("Get(1) error(%v)", err)
		goto failed
	}
	// old add
	if err = v.Add(7, 7, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	if err = v.Add(8, 8, data); err != nil {
		t.Errorf("Add() error(%v)", err)
		goto failed
	}
	if err = v.Del(8); err != nil {
		t.Errorf("Del error(%v)", err)
		goto failed
	}
	if err = v.StopCompress(nv); err != nil {
		t.Errorf("StartCompress() error(%v)", err)
		goto failed
	}
	if _, err = nv.Get(7, 7, buf); err != nil {
		t.Errorf("Get(1) error(%v)", err)
		goto failed
	}
	if _, err = v.Get(8, 8, buf); err != ErrNeedleDeleted {
		err = fmt.Errorf("err must be ErrNeedleDeleted")
		t.Error(err)
		goto failed
	} else {
		err = nil
	}
	t.Log("StopCompress")
failed:
	if v != nil {
		v.Close()
	}
	if err != nil {
		t.FailNow()
	}
}
