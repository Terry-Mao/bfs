package main

import (
	"fmt"
	log "github.com/golang/glog"
	"os"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	var (
		s      *Store
		v      *Volume
		err    error
		buf    []byte
		data   = []byte("test")
		file   = "./test/store.idx"
		bfile  = "./test/volume"
		ifile  = "./test/volume.idx"
		b2file = "./test/volume2"
		i2file = "./test/volume2.idx"
		b3file = "./test/volume3"
		i3file = "./test/volume3.idx"
	)
	defer os.Remove(file)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	defer os.Remove(b2file)
	defer os.Remove(i2file)
	defer os.Remove(b3file)
	defer os.Remove(i3file)
	t.Log("NewStore()")
	if s, err = NewStore(file); err != nil {
		t.Errorf("NewStore() error(%v)", err)
		goto failed

	}
	defer s.Close()
	t.Log("Buffer()")
	if buf = s.Buffer(); len(buf) != NeedleMaxSize {
		err = fmt.Errorf("buf size: %d not match %d", len(buf), NeedleMaxSize)
		t.Error(err)
		goto failed
	}
	defer s.FreeBuffer(buf)
	t.Log("AddVolume(1)")
	if v, err = s.AddVolume(1, bfile, ifile); err != nil {
		t.Errorf("AddVolume() error(%v)", err)
		goto failed
	}
	time.Sleep(1 * time.Second)
	t.Log("Volume(1)")
	if v = s.Volume(1); v == nil {
		err = fmt.Errorf("Volume(1) not exist")
		t.Error(err)
		goto failed
	}
	if err = v.Add(1, 1, data); err != nil {
		t.Errorf("v.Add(1) error(%v)", err)
		goto failed
	}
	log.Info("123123123")
	t.Log("Bulk(1)")
	if err = s.Bulk(1, b2file, i2file); err != nil {
		t.Errorf("Bulk(1) error(%v)", err)
		goto failed
	}
	time.Sleep(3 * time.Second)
	t.Log("Volume(1)")
	if v = s.Volume(1); v == nil {
		err = fmt.Errorf("Volume(1) not exist")
		t.Error(err)
		goto failed
	}
	if err = v.Add(1, 1, data); err != nil {
		t.Errorf("v.Add(1) error(%v)", err)
		goto failed
	}
	if _, err = v.Get(1, 1, buf); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		goto failed
	}
	t.Log("Compress(1)")
	if err = s.Compress(1, b3file, i3file); err != nil {
		t.Errorf("Compress(1) error(%v)", err)
		goto failed
	}
	time.Sleep(2 * time.Second)
	if v = s.Volume(1); v == nil {
		err = fmt.Errorf("Volume(1) not exist")
		t.Error(err)
		goto failed
	}
	if _, err = v.Get(1, 1, buf); err != nil {
		t.Errorf("v.Get(1) error(%v)", err)
		goto failed
	}
	t.Log("DelVolume(1)")
	s.DelVolume(1)
	time.Sleep(1 * time.Second)
	if v = s.Volume(1); v != nil {
		err = fmt.Errorf("Volume(1) exist")
		t.Error(err)
		goto failed
	}
failed:
	if err != nil {
		t.FailNow()
	}
}
