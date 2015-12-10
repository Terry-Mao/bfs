package main

import (
	"crypto/rand"
	"github.com/Terry-Mao/bfs/libs/encoding/binary"
	"github.com/Terry-Mao/bfs/libs/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	mrand "math/rand"
	"os"
	"testing"
)

func TestVolume(t *testing.T) {
	var (
		ts    int32
		v     *Volume
		err   error
		data  = []byte("test")
		buf   = make([]byte, 1024)
		bfile = "./test/test1"
		ifile = "./test/test1.idx"
		n     = &needle.Needle{}
		ns    = make([]needle.Needle, 3)
	)
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	if v, err = NewVolume(1, bfile, ifile, testConf); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		t.FailNow()
	}
	v.Close()
	// test open
	if err = v.Open(); err != nil {
		t.Errorf("Open() error(%v)", err)
		t.FailNow()
	}
	defer v.Close()
	n.Init(1, 1, data)
	n.Write(buf)
	if err = v.Add(n, buf[:n.TotalSize]); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if err = v.Add(n, buf[:n.TotalSize]); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n.Init(2, 2, data)
	n.Write(buf)
	if err = v.Add(n, buf[:n.TotalSize]); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n.Init(3, 3, data)
	n.Write(buf)
	if err = v.Add(n, buf[:n.TotalSize]); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n = &(ns[0])
	n.Init(4, 4, data)
	n.Write(buf)
	ts += n.TotalSize
	n = &(ns[1])
	n.Init(5, 5, data)
	n.Write(buf[ts:])
	ts += n.TotalSize
	n = &(ns[2])
	n.Init(6, 6, data)
	n.Write(buf[ts:])
	ts += n.TotalSize
	if err = v.Write(ns, buf[:ts]); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Del(3); err != nil {
		t.Errorf("Del error(%v)", err)
		t.FailNow()
	}
	if err = v.Get(3, 3, buf, n); err != errors.ErrNeedleDeleted {
		t.Error("err must be ErrNeedleDeleted")
		t.FailNow()
	} else {
		err = nil
	}
}

func BenchmarkVolumeAdd(b *testing.B) {
	var (
		v     *Volume
		err   error
		file  = "./test/testb1"
		ifile = "./test/testb1.idx"
		data  = make([]byte, 16351) // 16kb
	)
	os.Remove(file)
	os.Remove(ifile)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		b.FailNow()
	}
	if v, err = NewVolume(1, file, ifile, testConf); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var (
			t    int64
			err1 error
			buf  = make([]byte, 16351*2)
			n    = &needle.Needle{}
		)
		n.Init(1, 1, data)
		n.Write(buf)
		for pb.Next() {
			t = mrand.Int63()
			n.Key = t
			binary.BigEndian.PutInt64(buf[needle.KeyOffset:], n.Key)
			if err1 = v.Add(n, buf[:n.TotalSize]); err1 != nil {
				b.Errorf("Add() error(%v)", err1)
				b.FailNow()
			}
			b.SetBytes(int64(n.TotalSize))
		}
	})
	os.Remove(file)
	os.Remove(ifile)
}

func BenchmarkVolumeWrite(b *testing.B) {
	var (
		v     *Volume
		err   error
		file  = "./test/testb2"
		ifile = "./test/testb2.idx"
		data  = make([]byte, 16351) // 16kb
	)
	os.Remove(file)
	os.Remove(ifile)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		b.FailNow()
	}
	if v, err = NewVolume(1, file, ifile, testConf); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	defer v.Close()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var (
			i, j int
			ts   int32
			t    int64
			err1 error
			n    *needle.Needle
			ns   = make([]needle.Needle, 9)
			buf  = make([]byte, 16351*10) // 16kb
		)
		for i = 0; i < 9; i++ {
			t = mrand.Int63()
			n = &ns[i]
			n.Init(t, 1, data)
			n.Write(buf[ts:])
			ts += n.TotalSize
		}
		for pb.Next() {
			for j = 0; j < 9; j++ {
				t = mrand.Int63()
				n = &ns[j]
				n.Key = t
				binary.BigEndian.PutInt64(buf[n.TotalSize+needle.KeyOffset:], n.Key)
			}
			if err1 = v.Write(ns, buf); err1 != nil {
				b.Errorf("Add() error(%v)", err1)
				v.Unlock()
				b.FailNow()
			}
			b.SetBytes(int64(ts))
		}
	})
	os.Remove(file)
	os.Remove(ifile)
}

func BenchmarkVolumeGet(b *testing.B) {
	var (
		i     int64
		t     int64
		v     *Volume
		err   error
		file  = "./test/testb3"
		ifile = "./test/testb3.idx"
		buf   = make([]byte, 16777183*2) // 32kb
		data  = make([]byte, 16777183)   // 16kb
		n     = &needle.Needle{}
	)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		b.FailNow()
	}
	if v, err = NewVolume(1, file, ifile, testConf); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	defer v.Close()
	n.Init(1, 1, data)
	n.Write(buf)
	for i = 0; i < 1000000; i++ {
		n.Key = i
		binary.BigEndian.PutInt64(buf[needle.KeyOffset:], n.Key)
		if err = v.Add(n, buf[:n.TotalSize]); err != nil {
			b.Errorf("Add() error(%v)", err)
			b.FailNow()
		}
		t++
	}
	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		var (
			err1 error
			n    = &needle.Needle{}
			buf  = make([]byte, testConf.BatchMaxNum*testConf.NeedleMaxSize)
		)
		for pb.Next() {
			t1 := mrand.Int63n(1000000)
			if err1 = v.Get(t1, 1, buf, n); err1 != nil {
				b.Errorf("Get(%d) error(%v)", t1, err1)
				b.FailNow()
			}
		}
	})
}
