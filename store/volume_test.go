package main

import (
	"crypto/rand"
	"github.com/Terry-Mao/bfs/store/errors"
	"github.com/Terry-Mao/bfs/store/needle"
	mrand "math/rand"
	"os"
	"testing"
)

func TestVolume(t *testing.T) {
	var (
		v     *Volume
		err   error
		data  = []byte("test")
		buf   = make([]byte, 1024)
		bfile = "./test/test1"
		ifile = "./test/test1.idx"
		n     = &needle.Needle{}
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
	n.Parse(1, 1, data)
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n.Parse(2, 2, data)
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n.Parse(3, 3, data)
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	n.Parse(4, 4, data)
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	n.Parse(5, 5, data)
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	n.Parse(6, 6, data)
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Flush(); err != nil {
		t.Errorf("Flush() error(%v)", err)
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
			n    = &needle.Needle{}
		)
		n.Parse(1, 1, data)
		for pb.Next() {
			t = mrand.Int63()
			n.Key = t
			if err1 = v.Add(n); err1 != nil {
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
		i     int
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
			t    int64
			err1 error
			n    = &needle.Needle{}
		)
		n.Parse(t, 1, data)
		for pb.Next() {
			v.Lock()
			for i = 0; i < 9; i++ {
				t = mrand.Int63()
				n.Key = t
				if err1 = v.Write(n); err1 != nil {
					b.Errorf("Add() error(%v)", err1)
					v.Unlock()
					b.FailNow()
				}
			}
			if err1 = v.Flush(); err1 != nil {
				b.Errorf("Flush() error(%v)", err1)
				v.Unlock()
				b.FailNow()
			}
			v.Unlock()
			b.SetBytes(int64(n.TotalSize) * 9)
		}
	})
	os.Remove(file)
	os.Remove(ifile)
}

func BenchmarkVolumeGet(b *testing.B) {
	var (
		i     int
		t     int64
		v     *Volume
		err   error
		file  = "./test/testb3"
		ifile = "./test/testb3.idx"
		data  = make([]byte, 16777183) // 16kb
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
	n.Parse(1, 1, data)
	for i = 0; i < 1000000; i++ {
		t = int64(i)
		n.Key = t
		if err = v.Add(n); err != nil {
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
