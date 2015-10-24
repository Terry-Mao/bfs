package main

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"testing"
)

func init() {
	StartPprof("localhost:6060")
}

func TestVolume(t *testing.T) {
	var (
		v      *Volume
		nc     int64
		err    error
		offset uint32
		size   int32
		data   = []byte("test")
		buf    []byte
		bfile  = "./test/test1"
		ifile  = "./test/test1.idx"
	)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	offset = 134
	size = 1064
	t.Log("NeedleCache")
	nc = NeedleCache(offset, size)
	if offset, size = NeedleCacheValue(nc); offset != 134 || size != 1064 {
		err = fmt.Errorf("NeedlecacheValue() not match")
		t.Error(err)
		goto failed
	}
	t.Log("NewVolume()")
	if v, err = NewVolume(1, bfile, ifile); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		goto failed
	}
	t.Log("Buffer()")
	if buf = v.Buffer(); len(buf) != NeedleMaxSize {
		err = fmt.Errorf("buf size: %d not match %d", len(buf), NeedleMaxSize)
		t.Error(err)
		goto failed
	}
	defer v.FreeBuffer(buf)
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
	if o, s := NeedleCacheValue(v.needles[1]); o != 6 && s != 40 {
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
failed:
	if v != nil {
		v.Close()
	}
	if err != nil {
		t.FailNow()
	}
}

var (
	t int64
)

func BenchmarkVolumeAdd(b *testing.B) {
	var (
		v     *Volume
		err   error
		file  = "./test/testb1"
		ifile = "./test/testb1.idx"
		data  = make([]byte, 1*1024)
	)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		b.FailNow()
	}
	if v, err = NewVolume(1, file, ifile); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = v.Add(t, t, data); err != nil {
			b.Errorf("Add() error(%v)", err)
			b.FailNow()
		}
		t++
	}
}

func BenchmarkVolumeWrite(b *testing.B) {
	var (
		i     int
		t     int64
		v     *Volume
		err   error
		file  = "./test/testb2"
		ifile = "./test/testb2.idx"
		data  = make([]byte, 1*1024)
	)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		goto failed
	}
	if v, err = NewVolume(1, file, ifile); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		goto failed
	}
	defer v.Close()
	b.ResetTimer()
	v.Lock()
	for i = 0; i < b.N; i++ {
		if err = v.Write(t, t, data); err != nil {
			b.Errorf("Add() error(%v)", err)
			goto failed
		}
		t++
	}
	if err = v.Flush(); err != nil {
		b.Errorf("Flush() error(%v)", err)
		goto failed
	}
	v.Unlock()
failed:
	if err != nil {
		b.FailNow()
	}
}

func BenchmarkVolumeGet(b *testing.B) {
	var (
		i     int
		t     int64
		v     *Volume
		err   error
		file  = "./test/testb3"
		ifile = "./test/testb3.idx"
		data  = make([]byte, 1*1024)
	)
	defer os.Remove(file)
	defer os.Remove(ifile)
	if _, err = rand.Read(data); err != nil {
		b.Errorf("rand.Read() error(%v)", err)
		b.FailNow()
	}
	if v, err = NewVolume(1, file, ifile); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	defer v.Close()
	for i = 0; i < 1000000; i++ {
		t = int64(i)
		if err = v.Add(t, t, data); err != nil {
			b.Errorf("Add() error(%v)", err)
			b.FailNow()
		}
		t++
	}
	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		var buf = make([]byte, NeedleMaxSize)
		for pb.Next() {
			t1 := mrand.Int63n(1000000)
			if _, err := v.Get(t1, t1, buf); err != nil {
				b.Errorf("Get(%d) error(%v)", t1, err)
				b.FailNow()
			}
		}
	})
}
