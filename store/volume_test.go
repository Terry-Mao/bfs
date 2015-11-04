package main

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"testing"
)

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
		n      = &Needle{}
	)
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	offset = 134
	size = 1064
	t.Log("NeedleCache")
	nc = NeedleCache(offset, size)
	if offset, size = NeedleCacheValue(nc); offset != 134 || size != 1064 {
		err = fmt.Errorf("NeedlecacheValue() not match")
		t.Error(err)
		t.FailNow()
	}
	t.Log("NewVolume()")
	if v, err = NewVolume(1, bfile, ifile); err != nil {
		t.Errorf("NewVolume() error(%v)", err)
		t.FailNow()
	}
	defer v.Close()
	t.Log("Buffer()")
	if buf = v.Buffer(); len(buf) != NeedleMaxSize {
		err = fmt.Errorf("buf size: %d not match %d", len(buf), NeedleMaxSize)
		t.Error(err)
		t.FailNow()
	}
	defer v.FreeBuffer(buf)
	t.Log("Add(1)")
	if err = n.Parse(1, 1, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	t.Log("Dup Add(1)")
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if o, s := NeedleCacheValue(v.needles[1]); o != 6 && s != 40 {
		err = fmt.Errorf("needle.Value(1) not match")
		t.Error(err)
		t.FailNow()
	}
	t.Log("Add(2)")
	if err = n.Parse(2, 2, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	t.Log("Add(3)")
	if err = n.Parse(3, 3, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Add(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	t.Log("Write(4)")
	if err = n.Parse(4, 4, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	t.Log("Write(5)")
	if err = n.Parse(5, 5, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	t.Log("Write(6)")
	if err = n.Parse(6, 6, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	t.Log("Flush")
	if err = v.Flush(); err != nil {
		t.Errorf("Flush() error(%v)", err)
		t.FailNow()
	}
	t.Log("Del(3)")
	if err = v.Del(3); err != nil {
		t.Errorf("Del error(%v)", err)
		t.FailNow()
	}
	t.Log("Get(3)")
	if _, err = v.Get(3, 3, buf); err != ErrNeedleDeleted {
		err = fmt.Errorf("err must be ErrNeedleDeleted")
		t.Error(err)
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
	if v, err = NewVolume(1, file, ifile); err != nil {
		b.Errorf("NewVolume() error(%v)", err)
		b.FailNow()
	}
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var (
			t    int64
			err1 error
			n    = &Needle{}
		)
		if err = n.Parse(1, 1, data); err != nil {
			b.FailNow()
		}
		for pb.Next() {
			t = mrand.Int63()
			n.Key = t
			n.Cookie = t
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
	if v, err = NewVolume(1, file, ifile); err != nil {
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
			n    = &Needle{}
		)
		if err1 = n.Parse(t, t, data); err1 != nil {
			b.Errorf("n.Parse() error(%v)", err1)
			b.FailNow()
		}
		for pb.Next() {
			v.Lock()
			for i = 0; i < 9; i++ {
				t = mrand.Int63()
				n.Key = t
				n.Cookie = t
				if err1 = v.Write(n); err1 != nil {
					b.Errorf("Add() error(%v)", err1)
					v.Unlock()
					b.FailNow()
				}
				if err1 = v.Flush(); err1 != nil {
					b.Errorf("Flush() error(%v)", err1)
					v.Unlock()
					b.FailNow()
				}
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
		n     = &Needle{}
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
	if err = n.Parse(1, 1, data); err != nil {
		b.Errorf("n.Parse() error(%v)", err)
		b.FailNow()
	}
	for i = 0; i < 1000000; i++ {
		t = int64(i)
		n.Key = t
		n.Cookie = t
		if err = v.Add(n); err != nil {
			b.Errorf("Add() error(%v)", err)
			b.FailNow()
		}
		t++
	}
	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		var buf = make([]byte, NeedleMaxSize)
		var err1 error
		for pb.Next() {
			t1 := mrand.Int63n(1000000)
			if _, err1 = v.Get(t1, t1, buf); err1 != nil {
				b.Errorf("Get(%d) error(%v)", t1, err1)
				b.FailNow()
			}
		}
	})
}
