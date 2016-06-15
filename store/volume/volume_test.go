package volume

import (
	"bfs/libs/errors"
	"bfs/store/conf"
	"bfs/store/needle"
	"bytes"
	"os"
	"testing"
	"time"
)

const (
	_16kb = 16*1024 - needle.HeaderSize - needle.FooterSize
)

var (
	_vc = &conf.Volume{
		SyncDelete:      10,
		SyncDeleteDelay: conf.Duration{10 * time.Second},
	}
	_bc = &conf.Block{
		BufferSize:    4 * 1024 * 1024,
		SyncWrite:     1024,
		Syncfilerange: true,
	}
	_ic = &conf.Index{
		BufferSize:    4 * 1024 * 1024,
		MergeDelay:    conf.Duration{10 * time.Second},
		MergeWrite:    5,
		RingBuffer:    10,
		SyncWrite:     10,
		Syncfilerange: true,
	}
	_c = &conf.Config{
		NeedleMaxSize: 4 * 1024 * 1024,
		Volume:        _vc,
		Block:         _bc,
		Index:         _ic,
	}
)

func TestVolume(t *testing.T) {
	var (
		v     *Volume
		n     *needle.Needle
		err   error
		data  = []byte("test")
		bfile = "../test/test1"
		ifile = "../test/test1.idx"
		ns    = needle.NewNeedles(3)
		buf   = &bytes.Buffer{}
	)
	os.Remove(bfile)
	os.Remove(ifile)
	defer os.Remove(bfile)
	defer os.Remove(ifile)
	if v, err = NewVolume(1, bfile, ifile, _c); err != nil {
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
	// test write
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(1, 1, 4)
	defer n.Close()
	if err = n.ReadFrom(buf); err != nil {
		t.Errorf("n.Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(2, 2, 4)
	defer n.Close()
	if err = n.ReadFrom(buf); err != nil {
		t.Errorf("n.Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(3, 3, 4)
	defer n.Close()
	if err = n.ReadFrom(buf); err != nil {
		t.Errorf("n.Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Write(n); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	if err = ns.ReadFrom(4, 4, 4, buf); err != nil {
		t.Errorf("ns.Write() error(%v)", err)
		t.FailNow()
	}
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	if err = ns.ReadFrom(5, 5, 4, buf); err != nil {
		t.Errorf("ns.Write() error(%v)", err)
		t.FailNow()
	}
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	if err = ns.ReadFrom(6, 6, 4, buf); err != nil {
		t.Errorf("ns.Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Writes(ns); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	if err = v.Delete(3); err != nil {
		t.Errorf("Del error(%v)", err)
		t.FailNow()
	}
	if _, err = v.Read(3, 3); err != errors.ErrNeedleDeleted {
		t.Error("err must be ErrNeedleDeleted")
		t.FailNow()
	} else {
		err = nil
	}
}

/*
func BenchmarkVolumeAdd(b *testing.B) {
	var (
		v     *Volume
		err   error
		file  = "./test/testb1"
		ifile = "./test/testb1.idx"
		data  = make([]byte, _16kb) // 16kb
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
			n    = &needle.Needle{Buffer: buf}
		)
		n.Init(1, 1, data)
		n.Write()
		for pb.Next() {
			t = mrand.Int63()
			n.Key = t
			binary.BigEndian.PutInt64(buf[needle.KeyOffset:], n.Key)
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
		v     *Volume
		err   error
		file  = "./test/testb2"
		ifile = "./test/testb2.idx"
		data  = make([]byte, _16kb) // 16kb
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
			t    int64
			err1 error
			n    *needle.Needle
			buf  = make([]byte, 163840) // 16kb
			ns   = &needle.Needles{
				Items:  make([]needle.Needle, 9),
				Buffer: buf,
			}
		)
		for i = 0; i < 9; i++ {
			t = mrand.Int63()
			n = &ns.Items[i]
			n.Init(t, 1, data)
			ns.Write(n)
		}
		for pb.Next() {
			for j = 0; j < 9; j++ {
				t = mrand.Int63()
				n = &ns[j]
				n.Key = t
				binary.BigEndian.PutInt64(buf[n.TotalSize+needle.KeyOffset:], n.Key)
			}
			if err1 = v.Write(ns); err1 != nil {
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
*/
