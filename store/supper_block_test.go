package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func compareTestNeedle(t *testing.T, key, cookie int64, flag byte, n *Needle, data, buf []byte) (err error) {
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		t.Errorf("ParseNeedleHeader() error(%v)", err)
		return
	}
	if err = n.ParseData(buf[NeedleHeaderSize:]); err != nil {
		err = fmt.Errorf("ParseNeedleData() error(%v)", err)
		t.Error(err)
		return
	}
	if !bytes.Equal(n.Data, data) {
		err = fmt.Errorf("data: %s not match", n.Data)
		t.Error(err)
		return
	}
	if n.Cookie != cookie {
		err = fmt.Errorf("cookie: %d not match", n.Cookie)
		t.Error(err)
		return
	}
	if n.Key != key {
		err = fmt.Errorf("key: %d not match", n.Key)
		t.Error(err)
		return
	}
	if n.Flag != flag {
		err = fmt.Errorf("flag: %d not match", n.Flag)
		t.Error(err)
		return
	}
	if n.Size != int32(len(data)) {
		err = fmt.Errorf("size: %d not match", n.Size)
		t.Error(err)
		return
	}
	return
}

func TestSuperBlock(t *testing.T) {
	var (
		v       *Volume
		v1      uint32
		v3      int64
		buf     []byte
		n       = &Needle{}
		needles = make(map[int64]int64)
		data    = []byte("test")
		file    = "./test/test.block"
		bfile   = "./test/test.block.compress"
		bifile  = "./test/test.block.compress.idx"
		ifile   = "./test/test.idx"
		indexer *Indexer
	)
	defer os.Remove(file)
	// test new block file
	t.Log("NewSuperBlock() create a new file")
	b, err := NewSuperBlock(file)
	if err != nil {
		t.Errorf("NewSuperBlock(\"%s\") error(%v)", file, err)
		goto failed
	}
	b.Close()
	// test parse block file
	t.Log("NewSuperBlock() create a new file")
	b, err = NewSuperBlock(file)
	if err != nil {
		t.Errorf("NewSuperBlock(\"%s\") error(%v)", file, err)
		goto failed
	}
	if v, err := b.w.Seek(0, os.SEEK_CUR); err != nil {
		t.Errorf("b.Seek() error(%v)", err)
		goto failed
	} else {
		if v != 8 {
			t.Errorf("offset: %d not match", v)
			goto failed
		}
	}
	// test add
	t.Log("Add(1)")
	if err = n.Parse(1, 1, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	if err = b.Add(n); err != nil {
		t.Errorf("b.Add() error(%v)", err)
		goto failed
	}
	if b.Offset != 6 {
		t.Errorf("b.Offset: %d not match", b.Offset)
		goto failed
	}
	if v, err := b.w.Seek(0, os.SEEK_CUR); err != nil {
		t.Errorf("b.Seek() error(%v)", err)
		goto failed
	} else {
		if v != 48 {
			t.Errorf("offset: %d not match", v)
			goto failed
		}
	}
	// test get
	t.Log("Get(1)")
	buf = make([]byte, 40)
	if err = b.Get(1, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 1, 1, NeedleStatusOK, n, data, buf); err != nil {
		t.Error("compareTestNeedle(1)")
		goto failed
	}
	// test add
	t.Log("Add(2)")
	if err = n.Parse(2, 2, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	if err = b.Add(n); err != nil {
		t.Errorf("b.Add() error(%v)", err)
		goto failed
	}
	if b.Offset != 11 {
		t.Errorf("b.Offset: %d not match", b.Offset)
		goto failed
	}
	if v, err := b.w.Seek(0, os.SEEK_CUR); err != nil {
		t.Errorf("b.Seek() error(%v)", err)
		goto failed
	} else {
		if v != 88 {
			t.Errorf("offset: %d not match", v)
			goto failed
		}
	}
	t.Log("Get(2)")
	if err = b.Get(6, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 2, 2, NeedleStatusOK, n, data, buf); err != nil {
		t.Error("compareTestNeedle(2)")
		goto failed
	}
	// test write
	t.Log("Write(3)")
	if err = n.Parse(3, 3, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	if err = b.Write(n); err != nil {
		t.Errorf("b.Add() error(%v)", err)
		goto failed
	}
	if b.Offset != 16 {
		t.Errorf("b.Offset: %d not match", b.Offset)
		goto failed
	}
	// test write
	t.Log("Write(4)")
	if err = n.Parse(4, 4, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	if err = b.Write(n); err != nil {
		t.Errorf("b.Add() error(%v)", err)
		goto failed
	}
	if b.Offset != 21 {
		t.Errorf("b.Offset: %d not match", b.Offset)
		goto failed
	}
	t.Log("Flush()")
	if err = b.Flush(); err != nil {
		t.Errorf("b.Flush() error(%v)", err)
		goto failed
	}
	if v, err := b.w.Seek(0, os.SEEK_CUR); err != nil {
		t.Errorf("b.Seek() error(%v)", err)
		goto failed
	} else {
		if v != 21*8 {
			t.Errorf("offset: %d not match", v)
			goto failed
		}
	}
	t.Log("Get(3)")
	if err = b.Get(11, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 3, 3, NeedleStatusOK, n, data, buf); err != nil {
		t.Error("compareTestNeedle(3)")
		goto failed
	}
	t.Log("Get(4)")
	if err = b.Get(16, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 4, 4, NeedleStatusOK, n, data, buf); err != nil {
		t.Error("compareTestNeedle(r)")
		goto failed
	}
	t.Log("Del(1)")
	// test del, del first needles
	if err = b.Del(1); err != nil {
		t.Errorf("b.Del() error(%v)", err)
		goto failed
	}
	// test get
	if err = b.Get(1, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 1, 1, NeedleStatusDel, n, data, buf); err != nil {
		goto failed
	}
	t.Log("Get(3)")
	if err = b.Get(11, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 3, 3, NeedleStatusOK, n, data, buf); err != nil {
		goto failed
	}
	if err = b.Get(16, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 4, 4, NeedleStatusOK, n, data, buf); err != nil {
		goto failed
	}
	// test recovery
	t.Log("Recovery(0)")
	if indexer, err = NewIndexer(ifile, 10); err != nil {
		t.Errorf("NewIndexer() error(%v)", err)
		goto failed
	}
	defer os.Remove(ifile)
	if err = b.Recovery(0, func(rn *Needle, so, eo uint32) (err1 error) {
		if rn.Flag == NeedleStatusOK {
			if err1 = indexer.Write(n.Key, so, rn.TotalSize); err1 != nil {
				return
			}
		} else {
			so = NeedleCacheDelOffset
		}
		needles[rn.Key] = NeedleCache(so, rn.TotalSize)
		return
	}); err != nil {
		t.Errorf("b.Recovery() error(%v)", err)
		goto failed
	}
	if b.Offset != 21 {
		err = fmt.Errorf("b.Offset not match %d", b.Offset)
		t.Error(err)
		goto failed
	}
	if o, s := NeedleCacheValue(needles[1]); o != NeedleCacheDelOffset && s != 40 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[2]); o != 6 && s != 40 {
		t.Error("needle.Value(2) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[3]); o != 11 && s != 40 {
		t.Error("needle.Value(3) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[4]); o != 16 && s != 40 {
		t.Error("needle.Value(4) not match")
		goto failed
	}
	t.Log("Recovery(6)")
	if err = b.Recovery(6, func(rn *Needle, so, eo uint32) (err1 error) {
		if rn.Flag == NeedleStatusOK {
			if err1 = indexer.Write(n.Key, so, rn.TotalSize); err1 != nil {
				return
			}
		} else {
			so = NeedleCacheDelOffset
		}
		needles[rn.Key] = NeedleCache(so, rn.TotalSize)
		return
	}); err != nil {
		t.Errorf("b.Recovery() error(%v)", err)
		goto failed
	}
	// skip first needle, so key:1 must not exist
	if o, s := NeedleCacheValue(needles[1]); o != 0 && s != 0 {
		t.Error("needle.Value(1) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[2]); o != 6 && s != 40 {
		t.Error("needle.Value(2) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[3]); o != 11 && s != 40 {
		t.Error("needle.Value(3) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(needles[4]); o != 16 && s != 40 {
		t.Error("needle.Value(4) not match")
		goto failed
	}
	if v, err := b.w.Seek(0, os.SEEK_END); err != nil {
		t.Errorf("b.Seek() error(%v)", err)
		goto failed
	} else {
		if v != 168 {
			err = fmt.Errorf("recovery offset %d != %d not match", v, 168)
			t.Error(err)
			goto failed
		}
	}
	// test repair
	t.Log("Repair(3)")
	if err = n.Parse(3, 3, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	n.Fill(buf)
	if err = b.Repair(11, buf); err != nil {
		t.Errorf("b.Repair(3) error(%v)", err)
		goto failed
	}
	if err = b.Get(11, buf); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		goto failed
	}
	if err = compareTestNeedle(t, 3, 3, NeedleStatusOK, n, data, buf); err != nil {
		t.Error("compareTestNeedle(3)")
		goto failed
	}
	// test compress
	t.Log("Compress")
	defer os.Remove(bfile)
	defer os.Remove(bifile)
	if v, err = NewVolume(1, bfile, bifile); err != nil {
		t.Errorf("NewVolume(1) error(%v)", err)
		goto failed
	}
	if err = b.Compact(v1, func(rn *Needle, so, eo uint32) (err1 error) {
		if err1 = v.Write(rn); err1 == nil {
			v1 = eo
		}
		return
	}); err != nil {
		t.Errorf("b.Compress() error(%v)", err)
		goto failed
	} else {
		if err = v.Flush(); err != nil {
			return
		}
		if v3, err = b.w.Seek(0, os.SEEK_END); err != nil {
			t.Errorf("b.Seek() error(%v)", err)
			goto failed
		} else {
			if v1 != NeedleOffset(v3) {
				err = fmt.Errorf("compress offset %d != %d not match", v1, v3)
				t.Error(err)
				goto failed
			}
		}
	}
	// compact
	if o, s := NeedleCacheValue(v.needles[1]); o != 1 && s != 40 {
		t.Errorf("needle.Value(1) not match, o: %d, s: %d", o, s)
		goto failed
	}
	if o, s := NeedleCacheValue(v.needles[2]); o != 6 && s != 40 {
		t.Error("needle.Value(2) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(v.needles[3]); o != 11 && s != 40 {
		t.Error("needle.Value(3) not match")
		goto failed
	}
	if o, s := NeedleCacheValue(v.needles[4]); o != 16 && s != 40 {
		t.Error("needle.Value(4) not match")
		goto failed
	}
	// test dump
failed:
	if b != nil {
		b.Close()
	}
	if indexer != nil {
		indexer.Close()
	}
	if err != nil {
		t.FailNow()
	}
}
