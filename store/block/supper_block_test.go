package block

import (
	"bfs/store/conf"
	"bfs/store/needle"
	"bytes"
	"fmt"
	"os"
	"testing"
)

var (
	testConf = &conf.Config{
		NeedleMaxSize: 4 * 1024 * 1024,
		BlockMaxSize:  needle.Size(4 * 1024 * 1024),
		Block: &conf.Block{
			BufferSize:    4 * 1024 * 1024,
			SyncWrite:     1024,
			Syncfilerange: true,
		},
	}
)

func TestSuperBlock(t *testing.T) {
	var (
		b                  *SuperBlock
		n                  *needle.Needle
		offset, v2, v3, v4 uint32
		err                error
		buf                = &bytes.Buffer{}
		needles            = make(map[int64]int64)
		data               = []byte("test")
		file               = "../test/test.block"
		ifile              = "../test/test.idx"
		//indexer *Indexer
	)
	os.Remove(file)
	os.Remove(ifile)
	defer os.Remove(file)
	defer os.Remove(ifile)
	// test new block file
	if b, err = NewSuperBlock(file, testConf); err != nil {
		t.Errorf("NewSuperBlock(\"%s\") error(%v)", file, err)
		t.FailNow()
	}
	b.Close()
	// test parse block file
	if b, err = NewSuperBlock(file, testConf); err != nil {
		t.Errorf("NewSuperBlock(\"%s\") error(%v)", file, err)
		t.FailNow()
	}
	b.Close()
	// test open
	if err = b.Open(); err != nil {
		t.Errorf("Open() error(%v)", err)
		t.FailNow()
	}
	defer b.Close()
	// test write
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(1, 1, 4)
	defer n.Close()
	n.ReadFrom(buf)
	if err = b.Write(n); err != nil {
		t.Errorf("b.Write() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestOffset(b, n, needle.NeedleOffset(int64(_headerSize))); err != nil {
		t.Errorf("compareTestOffset() error(%v)", err)
		t.FailNow()
	}
	offset = b.Offset
	v2 = b.Offset
	// test get
	n.Offset = 1
	if err = b.ReadAt(n); err != nil {
		t.Errorf("b.ReadAt() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 1, 1, needle.FlagOK, n, data); err != nil {
		t.Errorf("compareTestNeedle() error(%v)", err)
		t.FailNow()
	}
	// test write
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(2, 2, 4)
	defer n.Close()
	n.ReadFrom(buf)
	if err = b.Write(n); err != nil {
		t.Errorf("b.Write() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestOffset(b, n, offset); err != nil {
		t.Errorf("compareTestOffset() error(%v)", err)
		t.FailNow()
	}
	offset = b.Offset
	v3 = b.Offset
	n.Offset = 6
	if err = b.ReadAt(n); err != nil {
		t.Errorf("b.ReadAt() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 2, 2, needle.FlagOK, n, data); err != nil {
		t.Error("compareTestNeedle(2)")
		t.FailNow()
	}
	// test write
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(3, 3, 4)
	defer n.Close()
	n.ReadFrom(buf)
	if err = b.Write(n); err != nil {
		t.Errorf("b.Write() error(%v)", err)
		t.FailNow()
	}
	offset = b.Offset
	v4 = b.Offset
	// test write
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(4, 4, 4)
	defer n.Close()
	n.ReadFrom(buf)
	if err = b.Write(n); err != nil {
		t.Errorf("b.Write() error(%v)", err)
		t.FailNow()
	}
	if err = b.flush(true); err != nil {
		t.Errorf("Flush() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestOffset(b, n, offset); err != nil {
		t.Errorf("compareTestOffset() error(%v)", err)
		t.FailNow()
	}
	n.Offset = 11
	if err = b.ReadAt(n); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 3, 3, needle.FlagOK, n, data); err != nil {
		t.Error("compareTestNeedle(3)")
		t.FailNow()
	}
	n.Offset = 16
	if err = b.ReadAt(n); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 4, 4, needle.FlagOK, n, data); err != nil {
		t.Error("compareTestNeedle(r)")
		t.FailNow()
	}
	// test del, del first needles
	if err = b.Delete(1); err != nil {
		t.Errorf("Del() error(%v)", err)
		t.FailNow()
	}
	// test get
	n.Offset = 1
	if err = b.ReadAt(n); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 1, 1, needle.FlagDel, n, data); err != nil {
		t.FailNow()
	}
	n.Offset = 11
	if err = b.ReadAt(n); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 3, 3, needle.FlagOK, n, data); err != nil {
		t.FailNow()
	}
	n.Offset = 16
	if err = b.ReadAt(n); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 4, 4, needle.FlagOK, n, data); err != nil {
		t.FailNow()
	}
	// test recovery
	offset = b.Offset
	if err = b.Recovery(0, func(rn *needle.Needle, so, eo uint32) (err1 error) {
		if rn.Flag != needle.FlagOK {
			so = needle.CacheDelOffset
		}
		needles[rn.Key] = needle.NewCache(so, rn.TotalSize)
		return
	}); err != nil {
		t.Errorf("Recovery() error(%v)", err)
		t.FailNow()
	}
	if b.Offset != offset {
		err = fmt.Errorf("b.Offset not match %d", b.Offset)
		t.Error(err)
		t.FailNow()
	}
	if o, s := needle.Cache(needles[1]); o != needle.CacheDelOffset && s != 40 {
		t.Error("needle.Cache() not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[2]); o != v2 && s != 40 {
		t.Error("needle.Cache() not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[3]); o != v3 && s != 40 {
		t.Error("needle.Cache() not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[4]); o != v4 && s != 40 {
		t.Error("needle.Cache() not match")
		t.FailNow()
	}
	needles = make(map[int64]int64)
	if err = b.Recovery(v2, func(rn *needle.Needle, so, eo uint32) (err1 error) {
		if rn.Flag != needle.FlagOK {
			so = needle.CacheDelOffset
		}
		needles[rn.Key] = needle.NewCache(so, rn.TotalSize)
		return
	}); err != nil {
		t.Errorf("b.Recovery() error(%v)", err)
		t.FailNow()
	}
	// skip first needle, so key:1 must not exist
	if o, s := needle.Cache(needles[1]); o != 0 && s != 0 {
		t.Error("needle.Value(1) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[2]); o != v2 && s != 40 {
		t.Error("needle.Value(2) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[3]); o != v3 && s != 40 {
		t.Error("needle.Value(3) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[4]); o != v4 && s != 40 {
		t.Error("needle.Value(4) not match")
		t.FailNow()
	}
	// test repair
	if _, err = buf.Write(data); err != nil {
		t.Errorf("buf.Write() error(%v)", err)
		t.FailNow()
	}
	n = needle.NewWriter(3, 3, 4)
	defer n.Close()
	n.ReadFrom(buf)
	if err = b.WriteAt(v3, n); err != nil {
		t.Errorf("b.Repair(3) error(%v)", err)
		t.FailNow()
	}
	n.Offset = v3
	if err = b.ReadAt(n); err != nil {
		t.Errorf("b.Get() error(%v)", err)
		t.FailNow()
	}
	if err = compareTestNeedle(t, 3, 3, needle.FlagOK, n, data); err != nil {
		t.Error("compareTestNeedle(3)")
		t.FailNow()
	}
	// test compress
	if err = b.Compact(0, func(rn *needle.Needle, so, eo uint32) (err1 error) {
		if rn.Flag != needle.FlagOK {
			return
		}
		needles[rn.Key] = needle.NewCache(so, rn.TotalSize)
		return
	}); err != nil {
		t.Errorf("b.Compress() error(%v)", err)
		t.FailNow()
	}
	// skip first needle, so key:1 must not exist
	if o, s := needle.Cache(needles[1]); o != 0 && s != 0 {
		t.Error("needle.Value(1) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[2]); o != v2 && s != 40 {
		t.Error("needle.Value(2) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[3]); o != v3 && s != 40 {
		t.Error("needle.Value(3) not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[4]); o != v4 && s != 40 {
		t.Error("needle.Value(4) not match")
		t.FailNow()
	}
}

func compareTestNeedle(t *testing.T, key int64, cookie int32, flag byte, n *needle.Needle, data []byte) (err error) {
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

func compareTestOffset(b *SuperBlock, n *needle.Needle, offset uint32) (err error) {
	var v int64
	if b.Offset != offset+needle.NeedleOffset(int64(n.TotalSize)) {
		err = fmt.Errorf("b.Offset: %d not match %d", b.Offset, offset)
		return
	}
	if v, err = b.w.Seek(0, os.SEEK_CUR); err != nil {
		err = fmt.Errorf("b.Seek() error(%v)", err)
		return
	} else {
		if v != needle.BlockOffset(b.Offset) {
			err = fmt.Errorf("offset: %d not match", v)
			return
		}
	}
	return
}
