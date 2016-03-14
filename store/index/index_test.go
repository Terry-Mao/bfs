package index

import (
	"bfs/libs/errors"
	"bfs/store/conf"
	"bfs/store/needle"
	"os"
	"testing"
	"time"
)

var (
	testConf = &conf.Config{
		NeedleMaxSize: 4 * 1024 * 1024,
		BlockMaxSize:  needle.Size(4 * 1024 * 1024),
		Index: &conf.Index{
			BufferSize:    4 * 1024 * 1024,
			MergeDelay:    conf.Duration{10 * time.Second},
			MergeWrite:    5,
			RingBuffer:    10,
			SyncWrite:     10,
			Syncfilerange: true,
		},
	}
)

func TestIndex(t *testing.T) {
	var (
		i       *Indexer
		err     error
		noffset uint32
		file    = "../test/test.idx"
		needles = make(map[int64]int64)
	)
	os.Remove(file)
	defer os.Remove(file)
	if i, err = NewIndexer(file, testConf); err != nil {
		t.Errorf("NewIndexer() error(%v)", err)
		t.FailNow()
	}
	i.Close()
	// test closed
	if err = i.Add(1, 1, 8); err != errors.ErrIndexClosed {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	// test open
	if err = i.Open(); err != nil {
		t.Errorf("Open() error(%v)", err)
		t.FailNow()
	}
	defer i.Close()
	// test add
	if err = i.Add(1, 1, 8); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if err = i.Add(2, 2, 8); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if err = i.Add(5, 3, 8); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	if err = i.Add(6, 4, 8); err != nil {
		t.Errorf("Add() error(%v)", err)
		t.FailNow()
	}
	i.Signal()
	time.Sleep(1 * time.Second)
	i.Flush()
	// test recovery
	if err = i.Recovery(func(ix *Index) error {
		needles[ix.Key] = needle.NewCache(ix.Offset, ix.Size)
		noffset = ix.Offset + needle.NeedleOffset(int64(ix.Size))
		return nil
	}); err != nil {
		t.Errorf("Recovery() error(%v)", err)
		t.FailNow()
	}
	// add 4 index, start with 5
	if noffset != 5 {
		t.Errorf("noffset: %d not match", noffset)
		t.FailNow()
	}
	if o, s := needle.Cache(needles[1]); o != 1 && s != 8 {
		t.Error("needle cache not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[2]); o != 2 && s != 8 {
		t.Error("needle cache not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[5]); o != 3 && s != 8 {
		t.Error("needle cache not match")
		t.FailNow()
	}
	if o, s := needle.Cache(needles[6]); o != 4 && s != 8 {
		t.Error("needle cache not match")
		t.FailNow()
	}
	// test write
	if err = i.Write(10, 5, 8); err != nil {
		t.Error("Write() error(%v)", err)
		t.FailNow()
	}
	if err = i.Flush(); err != nil {
		t.Error("Flush() error(%v)", err)
		t.FailNow()
	}
	// test recovery
	noffset = 0
	if err = i.Recovery(func(ix *Index) error {
		needles[ix.Key] = needle.NewCache(ix.Offset, ix.Size)
		noffset = ix.Offset + needle.NeedleOffset(int64(ix.Size))
		return nil
	}); err != nil {
		t.Errorf("Recovery() error(%v)", err)
		t.FailNow()
	}
	// add 5 index, start with 6
	if noffset != 6 {
		t.Errorf("noffset: %d not match", noffset)
		t.FailNow()
	}
	if o, s := needle.Cache(needles[10]); o != 5 && s != 8 {
		t.Error("needle.Value(1) not match")
		t.FailNow()
	}
}
