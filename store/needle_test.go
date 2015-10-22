package main

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestNeedle(t *testing.T) {
	var (
		err     error
		padding int32
		offset  uint32
		size    int32
		nc      NeedleCache
		n       = &Needle{}
		data    = []byte("test")
		buf     = make([]byte, 40)
		bbuf    = bytes.NewBuffer(buf)
		bw      = bufio.NewWriter(bbuf)
	)
	offset = 134
	size = 1064
	t.Log("NewNeedleCache")
	nc = NewNeedleCache(offset, size)
	if offset, size = nc.Value(); offset != 134 || size != 1064 {
		err = fmt.Errorf("needlecache.Value() not match")
		t.Error(err)
		goto failed
	}
	t.Log("NeedleOffset(32)")
	if offset = NeedleOffset(32); offset != 4 {
		err = fmt.Errorf("NeedleOffset(32) not match")
		t.Error(err)
		goto failed
	}
	t.Log("NeedleSize(4)")
	if padding, size, err = NeedleSize(4); padding != 3 || size != 40 || err != nil {
		err = fmt.Errorf("NeedleSize(4) not match")
		t.Error(err)
		goto failed
	}
	t.Log("FillNeedle")
	FillNeedle(padding, int32(len(data)), 1, 1, data, buf)
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		t.Errorf("n.ParseHeader() error(%v)", err)
		goto failed
	}
	if err = n.ParseData(buf[NeedleHeaderSize:]); err != nil {
		t.Errorf("n.ParseData() error(%v)", err)
		goto failed
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != NeedleStatusOK || n.PaddingSize != 3 {
		err = fmt.Errorf("needle Parse() error")
		t.Error(err)
		goto failed
	}
	if err = WriteNeedle(bw, padding, size, 1, 1, data); err != nil {
		t.Errorf("WriteNeedle() error(%v)", err)
		goto failed
	}
	buf = bbuf.Bytes()
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		t.Errorf("n.ParseHeader() error(%v)", err)
		goto failed
	}
	if err = n.ParseData(buf[NeedleHeaderSize:]); err != nil {
		t.Errorf("n.ParseData() error(%v)", err)
		goto failed
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != NeedleStatusOK || n.PaddingSize != 3 {
		err = fmt.Errorf("needle Parse() error")
		t.Error(err)
		goto failed
	}
failed:
	if err != nil {
		t.FailNow()
	}
}
