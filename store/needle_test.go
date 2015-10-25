package main

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestNeedle(t *testing.T) {
	var (
		err    error
		offset uint32
		n      = &Needle{}
		data   = []byte("test")
		buf    = make([]byte, 40)
		bbuf   = bytes.NewBuffer(buf)
		bw     = bufio.NewWriter(bbuf)
	)
	t.Log("NeedleOffset()")
	if offset = NeedleOffset(32); offset != 4 {
		err = fmt.Errorf("NeedleOffset(32) not match")
		t.Error(err)
		goto failed
	}
	t.Log("Parse()")
	if err = n.Parse(1, 1, data); err != nil {
		t.Errorf("n.Parse() error(%v)", err)
		goto failed
	}
	t.Log("Fill()")
	n.Fill(buf)
	t.Log("ParseHeader()")
	if err = n.ParseHeader(buf[:NeedleHeaderSize]); err != nil {
		t.Errorf("n.ParseHeader() error(%v)", err)
		goto failed
	}
	t.Log("ParseData()")
	if err = n.ParseData(buf[NeedleHeaderSize:]); err != nil {
		t.Errorf("n.ParseData() error(%v)", err)
		goto failed
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != NeedleStatusOK || n.PaddingSize != 3 {
		err = fmt.Errorf("needle Parse() error")
		t.Error(err)
		goto failed
	}
	t.Log("Write()")
	if err = n.Write(bw); err != nil {
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
