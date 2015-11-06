package needle

import (
	"bufio"
	"bytes"
	"testing"
)

func TestAlign(t *testing.T) {
	var i, m int32
	i = 1
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 2
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 3
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 4
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 5
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 6
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 7
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
	i = 8
	m = (i-1)/PaddingSize + 1
	if align(i) != m*PaddingSize {
		t.Errorf("align: %d != %d", align(i), m*PaddingSize)
		t.FailNow()
	}
}

func TestNeedleOffset(t *testing.T) {
	var (
		offset  int64
		noffset uint32
	)
	offset = 32
	if noffset = NeedleOffset(offset); noffset != uint32(offset/int64(PaddingSize)) {
		t.Errorf("noffset: %d not match", noffset)
		t.FailNow()
	}
	offset = 48
	if noffset = NeedleOffset(offset); noffset != uint32(offset/int64(PaddingSize)) {
		t.Errorf("noffset: %d not match", noffset)
		t.FailNow()
	}
	offset = 8
	if noffset = NeedleOffset(offset); noffset != uint32(offset/int64(PaddingSize)) {
		t.Errorf("noffset: %d not match", noffset)
		t.FailNow()
	}
}

func TestBlockOffset(t *testing.T) {
	var (
		offset  int64
		noffset uint32
	)
	noffset = 1
	if offset = BlockOffset(noffset); offset != int64(noffset*PaddingSize) {
		t.Errorf("offset: %d not match", offset)
		t.FailNow()
	}
	noffset = 2
	if offset = BlockOffset(noffset); offset != int64(noffset*PaddingSize) {
		t.Errorf("offset: %d not match", offset)
		t.FailNow()
	}
	noffset = 4
	if offset = BlockOffset(noffset); offset != int64(noffset*PaddingSize) {
		t.Errorf("offset: %d not match", offset)
		t.FailNow()
	}
}

func TestNeedle(t *testing.T) {
	var (
		err  error
		n    = &Needle{}
		data = []byte("test")
		buf  = make([]byte, 40)
		bbuf = bytes.NewBuffer(buf)
		bw   = bufio.NewWriter(bbuf)
	)
	n.Parse(1, 1, data)
	n.Fill(buf)
	if err = n.ParseHeader(buf[:HeaderSize]); err != nil {
		t.Errorf("ParseHeader() error(%v)", err)
		t.FailNow()
	}
	if err = n.ParseData(buf[HeaderSize:]); err != nil {
		t.Errorf("ParseData() error(%v)", err)
		t.FailNow()
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.Error("Parse()")
		t.FailNow()
	}
	if err = n.Write(bw); err != nil {
		t.Errorf("Write() error(%v)", err)
		t.FailNow()
	}
	buf = bbuf.Bytes()
	if err = n.ParseHeader(buf[:HeaderSize]); err != nil {
		t.Errorf("ParseHeader() error(%v)", err)
		t.FailNow()
	}
	if err = n.ParseData(buf[HeaderSize:]); err != nil {
		t.Errorf("ParseData() error(%v)", err)
		t.FailNow()
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.Error("Parse() error")
		t.FailNow()
	}
}
