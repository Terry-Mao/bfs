package needle

import (
	"bytes"
	"testing"
)

func TestNeedles(t *testing.T) {
	var (
		err error
		n   *Needle
		ns  = &Needles{
			Items:  make([]Needle, 2),
			Buffer: make([]byte, 80),
		}
		data = []byte("test")
	)
	n = &ns.Items[0]
	n.Init(1, 1, data)
	if err = ns.Write(n); err != nil {
		t.FailNow()
	}
	n = &ns.Items[1]
	n.Init(2, 2, data)
	if err = ns.Write(n); err != nil {
		t.FailNow()
	}
	n.Buffer = ns.Buffer[:40]
	n.Parse()
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.FailNow()
	}
	n.Buffer = ns.Buffer[40:]
	n.Parse()
	if n.Cookie != 2 || n.Key != 2 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.Error("Parse() error")
		t.FailNow()
	}
}

func TestNeedle(t *testing.T) {
	var (
		err  error
		n    = &Needle{}
		data = []byte("test")
		buf  = make([]byte, 40)
	)
	n.Buffer = buf
	n.Init(1, 1, data)
	if err = n.Write(); err != nil {
		t.FailNow()
	}
	if err = n.Parse(); err != nil {
		t.FailNow()
	}
	if n.Cookie != 1 || n.Key != 1 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.FailNow()
	}
	n.Init(2, 2, data)
	if err = n.Write(); err != nil {
		t.FailNow()
	}
	if err = n.Parse(); err != nil {
		t.FailNow()
	}
	if n.Cookie != 2 || n.Key != 2 || n.Size != 4 || !bytes.Equal(n.Data, data) || n.Flag != FlagOK || n.PaddingSize != 7 {
		t.Error("Parse() error")
		t.FailNow()
	}
}

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

func TestSize(t *testing.T) {
	if Size(4) != 40 {
		t.FailNow()
	}
}
