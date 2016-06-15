package needle

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"testing"
)

func TestNeedle(t *testing.T) {
	var (
		err       error
		n, tn     *Needle
		br        *bufio.Reader
		data1     = []byte("tes1")
		checksum1 = crc32.Update(0, _crc32Table, data1)
		data2     = []byte("tes2")
		checksum2 = crc32.Update(0, _crc32Table, data2)
		buf       = &bytes.Buffer{}
	)
	// WriteFrom
	if _, err = buf.Write(data1); err != nil {
		t.Error(err)
		t.FailNow()
	}
	n = NewWriter(3, 3, 4)
	defer n.Close()
	if err = n.ReadFrom(buf); err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Log(n)
	tn = new(Needle)
	tn.buffer = n.Buffer()
	// Parse
	if err = tn.Parse(); err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Log(n)
	compareNeedle(t, tn, 3, 3, data1, FlagOK, checksum1)
	buf.Write(data2)
	n = NewWriter(4, 4, 4)
	defer n.Close()
	if err = n.ReadFrom(buf); err != nil {
		t.Error(err)
		t.FailNow()
	}
	tn = new(Needle)
	tn.buffer = n.Buffer()
	if err = tn.Parse(); err != nil {
		t.Error(err)
		t.FailNow()
	}
	compareNeedle(t, tn, 4, 4, data2, FlagOK, checksum2)
	// ParseFrom
	if _, err = buf.Write(n.Buffer()); err != nil {
		t.Error(err)
		t.FailNow()
	}
	br = bufio.NewReader(buf)
	tn = new(Needle)
	if err = tn.ParseFrom(br); err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Log(tn)
	compareNeedle(t, tn, 4, 4, data2, FlagOK, checksum2)
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
