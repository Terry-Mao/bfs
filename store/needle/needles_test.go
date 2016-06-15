package needle

import (
	"bfs/libs/errors"
	"bytes"
	"hash/crc32"
	"testing"
)

func TestNeedles(t *testing.T) {
	var (
		err       error
		tn        *Needle
		data1     = []byte("tes1")
		checksum1 = crc32.Update(0, _crc32Table, data1)
		data2     = []byte("tes2")
		checksum2 = crc32.Update(0, _crc32Table, data2)
		ns        = NewNeedles(2)
		buf       = &bytes.Buffer{}
	)
	if _, err = buf.Write(data1); err != nil {
		t.FailNow()
	}
	if err = ns.ReadFrom(1, 1, 4, buf); err != nil {
		t.FailNow()
	}
	if _, err = buf.Write(data2); err != nil {
		t.FailNow()
	}
	if err = ns.ReadFrom(2, 2, 4, buf); err != nil {
		t.FailNow()
	}
	tn = new(Needle)
	tn.buffer = ns.Next().Buffer()
	if err = tn.Parse(); err != nil {
		t.FailNow()
	}
	t.Log(tn)
	compareNeedle(t, tn, 1, 1, data1, FlagOK, checksum1)
	tn = new(Needle)
	tn.buffer = ns.Next().Buffer()
	if err = tn.Parse(); err != nil {
		t.FailNow()
	}
	t.Log(tn)
	compareNeedle(t, tn, 2, 2, data2, FlagOK, checksum2)
	if err = ns.ReadFrom(3, 3, 4, buf); err != errors.ErrNeedleFull {
		t.FailNow()
	}
	if tn = ns.Next(); tn != nil {
		t.FailNow()
	}
}
