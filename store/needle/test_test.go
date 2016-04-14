package needle

import (
	"bytes"
	"testing"
)

func compareNeedle(t *testing.T, n *Needle, key int64, cookie int32, data []byte, flag byte, checksum uint32) {
	if n.Key != key || n.Cookie != cookie || !bytes.Equal(n.Data, data) || n.Flag != flag || n.Checksum != checksum {
		t.Errorf("not match: %s, %d, %d, %d", n, key, cookie, checksum)
		t.FailNow()
	}
}
