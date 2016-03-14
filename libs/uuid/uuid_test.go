package uuid

import (
	"testing"
)

func TestUUID(t *testing.T) {
	var (
		str string
		err error
	)
	if str, err = New(); err != nil {
		t.Errorf("New() error(%v)", err)
		t.FailNow()
	}
	t.Logf("uuid: %s", str)
}
