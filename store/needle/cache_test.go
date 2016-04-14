package needle

import (
	"testing"
)

func TestCache(t *testing.T) {
	var nc = NewCache(134, 1064)
	if offset, size := Cache(nc); offset != 134 || size != 1064 {
		t.FailNow()
	}
}
