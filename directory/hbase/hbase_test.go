package hbase

import (
	"bfs/libs/meta"
	"fmt"
	"testing"
	"time"
)

func TestHbase(t *testing.T) {
	var (
		err  error
		m, n *meta.Needle
	)
	if err = Init("172.16.13.90:9090", 5*time.Second, 10, 10); err != nil {
		t.Errorf("Init failed")
		t.FailNow()
	}

	h := NewHBaseClient()
	m = new(meta.Needle)
	m.Key = 445
	m.Vid = 55
	m.Cookie = 5
	n = new(meta.Needle)
	if err = h.Put(m); err != nil {
		t.Errorf("error: %v", err)
		t.FailNow()
	}
	if n, err = h.Get(m.Key); err != nil {
		t.Errorf("error: %v", err)
		t.FailNow()
	}
	fmt.Println("Get success", n)
	if err = h.Del(m.Key); err != nil {
		t.Errorf("error:%v", err)
		t.FailNow()
	}
	if n, err = h.Get(m.Key); err != nil {
		t.Errorf("error: %v", err)
		t.FailNow()
	}
	fmt.Println("Get success", n)
}
