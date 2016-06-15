package hbase

import (
	"bfs/libs/meta"
	"math/rand"
	"testing"
	"time"
)

func BenchmarkHbasePut(b *testing.B) {
	var (
		err error
		h   = NewHBaseClient()
		m   = &meta.Needle{}
		t   int64
	)
	ch := make(chan int64, 1000000)
	if err = Init("172.16.13.90:9090", 5*time.Second, 200, 200); err != nil {
		b.Errorf("Init failed")
		b.FailNow()
	}
	for j := 0; j < 1000000; j++ {
		k := int64(time.Now().UnixNano())
		ch <- k
	}
	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t = <-ch
			m.Key = t
			if err = h.Put(m); err != nil {
				continue
			}
		}
	})
}

func BenchmarkHbaseGet(b *testing.B) {
	var (
		err error
		h   = NewHBaseClient()
		t   int64
		r   *rand.Rand
	)
	if err = Init("172.16.13.90:9090", 5*time.Second, 200, 200); err != nil {
		b.Errorf("Init failed")
		b.FailNow()
	}
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
	b.ResetTimer()
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t = r.Int63n(1000000)
			if _, err = h.Get(t); err != nil {
				b.Errorf("Put() error(%v)", err)
				b.FailNow()
			}
		}
	})
}
