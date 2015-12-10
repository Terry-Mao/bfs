package hbase

import (
    "testing"
    "crypto/rand"
    mrand "math/rand"
    "github.com/Terry-Mao/bfs/directory/hbase/filemeta"
)

func BenchmarkHbasePut(b *testing.B) {
    var (
        err   error
        data  = make([]byte, 16351) // 16kb
        h     = NewHBaseClient()
        m     = &filemeta.File{}
        t     int64
    )
    if err = Init("172.16.13.90:9090", 5*time.Second, 10, 10); err != nil {
        t.Errorf("Init failed")
        t.FailNow()
    }
    if _, err = rand.Read(data); err != nil {
        b.Errorf("rand.Read() error(%v)", err)
        b.FailNow()
    }
    for i := 0; i<b.N; i++ {
        t = mrand.Int63()
        m.Key = t
        if err = h.Put(m); err != nil {
            b.Errorf("Put() error(%v)", err)
            b.FailNow()
        }
    }
}