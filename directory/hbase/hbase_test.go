package hbase

import (
	"testing"
	"time"
	"fmt"
)


func TestHbase(t *testing.T) {
    var (
            err             error
    )
    if err = Init("172.16.13.90:9090", 5*time.Second, 10, 10); err != nil {
            fmt.Println("Init failed")
            return
    }

    h := NewHBaseClient()
    m := &meta.Meta{}
    m.Key = 445
    m.Vid = 55
    m.Cookie=5
    n := &meta.Meta{}
    if err = h.Put(m); err != nil {
            fmt.Println("error: %v", err)
    }
    if n, err = h.Get(m.Key); err != nil {
            fmt.Println("error: %v", err)
    }
    fmt.Println("Get success", n)
    if err = h.Put(m); err != nil {
            fmt.Println("error: %v", err)
    }
    if err = h.Del(m.Key); err != nil {
            fmt.Println("error:%v", err)
    }
    if n, err = h.Get(m.Key); err != nil {
            fmt.Println("error: %v", err)
    }
    fmt.Println("Get success", n)
}