package snowflake

import (
	"testing"
	"time"
	"fmt"
)


func TestSnowflake(t *testing.T) {
    var (
            err             error
            genkey          *Genkey
            i               int
            key             int64
    )
    if genkey, err = NewGenkey([]string{"localhost:2181"}, "/gosnowflake-servers", time.Second * 15, 0); err != nil {
        t.Errorf("NewGenkey failed error(%v)",err)
        t.FailNow()
    }
    time.Sleep(3 * time.Second)  // wait rpc
    for i = 0; i < 10000; i++ {
        if key, err = genkey.Getkey(); err != nil {
            t.Errorf("Getkey failed error(%v)", err)
            t.FailNow()
        }
        fmt.Println("key ",i,":", key)
    }
}