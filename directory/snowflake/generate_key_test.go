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
        fmt.Println("NewGenkey failed ",err)
        return
    }
    time.Sleep(3 * time.Second)  // wait rpc
    for i = 0; i < 10000; i++ {
        if key, err = genkey.Getkey(); err != nil {
            fmt.Println("Getkey failed", err)
            return
        }
        fmt.Println("key ",i,":", key)
    }
}