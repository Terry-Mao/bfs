package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"bfs/libs/gohbase"
	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/hrpc"
)

var (
	zkStr string
	table string
	key   string
)

func init() {
	flag.StringVar(&zkStr, "zk", "", ", joined zk hosts")
	flag.StringVar(&table, "table", "", "table name")
	flag.StringVar(&key, "key", "", "key to get")
}

func main() {
	flag.Parse()
	fmt.Printf("do test_get for zk(%s), table(%s), key:(%s)\n", zkStr, table, key)
	var (
		get    *hrpc.Get
		err    error
		res    *hrpc.Result
		st, et int64
	)
	zks := strings.Split(zkStr, ",")
	c := gohbase.NewClient(conf.NewConf(zks, "", "", "", 30*time.Second, 0, 0, 0))
	if c == nil {
		fmt.Printf("new client get nil client for zks: (%v)\n", zks)
		return
	}
	if get, err = hrpc.NewGetStr(context.Background(), table, key); err != nil {
		fmt.Printf("new get met error: (%v)\n", err)
		return
	}
	st = time.Now().UnixNano()
	if res, err = c.Get(get); err != nil {
		fmt.Printf("get met error: (%v)\n", err)
		return
	} else {
		for _, cell := range res.Cells {
			fmt.Printf("%s-%s-%s: %s;", string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
			fmt.Println("")
		}
	}
	et = time.Now().UnixNano()
	fmt.Printf("get (%s) from (%s) cost %d ns (%d ms)\n", table, key, et-st, (et-st)/1000000)

	for _, n := range []int{10, 1000} {
		st = time.Now().UnixNano()
		for i := 0; i < n; i++ {
			if get, err = hrpc.NewGetStr(context.Background(), table, key); err != nil {
				fmt.Printf("new get met error: (%v)\n", err)
				return
			}
			if res, err = c.Get(get); err != nil {
				fmt.Printf("get met error: (%v)\n", err)
				return
			}
		}
		et = time.Now().UnixNano()
		fmt.Printf("get (%s) from (%s) for %d times cost %d ns (%d ms)\n", table, key, n, et-st, (et-st)/1000000)
	}
}
