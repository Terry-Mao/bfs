package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bfs/libs/gohbase"
	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/hrpc"
)

var (
	zkStr     string = "172.18.4.117:2181,172.18.4.118:2181,172.18.4.119:2181"
	testTable string = "test"
	spanTable string = "dapper_origin_v1"
	testKey   string = "test"
	spanKey   string = "13794899398466741090"
	tables           = []string{testTable, spanTable}
	keys             = []string{testKey, spanKey}
)

func main() {
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
	var counter = 0
	for {
		time.Sleep(1 * time.Second)
		var i int = 0
		for ; i < 2; i++ {
			var key = keys[i]
			var table = tables[i]
			if get, err = hrpc.NewGetStr(context.Background(), table, key); err != nil {
				fmt.Printf("new get met error: (%v)\n", err)
				continue
			}
			st = time.Now().UnixNano()
			if res, err = c.Get(get); err != nil {
				fmt.Printf("get met error: (%v)\n", err)
				continue
			} else {
				for _, cell := range res.Cells {
					fmt.Sprintf("%s-%s-%s: %s;", string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
				}
			}
			et = time.Now().UnixNano()
			if (et-st)/1000000000 > 1 {
				fmt.Printf("get (%s) from (%s) cost %d ns (%d ms)\n", table, key, et-st, (et-st)/1000000)
			}
		}
		counter += 1
		if counter > 30 {
			fmt.Printf("time: (%v), done (%d) loops\n", time.Now(), counter)
			counter = 0
		}
	}
}
