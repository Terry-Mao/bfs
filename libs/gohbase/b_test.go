package gohbase_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"bfs/libs/gohbase"
	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/filter"
	"bfs/libs/gohbase/hrpc"

	log "github.com/golang/glog"
)

func getStopRow(s []byte) []byte {
	res := make([]byte, len(s)+20)
	copy(res, s)
	return res
}

func TestScan4Split(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewCo
	// nf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	scan, err := hrpc.NewScan(context.Background(), []byte("fuckclient"))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	//scan.SetLimit(1)
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Logf("Scan returned an error: %v", err)
	}
	for i, rspOne := range rsp {
		if i%10000 == 0 {
			for _, cell := range rspOne.Cells {
				t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
			}
		}
	}

	time.Sleep(5 * time.Second)

	scan, err = hrpc.NewScan(context.Background(), []byte("fuckclient"))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	//scan.SetLimit(1)
	rsp, err = c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	for i, rspOne := range rsp {
		if i%10000 == 0 {
			for _, cell := range rspOne.Cells {
				t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
			}
		}
	}
}

func TestScanPrefix(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewConf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	scan, err := hrpc.NewScan(context.Background(), []byte("fuckclient"))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	ft := filter.NewPrefixFilter([]byte("row_0"))
	scan.SetFilter(ft)
	//scan.SetLimit(1)
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	for _, rspOne := range rsp {
		for _, cell := range rspOne.Cells {
			t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
		}
	}
}

func TestDel(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewConf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	var (
		err error
		put *hrpc.Mutate
		del *hrpc.Mutate
		get *hrpc.Get
		rsp *hrpc.Result
	)

	if put, err = hrpc.NewPutStr(context.Background(), "fuckclient", "haha", map[string]map[string][]byte{
		"v": map[string][]byte{
			"c1": []byte("v1"),
			"c2": []byte("v2"),
			"c3": []byte("v3"),
		},
	}); err != nil {
		t.Fatalf("Failed to create put request: %s", err)
	}
	if rsp, err = c.Put(put); err != nil {
		t.Errorf("put returned an error: %v", err)
	}

	if get, err = hrpc.NewGetStr(context.Background(), "fuckclient", "haha"); err != nil {
		t.Fatalf("Failed to create get request: %s", err)
	}
	if rsp, err = c.Get(get); err != nil {
		t.Errorf("get returned an error: %v", err)
	} else {
		resMap := map[string]string{}
		for _, cell := range rsp.Cells {
			resMap[fmt.Sprintf("%s:%s", string(cell.Family), string(cell.Qualifier))] = string(cell.Value)
		}
		if resMap["v:c1"] != "v1" || resMap["v:c2"] != "v2" || resMap["v:c3"] != "v3" {
			t.Errorf("get does not return just-put-item")
		}
	}

	if del, err = hrpc.NewDelStr(context.Background(), "fuckclient", "haha", map[string]map[string][]byte{
		"v": map[string][]byte{
			"c1": nil,
		},
	}); err != nil {
		t.Fatalf("Failed to create Del request: %s", err)
	}
	if rsp, err = c.Delete(del); err != nil {
		t.Errorf("del returned an error: %v", err)
	}
	if get, err = hrpc.NewGetStr(context.Background(), "fuckclient", "haha"); err != nil {
		t.Fatalf("Failed to create get request: %s", err)
	}
	if rsp, err = c.Get(get); err != nil {
		t.Errorf("get returned an error: %v", err)
	} else {
		resMap := map[string]string{}
		for _, cell := range rsp.Cells {
			if string(cell.Family) == "v" {
				resMap[string(cell.Qualifier)] = string(cell.Value)
			}
		}
		if resMap["c1"] != "" || resMap["c2"] == "" || resMap["c3"] == "" {
			t.Errorf("get returned uncorrect value just after del v:c1: %v", resMap)
		}
	}

	if del, err = hrpc.NewDelStr(context.Background(), "fuckclient", "haha", map[string]map[string][]byte{
		"v": nil,
	}); err != nil {
		t.Fatalf("Failed to create Del request: %s", err)
	}
	if rsp, err = c.Delete(del); err != nil {
		t.Errorf("del returned an error: %v", err)
	}
	if get, err = hrpc.NewGetStr(context.Background(), "fuckclient", "haha"); err != nil {
		t.Fatalf("Failed to create get request: %s", err)
	}
	if rsp, err = c.Get(get); err != nil {
		t.Errorf("get returned an error: %v", err)
	} else {
		resMap := map[string]string{}
		for _, cell := range rsp.Cells {
			if string(cell.Family) == "v" {
				resMap[string(cell.Qualifier)] = string(cell.Value)
			}
		}
		if len(resMap) != 0 {
			t.Errorf("get returned cf value just after del cf: %v", resMap)
		}
	}
}

func TestGetTimeRange(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewConf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	get, err := hrpc.NewGetStr(context.Background(), "test_tbl1", "row1")
	if err != nil {
		t.Fatalf("Failed to create get request: %s", err)
	}
	get.SetTimeRange(hrpc.TimeRange{1469179951934, 1469680333054}) // [). only return latest version which are in this range
	rsp, err := c.Get(get)
	if err != nil {
		t.Errorf("get returned an error: %v", err)
	}
	for _, cell := range rsp.Cells {
		t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
	}
}

func TestScanTimeRange(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewConf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	scan, err := hrpc.NewScanRangeStr(context.Background(), "test_tbl1", "row1", "")
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	ft := filter.NewPrefixFilter([]byte("row1"))
	scan.SetFilter(ft)
	scan.SetTimeRange(hrpc.TimeRange{1469179951934, 1469680333054}) // [). only return latest version which are in this range
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	for _, rspOne := range rsp {
		for _, cell := range rspOne.Cells {
			t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
		}
	}
}

func TestScanPrefix1(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	//c := gohbase.NewClient(conf.NewConf([]string{"172.16.13.94:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	scan, err := hrpc.NewScanRangeStr(context.Background(), "fuckclient", "14771787", "")
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	ft := filter.NewPrefixFilter([]byte("14771787"))
	scan.SetFilter(ft)
	rsp, err := c.Scan(scan)
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	for _, rspOne := range rsp {
		for _, cell := range rspOne.Cells {
			t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
		}
	}
}

func TestMGet(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	scan, err := hrpc.NewScan(context.Background(), []byte("fuckclient"))
	if err != nil {
		t.Fatalf("Failed to create Scan request: %s", err)
	}
	ft := filter.NewMultiRowRangeFilter([]*filter.RowRange{
		filter.NewRowRange([]byte("row_001"), getStopRow([]byte("row_001")), true, true),
		filter.NewRowRange([]byte("row_003"), getStopRow([]byte("row_003")), true, true),
		filter.NewRowRange([]byte("row_007"), getStopRow([]byte("row_007")), true, true),
	})
	scan.SetFilter(ft)
	rsp, err := c.Scan(scan)
	t.Log("len of rsps is %d", len(rsp))
	if err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	for _, rspOne := range rsp {
		for _, cell := range rspOne.Cells {
			t.Log(string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
		}
	}
	/*
			b_test.go:30: len of rsps is %d 3
			b_test.go:36: row_001 v c1 v1
			b_test.go:36: row_003 v c1 v1
			b_test.go:36: row_007 v c1 v1
		PASS
		ok  	golang/gohbase	0.138s
	*/
}

func TestConGet(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	for i := 0; i < 10; i++ {
		var end = 100
		ids := make([]int, end)
		for i := 0; i < end; i++ {
			ids[i] = i + 1
		}
		gets := make([]hrpc.Call, len(ids))
		for i, id := range ids {
			rowKey := fmt.Sprintf("row_%03d", id)
			get, err := hrpc.NewGetStr(context.Background(), "fuckclient", rowKey)
			if err != nil {
				t.Fatalf("Failed to create get request: %s", err)
			}
			gets[i] = get
		}
		ctx, _ := context.WithTimeout(context.Background(), 3000*time.Millisecond)
		st := time.Now()
		ress := c.Go(&hrpc.Calls{
			Calls: gets,
			Ctx:   ctx,
		})
		var cnt int
		for _, res := range ress {
			if res.Err != nil {
				t.Errorf("meet error %v", res.Err)
			}
			rRes := res.Result
			cnt += 1
			for _, cell := range rRes.Cells {
				_ = cell
				//t.Log(time.Now(), string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
			}
		}
		et := time.Now()
		t.Log(fmt.Sprintf("start time: %v, end time: %v, ok %d, cost: %d", st, et, cnt, et.UnixNano()-st.UnixNano()))
	}
}

func TestConGet1(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	ids := []int64{121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248}
	type aa struct {
		ids []int64
		c   int
	}
	aas := []aa{
		aa{
			ids,
			0,
		},
		aa{
			ids[:100],
			1,
		},
		aa{
			ids[:77],
			2,
		},
		aa{
			ids,
			len(ids[:1]) / 2,
		},
		aa{
			ids[:2],
			(len(ids) - 1) / 2,
		},
		aa{
			ids,
			(len(ids) + 1) / 2,
		},
	}
	for _, aai := range aas {
		ids := aai.ids
		gets := make([]hrpc.Call, len(ids))
		for i, id := range ids {
			rowKeyBS := make([]byte, 8)
			binary.LittleEndian.PutUint64(rowKeyBS, uint64(id))
			get, err := hrpc.NewGet(context.Background(), []byte("fuckclient"), rowKeyBS)
			if err != nil {
				t.Fatalf("Failed to create get request: %s", err)
			}
			gets[i] = get
		}
		ctx, _ := context.WithTimeout(context.Background(), 3000*time.Millisecond)
		st := time.Now()
		ress := c.Go(&hrpc.Calls{
			Calls: gets,
			Ctx:   ctx,
		})
		var cnt int
		for _, res := range ress {
			if res.Err != nil {
				t.Errorf("meet error %v", res.Err)
			}
			rRes := res.Result
			cnt += 1
			for _, cell := range rRes.Cells {
				_ = cell
				//t.Log(time.Now(), string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value))
			}
		}
		et := time.Now()
		t.Log(fmt.Sprintf("start time: %v, end time: %v, c %d, total %d, ok %d, cost: %d", st, et, aai.c, len(ids), cnt, et.UnixNano()-st.UnixNano()))
	}
}

func TestBenchmark(t *testing.T) {
	c := gohbase.NewClient(conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	keys := []string{
		"row_001", "row_003", "row_007",
	}
	concurrency := 300
	per := 300
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)
	allMax := make([]int64, concurrency)
	allAvg := make([]int64, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			gets := make([]*hrpc.Get, per*len(keys))
			for j := 0; j < per; j++ {
				for k, key := range keys {
					get, err := hrpc.NewGetStr(context.Background(), "fuckclient", key)
					if err != nil {
						log.Error("NewGetStr error for key %s, err is %v", key, err)
						continue
					}
					gets[j*len(keys)+k] = get
				}
			}
			totalTime := int64(0)
			maxTime := int64(0)

			//time.Sleep(15 * time.Second)
			for _, get := range gets {
				st := time.Now()
				_, err := c.Get(get)
				if err != nil {
					log.Error("get meet error, err is %v", err)
					continue
				}
				et := time.Now()
				cost := (et.UnixNano() - st.UnixNano()) / int64(time.Millisecond)
				if cost > maxTime {
					maxTime = int64(cost)
				}
				totalTime += int64(cost)
				//for _, cell := range res.Cells {
				//	log.Info("worker%2d value: %s-%s-%s-%s, st: %v, et: %v, cost: %d",
				//		i, string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value), st, et, (et.Nanosecond() - st.Nanosecond()) / 1000000)
				//}
			}
			avg := totalTime / int64(len(gets))
			log.Info("%v worker%2d, count %d, total %d, max %d, avg %d", time.Now(), i, len(gets), totalTime, maxTime, avg)
			allMax[i] = maxTime
			allAvg[i] = avg
			wg.Done()
		}(i)
	}
	wg.Wait()
	var allAvgSum, maxAllMax, avgAllAvg int64
	for _, m := range allAvg {
		allAvgSum += m
	}
	avgAllAvg = allAvgSum / int64(concurrency)
	for _, m := range allMax {
		if m > maxAllMax {
			maxAllMax = m
		}
	}
	log.Info("max of allMax is %d, avg of allAvg is %d", maxAllMax, avgAllAvg)
	time.Sleep(200 * time.Millisecond)
}
