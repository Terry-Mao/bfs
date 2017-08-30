package gohbase

import (
	"context"
	"testing"
	"time"

	"bfs/libs/gohbase/conf"
	"bfs/libs/gohbase/hrpc"

	log "github.com/golang/glog"
)

func getStopRow(s []byte) []byte {
	res := make([]byte, len(s)+20)
	copy(res, s)
	return res
}

func TestMGet1(t *testing.T) {
	c := newClient(standardClient, conf.NewConf([]string{"172.16.33.45:2181"}, "", "", "", 30*time.Second, 0, 0, 0))
	keys := []string{
		"row_001", "row_003", "row_007",
	}
	gets := make([]*hrpc.Get, len(keys))
	for i, key := range keys {
		get, err := hrpc.NewGetStr(context.Background(), "fuckclient", key)
		if err != nil {
			log.Error("NewGetStr error for key %s, err is %v", key, err)
			continue
		}
		gets[i] = get
	}
	//time.Sleep(15 * time.Second)
	for _, get := range gets {
		st := time.Now()
		res, err := c.Get(get)
		if err != nil {
			log.Error("get meet error, err is %v", err)
			continue
		}
		et := time.Now()
		for _, cell := range res.Cells {
			log.Info("%s-%s-%s-%s, st: %v, et: %v, cost: %d", string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value), st, et, (et.Nanosecond()-st.Nanosecond())/1000000)
		}
		time.Sleep(3 * time.Second)
	}

	c.clearAllRegions()
	log.Info("%v: clearAllRegions", time.Now())
	time.Sleep(3 * time.Second)
	//time.Sleep(30 * time.Second)
	log.Info("%v: do second scan", time.Now())

	gets = make([]*hrpc.Get, len(keys))
	for i, key := range keys {
		get, err := hrpc.NewGetStr(context.Background(), "fuckclient", key)
		if err != nil {
			log.Error("NewGetStr error for key %s, err is %v", key, err)
			continue
		}
		gets[i] = get
	}
	for _, get := range gets {
		st := time.Now()
		res, err := c.Get(get)
		if err != nil {
			log.Error("get meet error, err is %v", err)
			continue
		}
		et := time.Now()
		for _, cell := range res.Cells {
			log.Info("%s-%s-%s-%s, st: %v, et: %v, cost: %d", string(cell.Row), string(cell.Family), string(cell.Qualifier), string(cell.Value), st, et, (et.Nanosecond()-st.Nanosecond())/1000000)
		}
	}
	time.Sleep(1 * time.Second)
}
