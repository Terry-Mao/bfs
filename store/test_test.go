package main

import (
	"bfs/store/conf"
	"bfs/store/needle"
	"time"
)

var (
	testConf = &conf.Config{
		Pprof:         false,
		AdminListen:   "localhost:6063",
		ApiListen:     "localhost:6064",
		StatListen:    "localhost:6065",
		NeedleMaxSize: 4 * 1024 * 1024,
		BlockMaxSize:  needle.Size(4 * 1024 * 1024),
		BatchMaxNum:   16,
		Zookeeper: &conf.Zookeeper{
			Root:     "/rack",
			Rack:     "rack-a",
			ServerId: "store-a",
			Addrs:    []string{"localhost:2181"},
			Timeout:  conf.Duration{time.Second},
		},
		Store: &conf.Store{
			VolumeIndex:     "./test/volume.idx",
			FreeVolumeIndex: "./test/free_volume.idx",
		},
		Volume: &conf.Volume{
			SyncDelete:      10,
			SyncDeleteDelay: conf.Duration{10 * time.Second},
		},
		Block: &conf.Block{
			BufferSize:    4 * 1024 * 1024,
			SyncWrite:     1024,
			Syncfilerange: true,
		},
		Index: &conf.Index{
			BufferSize:    4 * 1024 * 1024,
			MergeDelay:    conf.Duration{10 * time.Second},
			MergeWrite:    5,
			RingBuffer:    10,
			SyncWrite:     10,
			Syncfilerange: true,
		},
		Limit: &conf.Limit{
			Read: &conf.Rate{
				Rate:  150.0,
				Brust: 200,
			},
			Write: &conf.Rate{
				Rate:  150.0,
				Brust: 200,
			},
			Delete: &conf.Rate{
				Rate:  150.0,
				Brust: 200,
			},
		},
	}
)

type testRet struct {
	Ret int `json:"ret"`
}
