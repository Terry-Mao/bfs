package main

import (
	"github.com/Terry-Mao/goconf"
	"time"
)

const (
	// store
	configStoreVolumeCache = 32
	configVolumeIndex      = "./volume.idx"
	configFreeVolumeIndex  = "./free_volume.idx"
	configNeedleMaxSize    = 30 * 1024 * 1024 // 30mb
	configBatchMaxNum      = 30
	// volume
	configVolumeDelChan       = 1024 * 10
	configVolumeSigCnt        = 1024 * 10
	configVolumeSigTime       = time.Second * 60 // 1min
	configVolumeCheckSize     = 32
	configVolumeCheckInterval = 10000
	// block
	configSuperBlockSync = 1024
	// index
	configIndexBufferio   = 4 * 1024
	configIndexRingBuffer = 1024 * 10
	configIndexMerge      = 1024
	configIndexMergeTime  = time.Second * 10 // 10s
	configIndexSync       = 1024
	// pprof
	configPprofListen = "localhost:6060"
	// stat
	configStatListen = "localhost:6061"
	// api
	configApiListen = "localhost:6062"
	// admin
	configAdminListen = "localhost:6063"
	// zookeeper
	configZookeeperTimeout = time.Second * 1 // 1s
	configZookeeperRoot    = "/rack"
)

var (
	configZookeeperAddrs = []string{"localhost:2181"}
)

type Config struct {
	// debug
	DebugVolume bool `goconf:"debug:volume"`
	// store
	StoreVolumeCache int    `goconf:"store:volume_cache_size"`
	ServerId         string `goconf:"store:server_id"`
	Rack             string `goconf:"store:rack"`
	VolumeIndex      string `goconf:"store:volume_index"`
	FreeVolumeIndex  string `goconf:"store:free_volume_index"`
	NeedleMaxSize    int    `goconf:"store:needle_max_size:memory"`
	BatchMaxNum      int    `goconf:"store:batch_max_num"`
	// volume
	VolumeDelChan       int           `goconf:"volume:delete_channel_size"`
	VolumeSigCnt        int           `goconf:"volume:delete_signal_count"`
	VolumeNeedleCache   int           `goconf:"volume:needle_cache_size"`
	VolumeSigTime       time.Duration `goconf:"volume:delete_signal_time:time"`
	VolumeCheckSize     int           `goconf:"volume:check_size"`
	VolumeCheckInterval int           `goconf:"volume:check_interval"`
	// block
	SuperBlockSync          int  `goconf:"block:Sync"`
	SuperBlockSyncfilerange bool `goconf:"block:sync_file_range"`
	// index
	IndexRingBuffer    int           `goconf:"index:ring_buffer_size"`
	IndexBufferio      int           `goconf:"index:buffer_io_size:memory"`
	IndexMerge         int           `goconf:"index:merge"`
	IndexMergeTime     time.Duration `goconf:"index:merge_time:time"`
	IndexSync          int           `goconf:"index:sync"`
	IndexSyncfilerange bool          `goconf:"index:sync_file_range"`
	// pprof
	PprofEnable bool   `goconf:"pprof:enable"`
	PprofListen string `goconf:"pprof:listen"`
	// stat
	StatListen string `goconf:"stat:listen"`
	// api
	ApiListen string `goconf:"api:listen"`
	// admin
	AdminListen string `goconf:"admin:listen"`
	// zookeeper
	ZookeeperAddrs   []string      `goconf:"zookeeper:addrs:,"`
	ZookeeperTimeout time.Duration `goconf:"zookeeper:timeout"`
	ZookeeperRoot    string        `goconf:"zookeeper:root"`
}

// NewConfig new a config.
func NewConfig(file string) (c *Config, err error) {
	var gconf = goconf.New()
	c = &Config{}
	if err = gconf.Parse(file); err != nil {
		return
	}
	if err = gconf.Unmarshal(c); err != nil {
		return
	}
	c.setDefault()
	return
}

// setDefault set the config default value.
func (c *Config) setDefault() {
	// store
	if c.StoreVolumeCache < 1 {
		c.StoreVolumeCache = configStoreVolumeCache
	}
	if len(c.ServerId) == 0 {
		panic("config server_id must set")
	}
	if len(c.Rack) == 0 {
		panic("config rack must set")
	}
	// volume
	if len(c.VolumeIndex) == 0 {
		c.VolumeIndex = configVolumeIndex
	}
	if len(c.FreeVolumeIndex) == 0 {
		c.FreeVolumeIndex = configFreeVolumeIndex
	}
	if c.NeedleMaxSize < 1 || c.NeedleMaxSize > configNeedleMaxSize {
		c.NeedleMaxSize = configNeedleMaxSize
	}
	if c.BatchMaxNum < 2 || c.BatchMaxNum > configBatchMaxNum {
		c.BatchMaxNum = configBatchMaxNum
	}
	if c.VolumeDelChan < 1 {
		c.VolumeDelChan = configVolumeDelChan
	}
	if c.VolumeSigCnt < 1 {
		c.VolumeSigCnt = configVolumeSigCnt
	}
	if c.VolumeSigTime < 1 {
		c.VolumeSigTime = configVolumeSigTime
	}
	if c.VolumeCheckSize < 1 {
		c.VolumeCheckSize = configVolumeCheckSize
	}
	if c.VolumeCheckInterval < 1 {
		c.VolumeCheckInterval = configVolumeCheckInterval
	}
	// block
	if c.IndexBufferio < 4*1024 {
		c.IndexBufferio = configIndexBufferio
	}
	if c.SuperBlockSync < 1 {
		c.SuperBlockSync = configSuperBlockSync
	}
	if c.IndexRingBuffer < configIndexRingBuffer {
		c.IndexRingBuffer = configIndexRingBuffer
	}
	if c.IndexMerge < 1 {
		c.IndexMerge = configIndexMerge
	}
	if c.IndexMergeTime < 1*time.Second {
		c.IndexMergeTime = configIndexMergeTime
	}
	if c.IndexSync < 1 {
		c.IndexSync = configIndexSync
	}
	if len(c.PprofListen) == 0 {
		c.PprofListen = configPprofListen
	}
	if len(c.StatListen) == 0 {
		c.StatListen = configStatListen
	}
	if len(c.ApiListen) == 0 {
		c.ApiListen = configApiListen
	}
	if len(c.AdminListen) == 0 {
		c.AdminListen = configAdminListen
	}
	if len(c.ZookeeperAddrs) == 0 {
		c.ZookeeperAddrs = configZookeeperAddrs
	}
	if c.ZookeeperTimeout < 1*time.Second {
		c.ZookeeperTimeout = configZookeeperTimeout
	}
	if len(c.ZookeeperRoot) == 0 {
		c.ZookeeperRoot = configZookeeperRoot
	}
}
