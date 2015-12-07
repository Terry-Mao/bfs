package main

import (
    "time"
    "github.com/Terry-Mao/goconf"
)

const (
	// snowflake zookeeper
	configSnowflakeZkTimeout        = time.Second * 1 // 1s
	configSnowflakeZkPath           = "/bfs_sf"
	configSnowflakeWorkId           = 72
	// zookeeper
	configZkTimeout                 = time.Second * 1 // 1s
	configZkStoreRoot               = "/rack"
	configZkVolumeRoot              = "/volume"
	configZkGroupRoot               = "/group"
	// hbase
	configHbaseAddr                 = "localhost:9090"
	configHbaseMaxActive            = 50
	configHbaseMaxIdle              = 10
	configHbaseTimeout              = time.Second * 5
	// http
	configApiListen                 = "localhost:6065"
	configMaxNum                    = 16
)

var (
	configSnowflakeZkAddrs = []string{"localhost:2181"}
	configZkAddrs = []string{"localhost:2181"}
)

type Config struct {
	// snowflake zookeeper
	SnowflakeZkAddrs          []string      `goconf:"snowflake:sfzkaddrs:,"`
	SnowflakeZkTimeout        time.Duration `goconf:"snowflake:sfzktimeout"`
	SnowflakeZkPath           string        `goconf:"snowflake:sfzkpath"`
	SnowflakeWorkId           int           `goconf:"snowflake:workid"`
	// bfs zookeeper
	ZkAddrs        []string      `goconf:"zookeeper:addrs:,"`
	ZkTimeout      time.Duration `goconf:"zookeeper:timeout:,"`
	ZkStoreRoot    string        `goconf:"zookeeper:storeroot"`
	ZkVolumeRoot   string        `goconf:"zookeeper:volumeroot"`
	ZkGroupRoot    string        `goconf:"zookeeper:grouproot"`
	// hbase
	HbaseAddr      string        `goconf:"hbase:addr"`
	HbaseMaxActive int           `goconf:"hbase:max.active"`
	HbaseMaxIdle   int           `goconf:"hbase:max.idle"`
	HbaseTimeout   time.Duration `goconf:"hbase:timeout:time"`
	// http
	MaxNum         int           `goconf:"http:maxnum"`
	ApiListen      string        `goconf:"http:apilisten"`
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
	if len(c.SnowflakeZkAddrs) == 0 {
		c.SnowflakeZkAddrs = configSnowflakeZkAddrs
	}
	if c.SnowflakeZkTimeout < 1*time.Second {
		c.SnowflakeZkTimeout = configSnowflakeZkTimeout
	}
	if c.SnowflakeZkPath == "" {
		c.SnowflakeZkPath = configSnowflakeZkPath
	}
	if c.SnowflakeWorkId == 0 {
		c.SnowflakeWorkId = configSnowflakeWorkId
	}
	if len(c.ZkAddrs) == 0 {
		c.ZkAddrs = configZkAddrs
	}
	if c.ZkTimeout < 1*time.Second {
		c.ZkTimeout = configZkTimeout
	}
	if c.ZkStoreRoot == "" {
		c.ZkStoreRoot = configZkStoreRoot
	}
	if c.ZkVolumeRoot == "" {
		c.ZkVolumeRoot = configZkVolumeRoot
	}
	if c.ZkGroupRoot == "" {
		c.ZkGroupRoot = configZkGroupRoot
	}
	if c.HbaseAddr == "" {
		c.HbaseAddr = configHbaseAddr
	}
	if c.HbaseMaxActive == 0 {
		c.HbaseMaxActive = configHbaseMaxActive
	}
	if c.HbaseMaxIdle == 0 {
		c.HbaseMaxIdle = configHbaseMaxIdle
	}
	if c.HbaseTimeout < 1*time.Second {
		c.HbaseTimeout = configHbaseTimeout
	}
	if c.MaxNum == 0 || c.MaxNum > configMaxNum {
		c.MaxNum = configMaxNum
	}
	if c.ApiListen == "" {
		c.ApiListen = configApiListen
	}
}
