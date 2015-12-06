package main

import (
    "time"
    "github.com/Terry-Mao/goconf"
)

const (
	// zookeeper
	configSfZookeeperTimeout        = time.Second * 1 // 1s
	configSfZookeeperPath           = "/bfs_sf"
	configSfWorkId                  = 72
	configLiseten                   = "localhost:6065"
	configMaxNum                    = 16
)

var (
	configSfZookeeperAddrs = []string{"localhost:2181"}
)

type Config struct {
	// zookeeper
	SfZookeeperAddrs          []string      `goconf:"snowflake:zkaddrs:,"`
	SfZookeeperTimeout        time.Duration `goconf:"snowflake:zktimeout"`
	SfZookeeperPath           string        `goconf:"snowflake:zkpath"`
	SfWorkId                  int           `goconf:"snowflake:workid"`
	// hbase
	HbaseAddr      string        `goconf:"hbase:addr"`
	HbaseMaxActive int           `goconf:"hbase:max.active"`
	HbaseMaxIdle   int           `goconf:"hbase:max.idle"`
	HbaseTimeout   time.Duration `goconf:"hbase:timeout:time"`
	// http
	MaxNum         int           `goconf:"http:port"`
	Listen         string        `goconf:"http:listen"`
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
	if len(c.SfZookeeperAddrs) == 0 {
		c.SfZookeeperAddrs = configSfZookeeperAddrs
	}
	if c.SfZookeeperTimeout < 1*time.Second {
		c.SfZookeeperTimeout = configSfZookeeperTimeout
	}
	if c.SfZookeeperPath == "" {
		c.SfZookeeperPath = configSfZookeeperPath
	}
	if c.SfWorkId == 0 {
		c.SfWorkId = configSfWorkId
	}
	if c.MaxNum == 0 || c.MaxNum > configMaxNum {
		c.MaxNum = configMaxNum
	}
	if c.Listen == "" {
		c.Listen = configLiseten
	}
}
