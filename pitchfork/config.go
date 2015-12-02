package main

import (
    "time"
    "github.com/Terry-Mao/goconf"
)

const (
	// zookeeper
	configZookeeperTimeout          = time.Second * 1 // 1s
	configZookeeperPitchforkRoot    = "/pitchfork"
	configZookeeperStoreRoot        = "/rack"
	configZookeeperVolumeRoot       = "/volume"
	configGetInterval               = time.Second * 3
	configHeadInterval              = time.Second * 60
	configMaxUsedSpacePercent       = 0.95
)

var (
	configZookeeperAddrs = []string{"localhost:2181"}
)

type Config struct {
	// zookeeper
	ZookeeperAddrs            []string      `goconf:"zookeeper:addrs:,"`
	ZookeeperTimeout          time.Duration `goconf:"zookeeper:timeout"`
	ZookeeperPitchforkRoot    string        `goconf:"zookeeper:pitchforkroot"`
	ZookeeperStoreRoot        string        `goconf:"zookeeper:storeroot"`
	ZookeeperVolumeRoot       string        `goconf:"zookeeper:volumeroot"`
	GetInterval               time.Duration `goconf:"store:get_interval:time"`
	HeadInterval              time.Duration `goconf:"store:head_interval:time"`
	MaxUsedSpacePercent       float32       `goconf:"store:max_used_space_percent"`
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
	if len(c.ZookeeperAddrs) == 0 {
		c.ZookeeperAddrs = configZookeeperAddrs
	}
	if c.ZookeeperTimeout < 1*time.Second {
		c.ZookeeperTimeout = configZookeeperTimeout
	}
	if len(c.ZookeeperPitchforkRoot) == 0 {
		c.ZookeeperPitchforkRoot = configZookeeperPitchforkRoot
	}
	if len(c.ZookeeperStoreRoot) == 0 {
		c.ZookeeperStoreRoot = configZookeeperStoreRoot
	}
	if len(c.ZookeeperVolumeRoot) == 0 {
		c.ZookeeperVolumeRoot = configZookeeperVolumeRoot
	}
	if c.GetInterval == 0 {
		c.GetInterval = configGetInterval
	}
	if c.HeadInterval == 0 {
		c.HeadInterval = configHeadInterval
	}
	if c.MaxUsedSpacePercent == 0 {
		c.MaxUsedSpacePercent = configMaxUsedSpacePercent
	}
}
