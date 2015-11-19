package main

import (
	"github.com/Terry-Mao/goconf"
	"time"
)

const (
	// zookeeper
	configZookeeperTimeout          = time.Second * 1 // 1s
	configZookeeperPitchforkRoot    = "/pitchfork"
	configZookeeperStoreRoot        = '/rack'
	configZookeeperDirectoryRoot    = "/directory"
	configProbeInterval             = 3
	configMaxUsedSpace              = 0.95
)

var (
	configZookeeperAddrs = []string{"localhost:2181"}
)

type Config struct {
	// zookeeper
	ZookeeperAddrs            []string      `goconf:"zookeeper:addrs:,"`
	ZookeeperTimeout          time.Duration `goconf:"zookeeper:timeout"`
	ZookeeperPitchforkRoot    string        `goconf:"zookeeper:root"`
	ZookeeperStoreRoot        string        `goconf:"zookeeper:root"`
	ZookeeperDirectoryRoot    string        `goconf:"zookeeper:root"`
	ProbeInterval             int           `goconf:"probeinterval"`
	MaxUsedSpacePercent       float32       `goconf:"restblockspace"`
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
	if len(c.ZookeeperDirectoryRoot) == 0 {
		c.ZookeeperDirectoryRoot = configZookeeperDirectoryRoot
	}
	if c.ProbeInterval == 0 {
		c.ProbeInterval = configProbeInterval
	}
	if c.MaxUsedSpacePercent == 0 {
		c.MaxUsedSpacePercent = configMaxUsedSpacePercent
}
