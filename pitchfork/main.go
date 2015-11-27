package main

import (
	"flag"

	log "github.com/golang/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./pitchfork.conf", " set pitchfork config file path")
}

func main() {
	var (
		config *Config
		zk     *Zookeeper
		p      *Pitchfork
		err    error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs pitchfork start")
	if config, err = NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("init zookeeper...")
	if zk, err = NewZookeeper(config.ZookeeperAddrs, config.ZookeeperTimeout, config.ZookeeperPitchforkRoot, config.ZookeeperStoreRoot); err != nil {
		log.Errorf("NewZookeeper() failed, Quit now")
		return
	}
	log.Infof("register pitchfork...")
	if p, err = NewPitchfork(zk, config); err != nil {
		log.Errorf("pitchfork NewPitchfork() failed, Quit now")
		return
	}
	log.Infof("starts probe stores...")
	go p.Probe()
	StartSignal()
	return
}
