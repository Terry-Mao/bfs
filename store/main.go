package main

import (
	"flag"
	log "github.com/golang/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./store.conf", " set store config file path")
}

func main() {
	var (
		c   *Config
		z   *Zookeeper
		s   *Store
		err error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs store[%s] start", Ver)
	if c, err = NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("init zookeeper...")
	if z, err = NewZookeeper(c.ZookeeperAddrs, c.ZookeeperTimeout, c.ZookeeperRoot, c.Rack, c.ServerId); err != nil {
		return
	}
	log.Infof("init store...")
	if s, err = NewStore(z, c); err != nil {
		return
	}
	log.Infof("init http stat...")
	StartStat(c.StatListen, s)
	log.Infof("init http api...")
	StartApi(c.ApiListen, s, c)
	log.Infof("init http admin...")
	StartAdmin(c.AdminListen, s)
	if c.PprofEnable {
		log.Infof("init http pprof...")
		StartPprof(c.PprofListen)
	}
	// update zk store meta
	if err = z.SetStore(c.ServerId, c.Rack, c.StatListen, c.AdminListen, c.ApiListen); err != nil {
		log.Errorf("zk.SetStore() error(%v)", err)
		return
	}
	// update zk root
	if err = z.SetRoot(); err != nil {
		log.Errorf("zk.SetRoot() error(%v)", err)
		return
	}
	log.Infof("wait signal...")
	StartSignal()
	return
}
