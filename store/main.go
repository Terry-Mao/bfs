package main

import (
	"flag"
	log "github.com/golang/glog"
	"path"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./store.conf", " set store config file path")
}

func main() {
	var (
		c     *Config
		z     *Zookeeper
		s     *Store
		fpath string
		err   error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs store[%s] start", Ver)
	if c, err = NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	fpath = path.Join(c.ZookeeperRoot, c.Rack, c.ServerId)
	log.Infof("init zookeeper...")
	if z, err = NewZookeeper(c.ZookeeperAddrs, c.ZookeeperTimeout, fpath); err != nil {
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
	if err = z.SetStore(c.StatListen, c.AdminListen, c.ApiListen); err != nil {
		log.Errorf("zk.SetStore() error(%v)", err)
		return
	}
	log.Infof("wait signal...")
	StartSignal()
	return
}
