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
	if z, err = NewZookeeper(c.ZookeeperAddrs, c.ZookeeperTimeout, fpath); err != nil {
		return
	}
	if s, err = NewStore(z, c); err != nil {
		return
	}
	StartStat(c.StatListen, s)
	StartApi(c.ApiListen, s, c)
	StartAdmin(c.AdminListen, s)
	if c.PprofEnable {
		StartPprof(c.PprofListen)
	}
	// update zk store meta
	if err = z.SetStore(c.StatListen, c.AdminListen, c.ApiListen); err != nil {
		log.Errorf("zk.SetStore() error(%v)", err)
		return
	}
	StartSignal()
	return
}
