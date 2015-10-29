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
	flag.StringVar(&configFile, "c", "./store.yaml", "set config file path")
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
	if c.Pprof.Enable {
		StartPprof(c.Pprof.Addr)
	}
	if z, err = NewZookeeper(c.Zookeeper.Addrs, c.Zookeeper.Timeout,
		path.Join(c.Zookeeper.Root, c.ServerId)); err != nil {
		return
	}
	if s, err = NewStore(z, c.Index); err != nil {
		log.Errorf("store init error(%v)", err)
		return
	}
	StartStat(s, c.Stat)
	StartApi(s, c.Api)
	if err = z.SetStore(c.Stat, c.Admin, c.Api); err != nil {
		log.Errorf("zk.SetStore() error(%v)", err)
		return
	}
	StartSignal()
	return
}
