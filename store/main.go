package main

import (
	"bfs/store/conf"
	"flag"
	log "github.com/golang/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./store.toml", " set store config file path")
}

func main() {
	var (
		c   *conf.Config
		s   *Store
		err error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs store[%s] start", Ver)
	if c, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("init store...")
	if s, err = NewStore(c); err != nil {
		return
	}
	log.Infof("init http...")
	StartStat(c.StatListen, s)
	StartApi(c.ApiListen, s, c)
	StartAdmin(c.AdminListen, s)
	if c.Pprof {
		StartPprof(c.PprofListen)
	}
	if err = s.SetZookeeper(); err != nil {
		return
	}
	log.Infof("wait signal...")
	StartSignal()
	return
}
