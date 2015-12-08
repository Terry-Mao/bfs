package main

import (
	"flag"
	"runtime"
	log "github.com/golang/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./directory.conf", " set directory config file path")
}

func main() {
	var (
		c      *Config
		zk     *Zookeeper
		d      *Directory
		err    error
	)
	flag.Parse()
	defer log.Flush()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Infof("bfs directory start")
	if c, err = NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("init zookeeper...")
	if zk, err = NewZookeeper(c.ZkAddrs, c.ZkTimeout, c.ZkStoreRoot, c.ZkVolumeRoot,
					 c.ZkGroupRoot); err != nil {
		log.Errorf("NewZookeeper() failed, Quit now")
		return
	}
	log.Infof("new directory...")
	if d, err = NewDirectory(c, zk); err != nil {
		log.Errorf("pitchfork NewDirectory() failed, Quit now")
		return
	}
	log.Infof("init http api...")
	StartApi(c.ApiListen, d)
	StartSignal()
	return
}
