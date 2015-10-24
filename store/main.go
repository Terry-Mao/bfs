package main

import (
	"flag"
	log "github.com/golang/glog"
	"time"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./store.yaml", "set config file path")
}

func main() {
	var (
		c      *Config
		s      *Store
		v      *Volume
		d, buf []byte
		err    error
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
	if s, err = NewStore("/tmp/hijohn.idx"); err != nil {
		log.Errorf("store init error(%v)", err)
		return
	}
	StartStat(s, c.Stat)
	if v, err = s.AddVolume(2, "/tmp/hijohn_2", "/tmp/hijohn_2.idx"); err != nil {
		return
	}
	v.Add(2, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	v.Add(3, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	v.Add(4, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	time.Sleep(1 * time.Second)
	if v = s.Volume(2); v == nil {
		log.Errorf("volume_id: %d not exist", 2)
		return
	}
	buf = s.Buffer()
	defer s.FreeBuffer(buf)
	if d, err = v.Get(2, 1, buf); err != nil {
		log.Errorf("v.Get() error(%v)", err)
		return
	}
	log.V(1).Infof("get: %s", d)
	time.Sleep(60 * time.Second)
	return
}
