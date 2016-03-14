package main

import (
	"bfs/pitchfork/conf"
	"flag"
	log "github.com/golang/glog"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "c", "./pitchfork.toml", " set pitchfork config file path")
}

func main() {
	var (
		config *conf.Config
		p      *Pitchfork
		err    error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs pitchfork start")
	if config, err = conf.NewConfig(configFile); err != nil {
		log.Errorf("NewConfig(\"%s\") error(%v)", configFile, err)
		return
	}
	log.Infof("register pitchfork...")
	if p, err = NewPitchfork(config); err != nil {
		log.Errorf("pitchfork NewPitchfork() failed, Quit now")
		return
	}
	log.Infof("starts probe stores...")
	go p.Probe()
	StartSignal()
	return
}
