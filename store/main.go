package main

import (
	"flag"
	log "github.com/golang/glog"
)

func main() {
	var (
		s   *Store
		v   *Volume
		d   []byte
		err error
	)
	flag.Parse()
	defer log.Flush()
	log.Infof("bfs store[%s] start", Ver)
	if s, err = NewStore("/tmp/hijohn.idx"); err != nil {
		log.Errorf("store init error(%v)", err)
		return
	}
	//if v, err = s.AddVolume(2, "/tmp/hijohn_2", "/tmp/hijohn_2.idx"); err != nil {
	//	return
	//}
	//v.Add(1, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(2, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(3, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(4, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//// v.block.Dump()
	if v = s.Volume(1); v == nil {
		log.Errorf("volume_id: %d not exist", 1)
		return
	}
	//	if err = v.Add(1, 1, []byte("test")); err != nil {
	//		log.Errorf("v.Add() error(%v)", err)
	//		return
	//	}
	if d, err = v.Get(1, 1, make([]byte, 100)); err != nil {
		log.Errorf("v.Get() error(%v)", err)
		return
	}
	log.V(1).Infof("get: %s", d)
	return
}
