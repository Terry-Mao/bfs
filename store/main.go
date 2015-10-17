package main

import (
	"log"
)

func main() {
	var (
		s   *Store
		v   *Volume
		d   []byte
		err error
	)
	s = NewStore()
	if v, err = s.AddVolume(1, "/tmp/hijohn_1", "/tmp/hijohn_1.idx"); err != nil {
		return
	}
	//v.Add(1, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(2, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(3, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.Add(4, 1, []byte("fa;dflkad;lfajdfkladf;ladjf"))
	//v.block.Dump()
	if d, err = v.Get(3, 1); err != nil {
		log.Printf("%v\n", err)
		return
	}
	log.Printf("%s\n", d)
}
