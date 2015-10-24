package main

import (
	log "github.com/golang/glog"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Config struct {
	Index string   `yaml: index`
	ZK    []string `yaml:",flow"`
	Pprof struct {
		Enable bool   `yaml: enable`
		Addr   string `yaml: addr`
	}
	Stat string `yaml: stat`
	file string
	f    *os.File
}

func NewConfig(file string) (c *Config, err error) {
	var data []byte
	c = &Config{}
	c.file = file
	if c.f, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\", os.O_RDONLY, 0664) error(%v)", file, err)
		return
	}
	if data, err = ioutil.ReadAll(c.f); err != nil {
		log.Errorf("ioutil.ReadAll(\"%s\") error(%v)", file, err)
		goto failed
	}
	err = yaml.Unmarshal(data, c)
failed:
	c.f.Close()
	return
}
