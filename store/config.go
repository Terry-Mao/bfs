package main

import (
	log "github.com/golang/glog"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"time"
)

type Config struct {
	Pprof struct {
		Enable bool   `yaml: "enable"`
		Addr   string `yaml: "addr"`
	}
	Zookeeper struct {
		Addrs   []string      `yaml: "addrs,flow"`
		Timeout time.Duration `yaml: "timeout"`
		Root    string        `yaml: "root"`
	}
	Stat     string `yaml: "stat"`
	Admin    string `yaml: "admin"`
	Api      string `yaml: "api"`
	Index    string `yaml: "index"`
	ServerId string `yaml: "serverid"`
	file     string

	f *os.File
}

func NewConfig(file string) (c *Config, err error) {
	var data []byte
	c = &Config{}
	c.file = file
	if c.f, err = os.OpenFile(file, os.O_RDONLY, 0664); err != nil {
		log.Errorf("os.OpenFile(\"%s\") error(%v)", file, err)
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
