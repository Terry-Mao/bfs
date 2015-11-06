package main

var (
	testConf *Config
)

func init() {
	StartPprof("localhost:6060")
	testConf = &Config{}
	testConf.StoreIndex = "./test/store.index"
	testConf.ServerId = "1"
	testConf.Rack = "test"
	testConf.setDefault()
}

type testRet struct {
	Ret int `json:"ret"`
}
