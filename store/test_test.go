package main

var (
	testConf *Config
)

func init() {
	StartPprof("localhost:6060")
	testConf = &Config{}
	testConf.VolumeIndex = "./test/volume.idx"
	testConf.FreeVolumeIndex = "./test/free_volume.idx"
	testConf.ServerId = "1"
	testConf.Rack = "test"
	testConf.setDefault()
}

type testRet struct {
	Ret int `json:"ret"`
}
