package main

func init() {
	StartPprof("localhost:6060")
}

type testRet struct {
	Ret int `json:"ret"`
}
