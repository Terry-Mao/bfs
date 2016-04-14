package meta

// StoreRet
type StoreRet struct {
	Ret int `json:"ret"`
}

// Response
type Response struct {
	Ret    int      `json:"ret"`
	Key    int64    `json:"key"`
	Cookie int32    `json:"cookie"`
	Vid    int32    `json:"vid"`
	Stores []string `json:"stores"`
}
