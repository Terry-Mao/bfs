package meta

// volume zk meta data.
type Volume struct {
	NumReqs         string `json:"numReqs"`
	RestSpace       string `json:"restSpace"`
	AvgResponseTime string `json:"avgResponseTime"`
	Id              string `json:"id"`
}
