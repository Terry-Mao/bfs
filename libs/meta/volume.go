package meta

// volume zk meta data.
type Volume struct {
	NumReqs         int    `json:"numReqs"`
	RestSpace       int    `json:"restSpace"`
	AvgResponseTime float  `json:"avgResponseTime"`
	Id              string `json:"id"`
}

type VolumeList []*Volume
