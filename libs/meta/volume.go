package meta

/*
// volume zk meta data.
type Volume struct {
	NumReqs         string `json:"numReqs"`
	RestSpace       string `json:"restSpace"`
	AvgResponseTime string `json:"avgResponseTime"`
	Id              string `json:"id"`
}
*/

type Volume struct {
	Id           int32       `json:"id"`
	Block        *SuperBlock `json:"block"`
	CheckNeedles []*Needle   `json:"check_needles"`
	//Stats   *stat.Stats       `json:"stats"`
	//Indexer *index.Indexer    `json:"index"`
}