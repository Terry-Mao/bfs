package meta
import "github.com/Terry-Mao/bfs/libs/stat"

type Volume struct {
	Id           int32       `json:"id"`
	Block        *SuperBlock `json:"block"`
	CheckNeedles []Needle    `json:"check_needles"`
	Stats        *stat.Stats `json:"stats"`
	//Indexer *index.Indexer    `json:"index"`
}

type InfoVolume struct {
	Volumes      []*Volume   `json:"volumes"`
}

// StateVolume  for zk /volume stat
type StateVolume struct {
	TotalAddProcessed       uint64 `json:"total_add_processed"`
	TotalAddDelay           uint64 `json:"total_add_delay"`
	RestSpace               uint32 `json:"rest_space"`
}