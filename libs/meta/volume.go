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
